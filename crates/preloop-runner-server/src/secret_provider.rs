//! Workflow secrets (`secrets.*` handed to jobs), resolved through one seam.
//!
//! Distinct from [`crate::credential_store::CredentialStore`], which holds the
//! engine's own credentials (GitHub App key, PAT, engine token) and is never
//! reachable from a job. A [`SecretProvider`] only ever returns tenant
//! workflow secrets for an explicit scope, so a scoping bug cannot hand an
//! engine credential to user code.
//!
//! Secret values never enter the control database. Jobs carry secret names;
//! values are resolved here when a runner acquires the job. Values a caller
//! supplies with a submission (`preloop --secret`) are written to the run
//! tier ([`SecretProvider::put_run`]) at submit and live as long as the run's
//! history, so a re-run re-resolves them.

use crate::state::SecretStore;
use crate::store::Envelope;
use preloop_gha_protocol::{RunId, SecretString};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

/// The scope a job's secrets are resolved for, mirroring GitHub's
/// org → repository → environment tiers, plus the submission's own run tier.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SecretScope<'a> {
    pub(crate) repository: &'a str,
    /// The job's resolved `environment:`; `None` skips the environment tier.
    pub(crate) environment: Option<&'a str>,
    /// The run whose submission-supplied values apply; `None` skips the run
    /// tier.
    pub(crate) run_id: Option<RunId>,
}

/// Source of workflow secrets. Implementations must never log values.
pub(crate) trait SecretProvider: Send + Sync {
    /// Every secret visible in `scope`, already merged by precedence:
    /// run > environment > repository > global, per name.
    fn resolve(&self, scope: SecretScope<'_>) -> anyhow::Result<BTreeMap<String, SecretString>>;

    /// Union of every stored secret value across all tiers, exposed values
    /// only. Used by the log masker's fallback for plan ids that never
    /// resolved to a run — best-effort over-masking, not a secrecy boundary.
    fn resolve_all(&self) -> anyhow::Result<Vec<String>>;

    /// Store the values a submission supplied for `run_id`. They outrank every
    /// other tier for that run. Must be durable before the run is committed:
    /// any node may acquire the run's jobs, at any later time.
    fn put_run(
        &self,
        run_id: RunId,
        secrets: &BTreeMap<String, SecretString>,
    ) -> anyhow::Result<()>;

    /// The run tier alone (for re-runs, which re-submit the same values).
    fn run_tier(&self, run_id: RunId) -> anyhow::Result<BTreeMap<String, SecretString>>;

    /// Drop the run tier. Idempotent.
    fn delete_run(&self, run_id: RunId) -> anyhow::Result<()>;

    /// Backend name for diagnostics.
    fn name(&self) -> &'static str;
}

/// Self-hosted default: the tiered in-memory store loaded from the config
/// file / systemd credential and mutated by the live secrets API, plus run
/// tiers sealed to `<state_dir>/run-secrets/<run_id>` with the cluster key.
///
/// Node-local: global/repo/environment tiers live in this node's config and
/// run tiers on this node's disk. Multi-node deployments need a shared
/// provider.
pub(crate) struct BuiltinSecretProvider {
    store: Arc<parking_lot::RwLock<SecretStore>>,
    runs: RunTierFiles,
}

impl BuiltinSecretProvider {
    pub(crate) fn new(
        store: Arc<parking_lot::RwLock<SecretStore>>,
        run_dir: PathBuf,
        cipher: Envelope,
    ) -> Self {
        Self {
            store,
            runs: RunTierFiles {
                dir: run_dir,
                cipher,
                cache: parking_lot::RwLock::new(HashMap::new()),
            },
        }
    }
}

impl SecretProvider for BuiltinSecretProvider {
    fn resolve(&self, scope: SecretScope<'_>) -> anyhow::Result<BTreeMap<String, SecretString>> {
        let run = match scope.run_id {
            Some(run_id) => Some(self.runs.get(run_id)?),
            None => None,
        };
        let store = self.store.read();
        let repo = store.repo.get(scope.repository);
        let env = scope.environment.and_then(|env| {
            store
                .env
                .get(scope.repository)
                .and_then(|envs| envs.get(env))
        });
        // Coarsest tier first so each finer tier overwrites per name.
        Ok(std::iter::once(&store.global)
            .chain(repo)
            .chain(env)
            .chain(run.as_deref())
            .flatten()
            .map(|(name, value)| (name.clone(), SecretString::new(value.clone())))
            .collect())
    }

    fn resolve_all(&self) -> anyhow::Result<Vec<String>> {
        let store = self.store.read();
        let mut values: Vec<String> = store
            .global
            .values()
            .chain(store.repo.values().flat_map(|repo| repo.values()))
            .chain(
                store
                    .env
                    .values()
                    .flat_map(|envs| envs.values().flat_map(|env| env.values())),
            )
            .cloned()
            .collect();
        values.extend(
            self.runs
                .cache
                .read()
                .values()
                .flat_map(|run| run.values().cloned()),
        );
        Ok(values)
    }

    fn put_run(
        &self,
        run_id: RunId,
        secrets: &BTreeMap<String, SecretString>,
    ) -> anyhow::Result<()> {
        if secrets.is_empty() {
            return Ok(());
        }
        self.runs
            .put(run_id, preloop_gha_protocol::masking::expose_all(secrets))
    }

    fn run_tier(&self, run_id: RunId) -> anyhow::Result<BTreeMap<String, SecretString>> {
        Ok(self
            .runs
            .get(run_id)?
            .iter()
            .map(|(name, value)| (name.clone(), SecretString::new(value.clone())))
            .collect())
    }

    fn delete_run(&self, run_id: RunId) -> anyhow::Result<()> {
        self.runs.delete(run_id)
    }

    fn name(&self) -> &'static str {
        "builtin"
    }
}

/// One sealed JSON map per run, cached in memory after first read. A missing
/// file is an empty tier (and is deliberately not cached: a run tier may
/// appear after another node's `put_run`).
struct RunTierFiles {
    dir: PathBuf,
    cipher: Envelope,
    cache: parking_lot::RwLock<HashMap<RunId, Arc<BTreeMap<String, String>>>>,
}

impl RunTierFiles {
    fn path(&self, run_id: RunId) -> PathBuf {
        self.dir.join(run_id.to_string())
    }

    fn put(&self, run_id: RunId, values: BTreeMap<String, String>) -> anyhow::Result<()> {
        let sealed = self.cipher.seal(&serde_json::to_vec(&values)?)?;
        std::fs::create_dir_all(&self.dir)?;
        let path = self.path(run_id);
        let tmp = path.with_extension("tmp");
        // Durable before the run commits: any node may acquire the run's jobs
        // at any later time, so the write must survive a crash between here
        // and the commit. fsync the file, then the directory that carries the
        // rename.
        {
            use std::io::Write as _;
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&sealed)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        std::fs::File::open(&self.dir)?.sync_all()?;
        self.cache.write().insert(run_id, Arc::new(values));
        Ok(())
    }

    fn get(&self, run_id: RunId) -> anyhow::Result<Arc<BTreeMap<String, String>>> {
        if let Some(values) = self.cache.read().get(&run_id) {
            return Ok(Arc::clone(values));
        }
        let values = match std::fs::read(self.path(run_id)) {
            Ok(sealed) => serde_json::from_slice(&self.cipher.unseal(&sealed)?)?,
            // A missing file is an empty tier, but it must not be cached as
            // one: another node may still be writing this run's tier, and a
            // cached empty value would hide the later write for the life of
            // the process (the whole point of `put_run`'s durability).
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Arc::new(BTreeMap::new()));
            }
            Err(error) => return Err(error.into()),
        };
        let values = Arc::new(values);
        self.cache.write().insert(run_id, Arc::clone(&values));
        Ok(values)
    }

    fn delete(&self, run_id: RunId) -> anyhow::Result<()> {
        self.cache.write().remove(&run_id);
        match std::fs::remove_file(self.path(run_id)) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(dir: &std::path::Path) -> BuiltinSecretProvider {
        let mut store = SecretStore::default();
        store.global.insert("SHARED".into(), "global".into());
        store.global.insert("GLOBAL_ONLY".into(), "g".into());
        store.repo.insert(
            "o/r".into(),
            BTreeMap::from([("SHARED".into(), "repo".into())]),
        );
        store.env.insert(
            "o/r".into(),
            BTreeMap::from([(
                "prod".into(),
                BTreeMap::from([("SHARED".into(), "env".into())]),
            )]),
        );
        BuiltinSecretProvider::new(
            Arc::new(parking_lot::RwLock::new(store)),
            dir.join("run-secrets"),
            Envelope::new(b"test-cluster-key"),
        )
    }

    fn get(values: &BTreeMap<String, SecretString>, name: &str) -> Option<String> {
        let value = values.get(name)?;
        Some(value.expose().to_owned())
    }

    fn scope<'a>(
        repository: &'a str,
        environment: Option<&'a str>,
        run_id: Option<RunId>,
    ) -> SecretScope<'a> {
        SecretScope {
            repository,
            environment,
            run_id,
        }
    }

    #[test]
    fn finer_tiers_win_per_name() {
        let dir = tempfile::tempdir().unwrap();
        let provider = provider(dir.path());
        let run_id = RunId::new();
        provider
            .put_run(
                run_id,
                &BTreeMap::from([("SHARED".into(), SecretString::new("run"))]),
            )
            .unwrap();

        let run = provider
            .resolve(scope("o/r", Some("prod"), Some(run_id)))
            .unwrap();
        assert_eq!(get(&run, "SHARED").as_deref(), Some("run"));

        let env = provider.resolve(scope("o/r", Some("prod"), None)).unwrap();
        assert_eq!(get(&env, "SHARED").as_deref(), Some("env"));
        assert_eq!(get(&env, "GLOBAL_ONLY").as_deref(), Some("g"));

        let repo = provider
            .resolve(scope("o/r", Some("staging"), None))
            .unwrap();
        assert_eq!(get(&repo, "SHARED").as_deref(), Some("repo"));

        let other = provider.resolve(scope("x/y", Some("prod"), None)).unwrap();
        assert_eq!(get(&other, "SHARED").as_deref(), Some("global"));
    }

    #[test]
    fn run_tier_is_sealed_on_disk_survives_restart_and_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let run_id = RunId::new();
        provider(dir.path())
            .put_run(
                run_id,
                &BTreeMap::from([("TOKEN".into(), SecretString::new("s3cr3t-value"))]),
            )
            .unwrap();

        let file = dir.path().join("run-secrets").join(run_id.to_string());
        let on_disk = std::fs::read(&file).unwrap();
        assert!(
            !on_disk
                .windows(b"s3cr3t-value".len())
                .any(|w| w == b"s3cr3t-value"),
            "run tier must be sealed at rest"
        );

        // A fresh provider (engine restart) reads the sealed file.
        let restarted = provider(dir.path());
        let resolved = restarted.resolve(scope("o/r", None, Some(run_id))).unwrap();
        assert_eq!(get(&resolved, "TOKEN").as_deref(), Some("s3cr3t-value"));

        restarted.delete_run(run_id).unwrap();
        assert!(!file.exists());
        let after = restarted.resolve(scope("o/r", None, Some(run_id))).unwrap();
        assert_eq!(get(&after, "TOKEN"), None);
        restarted.delete_run(run_id).unwrap();
    }
}
