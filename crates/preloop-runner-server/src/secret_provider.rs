//! Workflow secrets (`secrets.*` handed to jobs), resolved through one seam.
//!
//! Distinct from [`crate::credential_store::CredentialStore`], which holds the
//! engine's own credentials (GitHub App key, PAT, engine token) and is never
//! reachable from a job. A [`SecretProvider`] only ever returns tenant
//! workflow secrets for an explicit scope, so a scoping bug cannot hand an
//! engine credential to user code.

use crate::state::SecretStore;
use preloop_gha_protocol::SecretString;
use std::collections::BTreeMap;
use std::sync::Arc;

/// The scope a job's secrets are resolved for, mirroring GitHub's
/// org → repository → environment tiers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SecretScope<'a> {
    pub(crate) repository: &'a str,
    /// The job's resolved `environment:`; `None` skips the environment tier.
    pub(crate) environment: Option<&'a str>,
}

/// Source of workflow secrets. Implementations must never log values.
pub(crate) trait SecretProvider: Send + Sync {
    /// Every secret visible in `scope`, already merged by precedence:
    /// environment > repository > global, per name.
    fn resolve(&self, scope: SecretScope<'_>) -> anyhow::Result<BTreeMap<String, SecretString>>;

    /// Backend name for diagnostics.
    fn name(&self) -> &'static str;
}

/// Self-hosted default: the tiered in-memory store loaded from the config
/// file / systemd credential and mutated by the live secrets API.
pub(crate) struct BuiltinSecretProvider {
    store: Arc<parking_lot::RwLock<SecretStore>>,
}

impl BuiltinSecretProvider {
    pub(crate) fn new(store: Arc<parking_lot::RwLock<SecretStore>>) -> Self {
        Self { store }
    }
}

impl SecretProvider for BuiltinSecretProvider {
    fn resolve(&self, scope: SecretScope<'_>) -> anyhow::Result<BTreeMap<String, SecretString>> {
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
            .flatten()
            .map(|(name, value)| (name.clone(), SecretString::new(value.clone())))
            .collect())
    }

    fn name(&self) -> &'static str {
        "builtin"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> BuiltinSecretProvider {
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
        BuiltinSecretProvider::new(Arc::new(parking_lot::RwLock::new(store)))
    }

    fn get(values: &BTreeMap<String, SecretString>, name: &str) -> Option<String> {
        values.get(name).map(|value| value.expose().to_owned())
    }

    #[test]
    fn finer_tiers_win_per_name() {
        let provider = provider();
        let scope = |repository, environment| SecretScope {
            repository,
            environment,
        };
        let env = provider.resolve(scope("o/r", Some("prod"))).unwrap();
        assert_eq!(get(&env, "SHARED").as_deref(), Some("env"));
        assert_eq!(get(&env, "GLOBAL_ONLY").as_deref(), Some("g"));

        let repo = provider.resolve(scope("o/r", Some("staging"))).unwrap();
        assert_eq!(get(&repo, "SHARED").as_deref(), Some("repo"));

        let other = provider.resolve(scope("x/y", Some("prod"))).unwrap();
        assert_eq!(get(&other, "SHARED").as_deref(), Some("global"));
    }
}
