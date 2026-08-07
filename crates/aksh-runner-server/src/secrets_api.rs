//! Live secrets API (`/api/v1/secrets`).
//!
//! `preloop secret set/list/rm` talk to these endpoints when an engine is
//! running so stored secrets apply to the very next submitted run, instead
//! of only after a restart. The handlers mutate the in-memory store and
//! persist the config file, so the change survives both ways: it applies
//! immediately AND on the next engine start.
//!
//! Values are never returned — `list` returns names and their repository
//! scope only.

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::errors::ApiError;
use crate::state::SharedState;

/// Secret names mirror GitHub: UPPER_SNAKE (letters, digits, underscore).
pub(crate) fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// Repository scope must look like `owner/repo`.
pub(crate) fn valid_repo(repo: &str) -> bool {
    repo.split_once('/')
        .is_some_and(|(owner, name)| !owner.is_empty() && !name.is_empty())
}

/// Environment scope mirrors GitHub: letters, digits, hyphens, and
/// underscores, at most 255 characters, never starting with `-` or `_`.
pub(crate) fn valid_env(env: &str) -> bool {
    !env.is_empty()
        && env.len() <= 255
        && !env.starts_with(['-', '_'])
        && env
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// True when `name` in the given scope is managed by the systemd credential
/// (the operator-owned durable base set). The live API rejects mutations on
/// these: a `set` would be reverted at the next startup when the credential
/// re-applies over config.toml, and a `rm` would 404 or silently reappear.
/// Editing the credential file (and re-encrypting with `systemd-creds`) is
/// the supported path.
fn credential_scope_conflict(
    credential: &crate::config::ConfigFile,
    repo: Option<&str>,
    env: Option<&str>,
    name: &str,
) -> bool {
    match (repo, env) {
        (Some(repo), Some(env)) => credential
            .env_secrets
            .get(repo)
            .and_then(|envs| envs.get(env))
            .is_some_and(|names| names.contains_key(name)),
        (Some(repo), None) => credential
            .repo_secrets
            .get(repo)
            .is_some_and(|names| names.contains_key(name)),
        (None, None) => credential.secrets.contains_key(name),
        // Rejected earlier by validation; treat as no conflict.
        (None, Some(_)) => false,
    }
}

#[derive(Deserialize)]
pub(crate) struct SetSecretBody {
    #[serde(default)]
    pub value: Option<String>,
    /// `owner/repo` scope; absent = global.
    #[serde(default)]
    pub repo: Option<String>,
    /// Environment scope; requires `repo`. Mirrors GitHub environment secrets.
    #[serde(default)]
    pub env: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct SecretQuery {
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub env: Option<String>,
}

/// List stored secret names (never values), scoped by repo when asked.
pub(crate) async fn list_secrets(
    State(shared): State<Arc<SharedState>>,
    Query(query): Query<SecretQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if query.env.is_some() && query.repo.is_none() {
        return Err(ApiError::bad_request(
            "env scope requires repo (owner/repo)",
        ));
    }
    if let Some(repo) = &query.repo {
        if !valid_repo(repo) {
            return Err(ApiError::bad_request("repo must look like owner/repo"));
        }
    }
    if let Some(env) = &query.env {
        if !valid_env(env) {
            return Err(ApiError::bad_request(
                "env must be an alphanumeric name (letters, digits, - and _)",
            ));
        }
    }
    let store = shared.state.secrets.read();
    let mut secrets = Vec::new();
    match (query.repo, query.env) {
        (Some(repo), Some(env)) => {
            if let Some(map) = store.env.get(&repo).and_then(|envs| envs.get(&env)) {
                for name in map.keys() {
                    secrets.push(serde_json::json!({
                        "name": name,
                        "repo": repo,
                        "env": env,
                    }));
                }
            }
        }
        (Some(repo), None) => {
            if let Some(map) = store.repo.get(&repo) {
                for name in map.keys() {
                    secrets.push(serde_json::json!({ "name": name, "repo": repo }));
                }
            }
        }
        (None, None) => {
            for name in store.global.keys() {
                secrets.push(serde_json::json!({ "name": name, "repo": serde_json::Value::Null }));
            }
            for (repo, map) in &store.repo {
                for name in map.keys() {
                    secrets.push(serde_json::json!({ "name": name, "repo": repo }));
                }
            }
            for (repo, envs) in &store.env {
                for (env, map) in envs {
                    for name in map.keys() {
                        secrets.push(serde_json::json!({
                            "name": name,
                            "repo": repo,
                            "env": env,
                        }));
                    }
                }
            }
        }
        (None, Some(_)) => unreachable!("env without repo rejected above"),
    }
    Ok(Json(serde_json::json!({ "secrets": secrets })))
}

/// Store a secret (global or per-repo) in memory and in the config file.
pub(crate) async fn set_secret(
    State(shared): State<Arc<SharedState>>,
    Path(name): Path<String>,
    Json(body): Json<SetSecretBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if !valid_name(&name) {
        return Err(ApiError::bad_request(
            "secret name must be UPPER_SNAKE (letters, digits, underscore)",
        ));
    }
    let value = body
        .value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("empty secret value"))?;
    if let Some(repo) = &body.repo {
        if !valid_repo(repo) {
            return Err(ApiError::bad_request("repo must look like owner/repo"));
        }
    }
    if let Some(env) = &body.env {
        if !valid_env(env) {
            return Err(ApiError::bad_request(
                "env must be letters, digits, hyphens, underscores (max 255, not starting with `-` or `_`)",
            ));
        }
        if body.repo.is_none() {
            return Err(ApiError::bad_request(
                "env scope requires repo (owner/repo)",
            ));
        }
    }

    // The systemd credential is an operator-owned immutable scope: a live
    // `set` here would be overridden at the next startup when the credential
    // re-applies over config.toml — reject with guidance instead of silently
    // reverting.
    let credential = crate::config::load_credential_secrets()
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
    if credential_scope_conflict(
        &credential,
        body.repo.as_deref(),
        body.env.as_deref(),
        &name,
    ) {
        return Err(ApiError::conflict(format!(
            "secret `{name}` is managed by the systemd credential; edit the credential file \
             (re-encrypt with `systemd-creds encrypt --name=preloop-secrets ...`) instead"
        )));
    }

    // Persist first (unless the secrets store is memory-only): if the write
    // fails, the store stays untouched and the error surfaces instead of
    // silently diverging. In `secrets_store = "memory"` mode the write is
    // deliberately skipped — values exist for this process lifetime only.
    // The whole load → mutate → persist → publish sequence runs under
    // `secret_mutation`: two concurrent requests that both loaded the same
    // base config would otherwise drop one another's entry from the file
    // while both landed in the in-memory store. Lock order is
    // `secret_mutation` → `secrets` write guard; the global `inner` mutex is
    // never taken here.
    let _mutation = shared.state.secret_mutation.lock().await;
    let mut config = crate::config::load_config_from(&shared.state.config_path)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
    match (&body.repo, &body.env) {
        (Some(repo), Some(env)) => {
            config
                .env_secrets
                .entry(repo.clone())
                .or_default()
                .entry(env.clone())
                .or_default()
                .insert(name.clone(), value.clone());
        }
        (Some(repo), None) => {
            config
                .repo_secrets
                .entry(repo.clone())
                .or_default()
                .insert(name.clone(), value.clone());
        }
        (None, None) => {
            config.secrets.insert(name.clone(), value.clone());
        }
        (None, Some(_)) => unreachable!("env without repo rejected above"),
    }
    if !crate::config::store_memory(&config)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?
    {
        crate::config::write_config_to(&shared.state.config_path, &config)
            .map_err(|error| ApiError::internal(format!("{error:#}")))?;
    }

    let mut store = shared.state.secrets.write();
    match (&body.repo, &body.env) {
        (Some(repo), Some(env)) => {
            store
                .env
                .entry(repo.clone())
                .or_default()
                .entry(env.clone())
                .or_default()
                .insert(name, value);
        }
        (Some(repo), None) => {
            store
                .repo
                .entry(repo.clone())
                .or_default()
                .insert(name, value);
        }
        (None, None) => {
            store.global.insert(name, value);
        }
        (None, Some(_)) => unreachable!("env without repo rejected above"),
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Remove a stored secret. 404 when the name is not stored in that scope.
pub(crate) async fn delete_secret(
    State(shared): State<Arc<SharedState>>,
    Path(name): Path<String>,
    Query(query): Query<SecretQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if query.env.is_some() && query.repo.is_none() {
        return Err(ApiError::bad_request(
            "env scope requires repo (owner/repo)",
        ));
    }
    if let Some(repo) = &query.repo {
        if !valid_repo(repo) {
            return Err(ApiError::bad_request("repo must look like owner/repo"));
        }
    }
    if let Some(env) = &query.env {
        if !valid_env(env) {
            return Err(ApiError::bad_request(
                "env must be an alphanumeric name (letters, digits, - and _)",
            ));
        }
    }
    // Same immutable-scope rule as `set_secret`: credential-backed entries
    // re-apply at startup, so a config-file removal would either 404 or
    // silently reappear. The credential file is the only supported edit path.
    let credential = crate::config::load_credential_secrets()
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
    if credential_scope_conflict(
        &credential,
        query.repo.as_deref(),
        query.env.as_deref(),
        &name,
    ) {
        return Err(ApiError::conflict(format!(
            "secret `{name}` is managed by the systemd credential; edit the credential file \
             (re-encrypt with `systemd-creds encrypt --name=preloop-secrets ...`) instead"
        )));
    }
    // Same serialization as `set_secret`: load → mutate → persist → publish
    // is one critical section, so a concurrent set cannot resurrect the
    // deleted name in the file. Lock order: `secret_mutation` → `secrets`.
    let _mutation = shared.state.secret_mutation.lock().await;
    let config = crate::config::load_config_from(&shared.state.config_path)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
    let memory = crate::config::store_memory(&config)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;

    if memory {
        // The runtime store is the source of truth: values set through the
        // live API never reached the file, so a config-driven lookup would
        // 404 on exactly the secrets this mode exists to hold. Credential-
        // backed entries re-apply at restart — remove them from the
        // credential file to make the deletion permanent.
        let mut store = shared.state.secrets.write();
        if !remove_from_store(&mut store, &query, &name) {
            return Err(not_found_error(&name, &query));
        }
        return Ok(Json(serde_json::json!({ "ok": true })));
    }

    let mut config = config;
    let (removed, scope_key) = match (&query.repo, &query.env) {
        (Some(repo), Some(env)) => {
            let removed = config.env_secrets.get_mut(repo).is_some_and(|envs| {
                envs.get_mut(env)
                    .is_some_and(|map| map.remove(&name).is_some())
            });
            if removed {
                if config
                    .env_secrets
                    .get(repo)
                    .is_some_and(|envs| envs.get(env).is_some_and(BTreeMap::is_empty))
                {
                    config
                        .env_secrets
                        .get_mut(repo)
                        .expect("envs exists when env map exists")
                        .remove(env);
                }
                if config.env_secrets.get(repo).is_some_and(BTreeMap::is_empty) {
                    config.env_secrets.remove(repo);
                }
            }
            (removed, Some((repo.clone(), Some(env.clone()))))
        }
        (Some(repo), None) => {
            let removed = config
                .repo_secrets
                .get_mut(repo)
                .is_some_and(|map| map.remove(&name).is_some());
            if removed
                && config
                    .repo_secrets
                    .get(repo)
                    .is_some_and(BTreeMap::is_empty)
            {
                config.repo_secrets.remove(repo);
            }
            (removed, Some((repo.clone(), None)))
        }
        (None, None) => (config.secrets.remove(&name).is_some(), None),
        (None, Some(_)) => unreachable!("env without repo rejected above"),
    };
    if !removed {
        return Err(not_found_error(&name, &query));
    }
    crate::config::write_config_to(&shared.state.config_path, &config)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;

    let mut store = shared.state.secrets.write();
    let _ = scope_key;
    let _ = remove_from_store(&mut store, &query, &name);
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Remove a secret from the runtime store by scope, pruning empty maps.
fn remove_from_store(
    store: &mut crate::state::SecretStore,
    query: &SecretQuery,
    name: &str,
) -> bool {
    match (&query.repo, &query.env) {
        (Some(repo), Some(env)) => {
            let removed = store.env.get_mut(repo).is_some_and(|envs| {
                envs.get_mut(env)
                    .is_some_and(|map| map.remove(name).is_some())
            });
            if removed {
                if store
                    .env
                    .get(repo)
                    .is_some_and(|envs| envs.get(env).is_some_and(BTreeMap::is_empty))
                {
                    store.env.get_mut(repo).expect("envs exist").remove(env);
                }
                if store.env.get(repo).is_some_and(BTreeMap::is_empty) {
                    store.env.remove(repo);
                }
            }
            removed
        }
        (Some(repo), None) => {
            let removed = store
                .repo
                .get_mut(repo)
                .is_some_and(|map| map.remove(name).is_some());
            if removed && store.repo.get(repo).is_some_and(BTreeMap::is_empty) {
                store.repo.remove(repo);
            }
            removed
        }
        (None, None) => store.global.remove(name).is_some(),
        (None, Some(_)) => unreachable!("env without repo rejected above"),
    }
}

fn not_found_error(name: &str, query: &SecretQuery) -> ApiError {
    match (&query.repo, &query.env) {
        (Some(repo), Some(env)) => ApiError::not_found(format!(
            "no secret named {name} for {repo} in environment {env}"
        )),
        (Some(repo), None) => ApiError::not_found(format!("no secret named {name} for {repo}")),
        (None, None) => ApiError::not_found(format!("no secret named {name}")),
        (None, Some(_)) => unreachable!("env without repo rejected above"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_scope_conflict_matches_all_three_tiers() {
        let mut credential = crate::config::ConfigFile::default();
        credential.secrets.insert("GLOBAL".into(), "v".into());
        credential
            .repo_secrets
            .entry("owner/repo".into())
            .or_default()
            .insert("REPO".into(), "v".into());
        credential
            .env_secrets
            .entry("owner/repo".into())
            .or_default()
            .entry("prod".into())
            .or_default()
            .insert("ENV".into(), "v".into());

        assert!(credential_scope_conflict(&credential, None, None, "GLOBAL"));
        assert!(credential_scope_conflict(
            &credential,
            Some("owner/repo"),
            None,
            "REPO"
        ));
        assert!(credential_scope_conflict(
            &credential,
            Some("owner/repo"),
            Some("prod"),
            "ENV"
        ));
        // Wrong tier, wrong repo, or unknown name: no conflict.
        assert!(!credential_scope_conflict(
            &credential,
            Some("owner/repo"),
            Some("prod"),
            "REPO"
        ));
        assert!(!credential_scope_conflict(
            &credential,
            Some("other/repo"),
            None,
            "REPO"
        ));
        assert!(!credential_scope_conflict(&credential, None, None, "NOPE"));
    }
}
