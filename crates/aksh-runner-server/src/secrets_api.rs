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

#[derive(Deserialize)]
pub(crate) struct SetSecretBody {
    #[serde(default)]
    pub value: Option<String>,
    /// `owner/repo` scope; absent = global.
    #[serde(default)]
    pub repo: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct SecretQuery {
    #[serde(default)]
    pub repo: Option<String>,
}

/// List stored secret names (never values), scoped by repo when asked.
pub(crate) async fn list_secrets(
    State(shared): State<Arc<SharedState>>,
    Query(query): Query<SecretQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let store = shared.state.secrets.read();
    let mut secrets = Vec::new();
    match query.repo {
        Some(repo) => {
            if let Some(map) = store.repo.get(&repo) {
                for name in map.keys() {
                    secrets.push(serde_json::json!({ "name": name, "repo": repo }));
                }
            }
        }
        None => {
            for name in store.global.keys() {
                secrets.push(serde_json::json!({ "name": name, "repo": serde_json::Value::Null }));
            }
            for (repo, map) in &store.repo {
                for name in map.keys() {
                    secrets.push(serde_json::json!({ "name": name, "repo": repo }));
                }
            }
        }
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

    // Persist first: if the write fails, the store stays untouched and the
    // error surfaces instead of silently diverging.
    // The whole load → mutate → persist → publish sequence runs under
    // `secret_mutation`: two concurrent requests that both loaded the same
    // base config would otherwise drop one another's entry from the file
    // while both landed in the in-memory store. Lock order is
    // `secret_mutation` → `secrets` write guard; the global `inner` mutex is
    // never taken here.
    let _mutation = shared.state.secret_mutation.lock().await;
    let mut config = crate::config::load_config_from(&shared.state.config_path)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
    match &body.repo {
        Some(repo) => {
            config
                .repo_secrets
                .entry(repo.clone())
                .or_default()
                .insert(name.clone(), value.clone());
        }
        None => {
            config.secrets.insert(name.clone(), value.clone());
        }
    }
    crate::config::write_config_to(&shared.state.config_path, &config)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;

    let mut store = shared.state.secrets.write();
    match &body.repo {
        Some(repo) => {
            store
                .repo
                .entry(repo.clone())
                .or_default()
                .insert(name, value);
        }
        None => {
            store.global.insert(name, value);
        }
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Remove a stored secret. 404 when the name is not stored in that scope.
pub(crate) async fn delete_secret(
    State(shared): State<Arc<SharedState>>,
    Path(name): Path<String>,
    Query(query): Query<SecretQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Same serialization as `set_secret`: load → mutate → persist → publish
    // is one critical section, so a concurrent set cannot resurrect the
    // deleted name in the file. Lock order: `secret_mutation` → `secrets`.
    let _mutation = shared.state.secret_mutation.lock().await;
    let mut config = crate::config::load_config_from(&shared.state.config_path)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
    let (removed, repo_key) = match &query.repo {
        Some(repo) => {
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
            (removed, Some(repo.clone()))
        }
        None => (config.secrets.remove(&name).is_some(), None),
    };
    if !removed {
        return Err(ApiError::not_found(match &repo_key {
            Some(repo) => format!("no secret named {name} for {repo}"),
            None => format!("no secret named {name}"),
        }));
    }
    crate::config::write_config_to(&shared.state.config_path, &config)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;

    let mut store = shared.state.secrets.write();
    match &repo_key {
        Some(repo) => {
            if let Some(map) = store.repo.get_mut(repo) {
                map.remove(&name);
                if map.is_empty() {
                    store.repo.remove(repo);
                }
            }
        }
        None => {
            store.global.remove(&name);
        }
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}
