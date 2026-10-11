use super::*;

/// Deduplicate label strings case-insensitively, preserving first occurrence.
///
/// The official `actions/runner` builds labels as 3 system entries
/// (`self-hosted`, OS, arch) plus any user-supplied labels via `--labels`. A
/// user who adds a label that already exists as a system entry — most
/// commonly `self-hosted`, which is the default `config.sh` suggestion — would
/// otherwise produce a duplicate that violates the `(runner_id, label)`
/// primary key on `runner_labels` and surface as a 500. Dispatch matching in
/// `runtime_scheduling::job_matches_runner` is already case-insensitive, so
/// collapsing here keeps the stored set consistent with the matcher without
/// changing semantics.
fn dedupe_labels_ci(labels: &[String]) -> Vec<String> {
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(labels.len());
    let mut out: Vec<String> = Vec::with_capacity(labels.len());
    for label in labels {
        if seen.insert(label.to_lowercase()) {
            out.push(label.clone());
        }
    }
    out
}

/// Build the typed `TaskAgent.Properties` payload shared by agent
/// registration, lookup, and update responses.
///
/// Legacy v2 settings are always present because the runner already depends
/// on them. Runner-admin additions remain opt-in: unset environment variables
/// do not add wire keys to the response.
fn runner_admin_properties() -> serde_json::Map<String, serde_json::Value> {
    let mut properties = serde_json::Map::new();
    let runner_root = runner_server_url();
    let options = runner_v2_connection_options();
    let server_url_v2 = options
        .broker_url
        .clone()
        .unwrap_or_else(|| runner_root.clone());
    properties.insert(
        "RequireFipsCryptography".to_owned(),
        json!({"$type": "System.Boolean", "$value": false}),
    );
    properties.insert(
        "ServerUrl".to_owned(),
        json!({"$type": "System.String", "$value": runner_root}),
    );
    properties.insert(
        "ServerUrlV2".to_owned(),
        json!({"$type": "System.String", "$value": server_url_v2}),
    );
    properties.insert(
        "UseV2Flow".to_owned(),
        json!({"$type": "System.Boolean", "$value": true}),
    );

    if let Some(auth_url_v2) = options.auth_url_v2 {
        properties.insert(
            "EnableAuthMigrationByDefault".to_owned(),
            json!({"$type": "System.Boolean", "$value": true}),
        );
        properties.insert(
            "AuthorizationUrlV2".to_owned(),
            json!({"$type": "System.String", "$value": auth_url_v2}),
        );
    }
    if let Some(broker_url) = options.broker_url {
        properties.insert(
            "BrokerUrl".to_owned(),
            json!({"$type": "System.String", "$value": broker_url}),
        );
    }
    if let Some(use_runner_admin_flow) = options.use_runner_admin_flow {
        properties.insert(
            "UseRunnerAdminFlow".to_owned(),
            json!({"$type": "System.Boolean", "$value": use_runner_admin_flow}),
        );
    }
    properties
}

pub async fn register_runner(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<RunnerRegistrationRequest>,
) -> Result<Json<RegisteredRunner>, ApiError> {
    let runner = register_runner_inner(&shared, request).await?;
    {
        // Native-bearer registration is engine-authorized: pair the fresh
        // runner with a pending pool-assigned job immediately, same as
        // `register_runner_native`.
        shared
            .state
            .backend
            .pair_runner(runner.id)
            .await
            .map_err(ApiError::from)?;
    }
    if let Err(error) = persist_full_state(&shared).await {
        if let Err(purge_error) = purge_runner_identity(&shared, runner.id).await {
            tracing::error!(
                ?purge_error,
                runner_id = runner.id,
                "runner rollback purge failed — listen token remains valid"
            );
        }
        return Err(error);
    }
    Ok(Json(runner))
}

/// Mutate in-memory registration state without persisting. The public entry
/// points persist once, after every identity-bearing mutation (OAuth
/// `client_id`, pool pairing) has happened, so a restart cannot lose the
/// runner's credentials.
async fn register_runner_inner(
    shared: &Arc<SharedState>,
    request: RunnerRegistrationRequest,
) -> Result<RegisteredRunner, ApiError> {
    let parsed_public_key = request
        .public_key
        .as_deref()
        .map(AgentRsaPublicKey::parse)
        .transpose()
        .map_err(ApiError::from)?;
    let row = shared
        .state
        .backend
        .register_runner(crate::control::backend::RegisterRunner {
            name: request.name,
            labels: dedupe_labels_ci(&request.labels),
            ephemeral: request.ephemeral,
            public_key: request.public_key.clone(),
            rsa_public_key: parsed_public_key,
            client_id: None,
            runner_group_id: request.runner_group_id,
            runner_group_name: request.runner_group_name,
            pool_proven: false,
        })
        .await
        .map_err(ApiError::from)?;
    // `runner_public_keys` (the PEM/string form) is node-local — it is not part
    // of the scheduling working set the backend owns.
    if let Some(public_key) = &row.runner.public_key {
        let mut inner = shared.state.inner.lock().await;
        inner
            .runner_public_keys
            .insert(row.runner.id, public_key.clone());
    }
    Ok(row.runner)
}

/// Retired: the control backend commits every mutation durably inside its own
/// transaction, so the legacy `StoreSnapshot`/`store_inner` dual-write is no
/// longer needed to survive a restart. Kept as a no-op so the registration /
/// session call sites read unchanged; the whole helper is deleted once the
/// snapshot store is removed.
async fn persist_full_state(_shared: &Arc<SharedState>) -> Result<(), ApiError> {
    Ok(())
}
/// Wrapper for the native registration route: native-bearer gated, so the
/// registration is engine-authorized and the fresh runner may be paired
/// with a pending pool-assigned job immediately.
pub async fn register_runner_native(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<RunnerRegistrationRequest>,
) -> Result<Json<RegisteredRunner>, ApiError> {
    let runner = register_runner_inner(&shared, request).await?;
    {
        shared
            .state
            .backend
            .pair_runner(runner.id)
            .await
            .map_err(ApiError::from)?;
    }
    if let Err(error) = persist_full_state(&shared).await {
        if let Err(purge_error) = purge_runner_identity(&shared, runner.id).await {
            tracing::error!(
                ?purge_error,
                runner_id = runner.id,
                "runner rollback purge failed — listen token remains valid"
            );
        }
        return Err(error);
    }
    Ok(Json(runner))
}

/// Optional run-scoped query for [`list_runners_native`].
#[derive(Debug, Deserialize)]
pub struct RunnerListQuery {
    #[serde(default)]
    run_id: Option<RunId>,
}

/// GET /api/v1/runners — list the runners currently registered with the
/// control plane.
///
/// Read-only operator surface (native bearer). The CLI uses it to tell a
/// queued run apart from a dead one: with `?run_id=`, `queued` and
/// `claimable` report whether any registered runner could actually claim the
/// run's ready-queue jobs. Zero claimable means no job will ever be picked
/// up, however long `still waiting` prints — even when other runners are
/// registered whose labels match nothing in the queue. Without `run_id` the
/// response is the plain list.
pub async fn list_runners_native(
    State(shared): State<Arc<SharedState>>,
    Query(query): Query<RunnerListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let listing = shared
        .state
        .backend
        .list_runners(query.run_id)
        .await
        .map_err(ApiError::from)?;
    let runners: Vec<serde_json::Value> = listing
        .runners
        .iter()
        .map(|runner| {
            json!({
                "id": runner.id,
                "name": runner.name,
                "labels": runner.labels,
            })
        })
        .collect();
    let mut response = json!({
        "count": runners.len(),
        "runners": runners,
    });
    if let Some(run_queue) = listing.run_queue {
        response["queued"] = json!(run_queue.queued);
        response["claimable"] = json!(run_queue.claimable);
    }
    Ok(Json(response))
}

pub async fn create_session(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<RunnerSessionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let session_id = uuid::Uuid::new_v4();

    // The AES session key is derived from the cluster key and the session id
    // (never stored): any node re-derives it to encrypt this session's
    // messages.
    let session_enc = shared.state.session_encryption(&session_id.to_string());

    let runner_public_key = shared
        .state
        .backend
        .runner_rsa_public_key(request.runner_id)
        .await
        .map_err(ApiError::from)?;
    let (key_bytes, encrypted) = if let Some(public_key) = runner_public_key {
        (public_key.wrap_key(&session_enc.key)?, true)
    } else {
        (session_enc.key.clone(), false)
    };
    let key_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key_bytes);

    shared
        .state
        .backend
        .open_runner_session(crate::control::OpenRunnerSession {
            session_id: session_id.to_string(),
            runner_id: Some(request.runner_id),
            protocol: crate::control::SessionProtocol::Broker,
            verified: false,
            // The legacy AzDO/AgentSession create may name an agent id whose
            // registration arrives later.
            require_live_runner: false,
        })
        .await
        .map_err(ApiError::from)?;

    info!(%session_id, runner_id = request.runner_id, encrypted, "session created with AES key");

    Ok(Json(json!({
        "sessionId": session_id.to_string(),
        "encryptionKey": {
            "value": key_b64,
            "encrypted": encrypted
        }
    })))
}

use preloop_gha_protocol::crypto::RsaOaepHash;

pub async fn create_session_disttask(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    // For the AzDO message path, generate an unencrypted session key directly.
    // RSA-wrapped keys are only needed for real internet-facing GHES; for local
    // use the runner's from_rsaparams may not reconstruct the keypair correctly.
    let session_id = uuid::Uuid::new_v4();
    let session_enc = shared.state.session_encryption(&session_id.to_string());

    let requested_runner_id = body
        .pointer("/agent/id")
        .and_then(serde_json::Value::as_i64);
    // A verified listen token decides the binding; the body's self-declared
    // `agent.id` never elevates privilege. Without a token the legacy
    // body-driven binding stays so older clients keep working — such sessions
    // are treated as unverified at claim time, which is where the pool's
    // assignment enforcement lives.
    let verified = identity.and_then(|axum::Extension(id)| id.runner_id);
    let runner_id = match (verified, requested_runner_id) {
        (Some(verified), Some(requested)) if verified != requested => {
            return Err(ApiError::forbidden(format!(
                "listen token names runner {verified} but session body requests agent {requested}"
            )));
        }
        (Some(verified), _) => Some(verified),
        (None, requested) => requested,
    };

    let use_fips_encryption = body
        .get("useFipsEncryption")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let runner_public_key = match runner_id {
        Some(id) => shared
            .state
            .backend
            .runner_rsa_public_key(id)
            .await
            .map_err(ApiError::from)?,
        None => None,
    };
    let (key_bytes, _encrypted) = if use_fips_encryption {
        let Some(public_key) = runner_public_key else {
            return Err(ApiError::bad_request(
                "FIPS session encryption requires a registered RSA public key",
            ));
        };
        (
            public_key.wrap_key_with_hash(&session_enc.key, RsaOaepHash::Sha256)?,
            true,
        )
    } else {
        (session_enc.key.clone(), false)
    };
    let key_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key_bytes);
    let azdo_opt_in = body
        .get("preloopAzdo")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    // The official control plane answers 409 when the runner already holds a
    // live session. Only *verified* sessions count: an unverified compat
    // session (no listen token) must not squat a runner id.
    shared
        .state
        .backend
        .open_runner_session(crate::control::OpenRunnerSession {
            session_id: session_id.to_string(),
            runner_id,
            protocol: if azdo_opt_in {
                crate::control::SessionProtocol::Azdo
            } else {
                crate::control::SessionProtocol::Broker
            },
            verified: runner_id.is_some() && verified == runner_id,
            // The distributedtask session create serves legacy clients that
            // may name an agent id before its registration lands.
            require_live_runner: false,
        })
        .await
        .map_err(ApiError::from)?;
    persist_full_state(&shared).await?;

    let owner_name = body
        .get("ownerName")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    info!(%session_id, "AzDO session created (unencrypted key)");

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "sessionId": session_id.to_string(),
            "ownerName": owner_name,
            "agent": {
                "authorization": {},
            },
            "assignmentQueued": false,
            "orchestrationId": "",
            "encryptionKey": {
                "value": key_b64,
                "encrypted": false,
            },
        })),
    ))
}

pub async fn delete_session(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Path((_pool_id, session_id)): Path<(i64, String)>,
) -> Result<StatusCode, ApiError> {
    let caller = crate::auth::admin_caller(
        &shared.state,
        &headers,
        identity.as_ref().map(|axum::Extension(id)| id),
    )?;
    if caller == crate::auth::AdminCaller::RunnerManager {
        return Err(ApiError::forbidden(
            "registration tokens cannot delete sessions",
        ));
    }
    let caller_runner_id = match caller {
        crate::auth::AdminCaller::Runner(runner_id) => Some(runner_id),
        _ => None,
    };
    shared
        .state
        .backend
        .close_runner_session(&session_id, caller_runner_id)
        .await
        .map_err(ApiError::from)?;
    if let Err(error) = persist_full_state(&shared).await {
        tracing::warn!(?error, "failed to persist deleted runner session");
    }
    shared.state.message_notify.notify_waiters();
    shared.state.sampler_notify.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}
/// DELETE /runner/server/_apis/distributedtask/pools/:pool_id/agents/:agent_id
/// Agent deregistration is idempotent for the management credential; a live
/// runner listen token may only purge its own identity.
/// Purges everything the runner's identity was good for: OAuth client id,
/// RSA key, sessions, and any job assignments it still held, so a stolen
/// identity cannot mint tokens or receive work after the machine is gone.
/// Jobs it was assigned but never claimed go back to pool-pending so the
/// pool provisions a replacement machine for them.
/// Returns null response body in JSON to match official.
pub async fn delete_agent(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Path((_pool_id, agent_id)): Path<(i64, i64)>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    // A runner may deregister itself and nothing else; the system token may
    // deregister anything. Purging another runner revokes its listen tokens
    // and requeues its work, so this is a live denial-of-service otherwise.
    let caller = crate::auth::admin_caller(
        &shared.state,
        &headers,
        identity.as_ref().map(|axum::Extension(id)| id),
    )?;
    purge_runner_identity_guarded(&shared, caller, agent_id).await?;
    Ok((StatusCode::NO_CONTENT, Json(serde_json::Value::Null)))
}

pub async fn purge_runner_identity_guarded(
    shared: &Arc<SharedState>,
    caller: crate::auth::AdminCaller,
    agent_id: i64,
) -> Result<(), ApiError> {
    // Caller-guard + active-session check + purge run in ONE writer
    // transaction: a session created between a separate read and the purge
    // would let a RunnerManager token delete an active runner (TOCTOU).
    let guard = match caller {
        crate::auth::AdminCaller::System => crate::control::PurgeGuard::System,
        crate::auth::AdminCaller::Runner(runner_id) => {
            crate::control::PurgeGuard::Runner(runner_id)
        }
        crate::auth::AdminCaller::RunnerManager => crate::control::PurgeGuard::RegistrationToken,
    };
    let retired = shared
        .state
        .backend
        .purge_runner_guarded(agent_id, guard)
        .await
        .map_err(ApiError::from)?
        .unwrap_or_default();
    // `runner_public_keys` and the live-log feeds are node-local — outside
    // the scheduling tx. An abandoned attempt's feed closes so its
    // `logs -f` followers exit; the retry streams under its own identity.
    {
        let mut inner = shared.state.inner.lock().await;
        inner.runner_public_keys.remove(&agent_id);
        for agent_job_id in &retired {
            crate::live_logs::close_live_log(&mut inner, &agent_job_id.to_string());
        }
    }
    shared.state.message_notify.notify_waiters();
    shared.state.sampler_notify.notify_waiters();
    Ok(())
}

/// Remove every trace of a runner identity: keys, client ids, sessions and
/// assignments. Shared by agent deregistration and pool machine teardown.
///
/// This *is* the revocation mechanism for listen tokens (`auth.rs`: a JWT
/// that outlives its registration must stop authenticating), so a failure
/// here leaves a live credential — it must surface, not be dropped.
pub async fn purge_runner_identity(
    shared: &Arc<SharedState>,
    runner_id: i64,
) -> Result<(), ApiError> {
    purge_runner_identity_guarded(shared, crate::auth::AdminCaller::System, runner_id).await
}

/// Remove a runner identity, optionally requiring it to remain sessionless.
async fn purge_runner_identity_with_phantom_check(
    shared: &Arc<SharedState>,
    runner_id: i64,
    only_if_phantom: bool,
) -> bool {
    // Phantom-check + purge in ONE writer transaction: a session created
    // between a separate read and the purge would let us delete a runner that
    // just went active (TOCTOU).
    let guard = if only_if_phantom {
        crate::control::PurgeGuard::IfPhantom
    } else {
        crate::control::PurgeGuard::System
    };
    let Some(retired) = shared
        .state
        .backend
        .purge_runner_guarded(runner_id, guard)
        .await
        .ok()
        .flatten()
    else {
        if only_if_phantom {
            tracing::info!(
                runner_id,
                "runner established session before phantom purge; skipping cleanup"
            );
        }
        return false;
    };
    {
        let mut inner = shared.state.inner.lock().await;
        inner.runner_public_keys.remove(&runner_id);
        for agent_job_id in &retired {
            crate::live_logs::close_live_log(&mut inner, &agent_job_id.to_string());
        }
    }
    shared.state.message_notify.notify_waiters();
    shared.state.sampler_notify.notify_waiters();
    true
}

pub async fn purge_phantom_runner(shared: &Arc<SharedState>, runner_id: i64) -> bool {
    purge_runner_identity_with_phantom_check(shared, runner_id, true).await
}

/// A server restart destroys every process-owned ephemeral VM. Their durable
/// runner registrations and sessions cannot reconnect, so purge them as one
/// transaction before serving or they masquerade as idle capacity forever.
pub async fn purge_restored_ephemeral_runners(shared: &Arc<SharedState>) {
    let runner_ids: Vec<i64> = shared
        .state
        .backend
        .ephemeral_runner_ids()
        .await
        .unwrap_or_default();
    if runner_ids.is_empty() {
        return;
    }

    for runner_id in &runner_ids {
        let _ = shared.state.backend.purge_runner(*runner_id).await;
    }
    {
        let mut inner = shared.state.inner.lock().await;
        for runner_id in &runner_ids {
            inner.runner_public_keys.remove(runner_id);
        }
    }
    info!(
        count = runner_ids.len(),
        "purged restored ephemeral runner identities"
    );
    shared.state.message_notify.notify_waiters();
    shared.state.sampler_notify.notify_waiters();
}

/// DELETE /runner/server/_apis/distributedtask/pools/:pool_id/sessions (no session_id)
/// Broker-side session teardown: the runner deletes the session-less path on the broker host.
/// Return 204 unconditionally; the concrete session was already cleaned up individually.
/// Returns null response body in JSON to match official.
pub async fn delete_sessions_for_pool(
    Path(_pool_id): Path<i64>,
) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NO_CONTENT, Json(serde_json::Value::Null))
}

pub fn rsa_public_key_xml_from_value(value: &serde_json::Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_owned());
    }
    let modulus = value.get("modulus").and_then(|v| v.as_str())?;
    let exponent = value.get("exponent").and_then(|v| v.as_str())?;
    Some(format!(
        "<RSAKeyValue><Modulus>{modulus}</Modulus><Exponent>{exponent}</Exponent></RSAKeyValue>"
    ))
}

pub fn task_agent_public_key(request: &serde_json::Value) -> Option<String> {
    request
        .get("authorization")
        .and_then(|authorization| authorization.get("publicKey"))
        .and_then(rsa_public_key_xml_from_value)
        .or_else(|| {
            request
                .get("publicKey")
                .and_then(rsa_public_key_xml_from_value)
        })
}

/// GET /_apis/v1/Agent/:pool_id — look up runner by agentName query param.
/// Returns 200 with the agent if found, or 200 with an empty array if not found.
/// The runner treats a non-empty result as "agent exists" and empty as "needs registration".
pub async fn agent_lookup(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let Some(agent_name) = params.get("agentName") else {
        return Json(json!({"count": 0, "value": []}));
    };
    let found = shared
        .state
        .backend
        .lookup_agent(agent_name)
        .await
        .unwrap_or(None);
    if let Some((runner, client_id)) = found {
        return Json(json!({"count": 1, "value": [{
            "id": runner.id,
            "name": runner.name,
            "version": "2.335.1",
            "osDescription": "Linux",
            "enabled": true,
            "status": "online",
            "ephemeral": runner.ephemeral,
            "maxParallelism": 1,
            "currentParallelism": 0,
            "disableUpdate": false,
            "isElastic": false,
            "isVirtual": false,
            "provisioningState": "Provisioned",
            "queueName": format!("taskagent-{}", runner.id),
            "runnerGroupId": runner.runner_group_id.unwrap_or(1),
            "runnerGroupName": runner.runner_group_name.clone(),
            "owningTenant": null,
            "createdOn": "2026-01-01T00:00:00Z",
            "lastConnectedOn": "2026-01-01T00:00:00",
            "labels": runner.labels.iter().enumerate().map(|(i, l)| json!({"id": i + 1, "name": l, "type": "user"})).collect::<Vec<_>>(),
            "authorization": {
                "clientId": client_id,
                "publicKey": {"exponent": "AQAB", "modulus": ""}
            },
            "properties": runner_admin_properties()
        }]}));
    }
    // Return empty collection (not 404) — runner expects VssJsonCollectionWrapper format
    Json(json!({"count": 0, "value": []}))
}

/// GET /_apis/v1/Agent/:pool_id/:agent_id — look up runner by agentId in path.
/// The runner constructs URLs from the service definition template `{poolId}/{agentId}`.
/// For lookups it uses agentId=0; for registration it POSTs.
pub async fn agent_lookup_by_id(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, _agent_id)): Path<(i64, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    agent_lookup(State(shared), Path(_pool_id), Query(params)).await
}

pub async fn runner_pools() -> Json<serde_json::Value> {
    let instance_id = crate::connection::INSTANCE_ID;
    Json(json!({
        "count": 2,
        "value": [{
            "id": 1,
            "name": "Default",
            "isHosted": false,
            "agentCloudId": null,
            "autoSize": true,
            "createdOn": "2026-01-01T00:00:00Z",
            "isInternal": true,
            "scope": instance_id,
            "size": 1,
            "targetSize": null
        }, {
            "id": 2,
            "name": "GitHub Actions",
            "isHosted": true,
            "agentCloudId": 1,
            "autoSize": true,
            "createdOn": "2026-01-01T00:00:00Z",
            "isInternal": false,
            "scope": instance_id,
            "size": 1,
            "targetSize": 1
        }]
    }))
}

/// Compat handler: register runner via AzDO Agent path.
pub async fn register_runner_compat(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, _agent_id)): Path<(i64, String)>,
    headers: axum::http::HeaderMap,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // The runner sends a TaskAgent-style body; extract what we need.
    let name = request
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("runner")
        .to_owned();
    let labels: Vec<String> = request
        .get("labels")
        .and_then(|v| v.as_array())
        .map(|arr| {
            let raw: Vec<String> = arr
                .iter()
                .filter_map(|v| {
                    v.as_str()
                        .or_else(|| v.get("name").and_then(|name| name.as_str()))
                        .map(str::to_owned)
                })
                .collect();
            dedupe_labels_ci(&raw)
        })
        .unwrap_or_default();
    let ephemeral = request
        .get("ephemeral")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let runner_group_id = request
        .get("runnerGroupId")
        .or_else(|| request.get("runner_group_id"))
        .and_then(serde_json::Value::as_i64);
    let runner_group_name = request
        .get("runnerGroupName")
        .or_else(|| request.get("runner_group_name"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let public_key_xml = task_agent_public_key(&request);
    let public_key_object = request
        .get("authorization")
        .and_then(|authorization| authorization.get("publicKey"))
        .cloned()
        .or_else(|| request.get("publicKey").cloned())
        .unwrap_or_else(|| {
            json!({
                "exponent": "AQAB",
                "modulus": ""
            })
        });
    let reg_request = RunnerRegistrationRequest {
        name: name.clone(),
        labels,
        ephemeral,
        public_key: public_key_xml,
        runner_group_id,
        runner_group_name,
    };
    let provision_token = headers
        .get("x-preloop-provision-token")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bearer_authorized = crate::auth::bearer_from_headers(&headers).is_some_and(|token| {
        crate::auth::runner_registration_bearer_authorized(&shared.state, token)
    });
    let provision_issued_at = provision_token
        .as_deref()
        .and_then(|token| crate::auth::consume_pending_provision_token(&shared.state, token));
    let provision_authorized = provision_issued_at.is_some();
    if !bearer_authorized && !provision_authorized {
        return Err(ApiError::unauthorized(
            "runner registration credential required",
        ));
    }
    let result = match register_runner_inner(&shared, reg_request).await {
        Ok(result) => result,
        Err(error) => {
            if let (Some(token), Some(issued_at)) =
                (provision_token.as_deref(), provision_issued_at)
            {
                crate::auth::restore_pending_provision_token(&shared.state, token, issued_at);
            }
            return Err(error);
        }
    };
    let client_id = uuid::Uuid::new_v4().to_string();
    {
        let runner_id = result.id;
        let pair = provision_authorized
            .then(|| provision_token.clone())
            .flatten();
        // The OAuth client id must be durable before the runner's next token
        // request. Pairing is gated on the one-time provision token the pool
        // generated host-side — a rogue process on another machine cannot
        // mint it, so it cannot steal pairings.
        shared
            .state
            .backend
            .bind_runner_client(runner_id, &client_id, pair.is_some())
            .await
            .map_err(ApiError::from)?;
        // Mirror into the consolidated pool handle so the sampler's
        // pending-registration count drops with the consume (node-local).
        if let Some(token) = pair.as_deref() {
            shared.state.pool_status.remove_pending(token);
        }
    }
    // One persist after every identity-bearing mutation, so client_id and any
    // pairing land in the same transaction as the runner row.
    if let Err(error) = persist_full_state(&shared).await {
        if let (Some(token), Some(issued_at)) = (provision_token.as_deref(), provision_issued_at) {
            crate::auth::restore_pending_provision_token(&shared.state, token, issued_at);
            shared
                .state
                .pool_status
                .insert_pending(token.to_owned(), issued_at);
        }
        if let Err(purge_error) = purge_runner_identity(&shared, result.id).await {
            tracing::error!(
                ?purge_error,
                runner_id = result.id,
                "provision rollback purge failed — listen token remains valid"
            );
        }
        return Err(error);
    }
    Ok(Json(json!({
        "id": result.id,
        "name": result.name,
        "version": request.get("version").and_then(|v| v.as_str()).unwrap_or("2.335.1"),
        "osDescription": request.get("osDescription").and_then(|v| v.as_str()).unwrap_or("Linux"),
        "enabled": true,
        "status": "offline",
        "ephemeral": ephemeral,
        "maxParallelism": 1,
        "currentParallelism": 0,
        "disableUpdate": false,
        "isElastic": false,
        "isVirtual": false,
        "provisioningState": "Provisioned",
        "queueName": format!("taskagent-{}", result.id),
        "runnerGroupId": result.runner_group_id.unwrap_or(1),
        "runnerGroupName": result.runner_group_name,
        "owningTenant": null,
        "createdOn": "2026-01-01T00:00:00Z",
        "labels": result.labels.iter().enumerate().map(|(i, l)| json!({"id": i + 1, "name": l, "type": "user"})).collect::<Vec<_>>(),
        "authorization": {
            "authorizationUrl": format!("{}/_apis/v1/oauth2/token", runner_server_url()),
            "clientId": client_id,
            "publicKey": public_key_object
        },
        "properties": runner_admin_properties()
    })))
}

/// Compat handler for the official runner's PUT replacement flow. The old
/// identity is purged before a fresh registration is created, so the PUT does
/// not leave two live runners and the old listen token is revoked.
pub async fn replace_runner_compat(
    State(shared): State<Arc<SharedState>>,
    Path((pool_id, agent_id)): Path<(i64, String)>,
    headers: axum::http::HeaderMap,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let old_id = agent_id
        .parse::<i64>()
        .map_err(|_| ApiError::bad_request("agent id must be numeric"))?;
    let exists = shared
        .state
        .backend
        .runner_exists(old_id)
        .await
        .unwrap_or(false);
    if !exists {
        return Err(ApiError::not_found("runner to replace not found"));
    }
    purge_runner_identity(&shared, old_id).await?;
    register_runner_compat(
        State(shared),
        Path((pool_id, "0".to_owned())),
        headers,
        Json(request),
    )
    .await
}

/// Compat handler for the official runner's in-place agent update:
/// `PUT /_apis/distributedtask/pools/{pool}/agents/{id}`. The runner PUTs its
/// current label/name set against the id it already holds and expects the
/// same id back — unlike `replace_runner_compat`, which purges and
/// re-registers under a fresh id for the `/_apis/v1/Agent` flow.
pub(crate) async fn update_agent(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Path((_pool_id, agent_id)): Path<(i64, String)>,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runner_id = agent_id
        .parse::<i64>()
        .map_err(|_| ApiError::bad_request("agent id must be numeric"))?;
    // A runner may refresh its own name/labels and nothing else; labels are
    // the dispatch predicate, so rewriting a peer's row starves it or makes
    // it claim jobs it was never provisioned for. The system token (and the
    // registration credential behind the management flows) stays unrestricted.
    let caller = crate::auth::admin_caller(
        &shared.state,
        &headers,
        identity.as_ref().map(|axum::Extension(id)| id),
    )?;
    if let crate::auth::AdminCaller::Runner(caller_runner_id) = caller
        && caller_runner_id != runner_id
    {
        return Err(ApiError::forbidden(
            "a runner may only update its own agent",
        ));
    }
    let name = request
        .get("name")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let labels: Option<Vec<String>> = request.get("labels").and_then(|v| v.as_array()).map(|arr| {
        let raw: Vec<String> = arr
            .iter()
            .filter_map(|v| {
                v.as_str()
                    .or_else(|| v.get("name").and_then(|name| name.as_str()))
                    .map(str::to_owned)
            })
            .collect();
        dedupe_labels_ci(&raw)
    });
    let row = shared
        .state
        .backend
        .update_runner(runner_id, name, labels)
        .await
        .map_err(ApiError::from)?;
    let runner = row.runner;
    Ok(Json(json!({
        "id": runner.id,
        "name": runner.name,
        "enabled": true,
        "status": "online",
        "ephemeral": runner.ephemeral,
        "maxParallelism": 1,
        "currentParallelism": 0,
        "disableUpdate": false,
        "provisioningState": "Provisioned",
        "runnerGroupId": runner.runner_group_id.unwrap_or(1),
        "runnerGroupName": runner.runner_group_name,
        "labels": runner.labels.iter().enumerate().map(|(i, l)| json!({"id": i + 1, "name": l, "type": "user"})).collect::<Vec<_>>(),
        "properties": runner_admin_properties()
    })))
}

/// Compat handler: register runner via `/_apis/v1/Agent/:pool_id` (no agent_id in path).
pub async fn register_runner_compat_pool_only(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    headers: axum::http::HeaderMap,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    register_runner_compat(
        State(shared),
        Path((_pool_id, "0".to_owned())),
        headers,
        Json(request),
    )
    .await
}

/// Compat handler: create session via AzDO AgentSession path.
pub async fn create_session_compat(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, _session_id)): Path<(i64, String)>,
    headers: axum::http::HeaderMap,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let requested_runner_id = body
        .get("agent")
        .and_then(|a| a.get("id"))
        .and_then(|v| v.as_i64());
    let system_authorized =
        crate::auth::bearer_from_headers(&headers) == Some(shared.state.system_token.as_str());
    let runner_id = if system_authorized {
        requested_runner_id.unwrap_or(1)
    } else {
        let verified = identity
            .and_then(|axum::Extension(id)| id.runner_id)
            .ok_or_else(|| ApiError::unauthorized("runner listen token required"))?;
        if let Some(requested) = requested_runner_id
            && requested != verified
        {
            return Err(ApiError::forbidden(format!(
                "listen token names runner {verified} but session body requests agent {requested}"
            )));
        }
        verified
    };
    let name = body
        .get("agent")
        .and_then(|a| a.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("runner")
        .to_owned();
    let result = create_session(
        State(shared),
        Json(RunnerSessionRequest { runner_id, name }),
    )
    .await?;
    Ok(result)
}

/// Compat handler: next message via AzDO Message path.
pub async fn next_message_compat(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<(StatusCode, Json<Option<azdo::TaskAgentMessage>>), ApiError> {
    next_message(State(shared), identity, Query(params)).await
}
/// POST /api/v1/runners/purge — orchestrator-facing runner deregistration:
/// purges the identity AND requeues any claimed-but-unfinished job, so a
/// machine torn down mid-job stops stalling the job until the lease reaper.
pub async fn purge_runners_by_name(
    State(shared): State<Arc<SharedState>>,
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    let name = body
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "name is required" })),
        );
    }
    let id_or_ids: Vec<i64> = shared
        .state
        .backend
        .runner_ids_named(name)
        .await
        .unwrap_or_default();
    let mut purge_failures = 0usize;
    for id in &id_or_ids {
        if let Err(error) = purge_runner_identity(&shared, *id).await {
            purge_failures += 1;
            tracing::error!(
                ?error,
                runner_id = *id,
                "runner teardown purge failed — listen token remains valid"
            );
        }
    }
    let purged = id_or_ids.len() - purge_failures;
    let status = if purge_failures == 0 {
        StatusCode::OK
    } else {
        // Partial failure: some listen tokens are still valid. 207-style —
        // report what actually happened so the caller can retry the rest.
        StatusCode::MULTI_STATUS
    };
    (
        status,
        Json(json!({ "purged": purged, "failed": purge_failures, "ids": id_or_ids })),
    )
}
