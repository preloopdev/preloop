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

pub(crate) async fn register_runner(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<RunnerRegistrationRequest>,
) -> Result<Json<RegisteredRunner>, ApiError> {
    let runner = register_runner_inner(&shared, request).await?;
    persist_full_state(&shared).await?;
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
    let mut inner = shared.state.inner.lock().await;
    inner.next_runner_id += 1;
    let runner_id = inner.next_runner_id;
    let public_key = request.public_key.clone();
    let runner = RegisteredRunner {
        id: runner_id,
        name: request.name,
        labels: dedupe_labels_ci(&request.labels),
        ephemeral: request.ephemeral,
        public_key,
        runner_group_id: request.runner_group_id,
        runner_group_name: request.runner_group_name,
    };
    if let Some(public_key) = &runner.public_key {
        inner
            .runner_public_keys
            .insert(runner_id, public_key.clone());
    }
    if let Some(public_key) = parsed_public_key {
        inner.runner_rsa_public_keys.insert(runner_id, public_key);
    }
    inner.runners.insert(runner.id, runner.clone());
    Ok(runner)
}

/// Capture the full in-memory state under the lock and persist it after the
/// lock is released. Registration is the one surface where a persistence
/// failure rejects the request: a runner that disappears from the store after
/// the client was told it registered would be worse than a retryable error.
async fn persist_full_state(shared: &Arc<SharedState>) -> Result<(), ApiError> {
    let snapshot = {
        let inner = shared.state.inner.lock().await;
        crate::store::StoreSnapshot::from_inner(&inner)
    };
    shared
        .state
        .store
        .store_inner(&snapshot)
        .await
        .map_err(|error| ApiError::internal(format!("failed to persist runner state: {error}")))
}

/// Wrapper for the native registration route: native-bearer gated, so the
/// registration is engine-authorized and the fresh runner may be paired
/// with a pending pool-assigned job immediately.
pub(crate) async fn register_runner_native(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<RunnerRegistrationRequest>,
) -> Result<Json<RegisteredRunner>, ApiError> {
    let runner = register_runner_inner(&shared, request).await?;
    {
        let mut inner = shared.state.inner.lock().await;
        crate::runtime_scheduling::pair_registered_runner(&mut inner, runner.id);
    }
    persist_full_state(&shared).await?;
    Ok(Json(runner))
}

/// GET /api/v1/runners — list the runners currently registered with the
/// control plane.
///
/// Read-only operator surface (native bearer). The CLI uses it to tell a
/// queued run apart from a dead one: zero runners means no job will ever be
/// picked up, however long `still waiting` prints.
pub(crate) async fn list_runners_native(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let runners: Vec<serde_json::Value> = inner
        .runners
        .values()
        .map(|runner| {
            json!({
                "id": runner.id,
                "name": runner.name,
                "labels": runner.labels,
            })
        })
        .collect();
    Ok(Json(json!({
        "count": runners.len(),
        "runners": runners,
    })))
}

pub(crate) async fn create_session(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<RunnerSessionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let session_id = uuid::Uuid::new_v4();

    // Generate AES session key
    let session_enc = SessionEncryption::generate();

    let runner_public_key = {
        let inner = shared.state.inner.lock().await;
        inner
            .runner_rsa_public_keys
            .get(&request.runner_id)
            .cloned()
    };
    let (key_bytes, encrypted) = if let Some(public_key) = runner_public_key {
        (public_key.wrap_key(&session_enc.key)?, true)
    } else {
        (session_enc.key.clone(), false)
    };
    let key_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key_bytes);

    // Store the session key for later message decryption
    let snapshot = {
        let mut inner = shared.state.inner.lock().await;
        inner
            .session_keys
            .insert(session_id.to_string(), session_enc);
        inner.sessions.insert(
            session_id.to_string(),
            RunnerSession {
                session_id: SessionId(session_id),
                runner_id: request.runner_id,
            },
        );
        inner.mark_session_seen(&session_id.to_string());
        inner
            .broker_session_runners
            .insert(session_id.to_string(), request.runner_id);
        crate::store::StoreSnapshot::from_inner(&inner)
    };
    shared
        .state
        .store
        .store_inner(&snapshot)
        .await
        .map_err(|error| ApiError::internal(format!("failed to persist session: {error}")))?;

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

pub(crate) async fn create_session_disttask(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    // For the AzDO message path, generate an unencrypted session key directly.
    // RSA-wrapped keys are only needed for real internet-facing GHES; for local
    // use the runner's from_rsaparams may not reconstruct the keypair correctly.
    let session_id = uuid::Uuid::new_v4();
    let session_enc = SessionEncryption::generate();

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

    let runner_public_key = {
        let inner = shared.state.inner.lock().await;
        runner_id.and_then(|id| inner.runner_rsa_public_keys.get(&id).cloned())
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
    let snapshot = {
        let mut inner = shared.state.inner.lock().await;
        inner
            .session_keys
            .insert(session_id.to_string(), session_enc);
        if let Some(runner_id) = runner_id {
            inner
                .broker_session_runners
                .insert(session_id.to_string(), runner_id);
            inner.sessions.insert(
                session_id.to_string(),
                RunnerSession {
                    session_id: SessionId(session_id),
                    runner_id,
                },
            );
            inner.mark_session_seen(&session_id.to_string());
        }
        // Only mark as AzDO if the client explicitly opts in.
        // This preserves backward compat: test and broker-hybrid sessions do NOT
        // include `preloopAzdo: true` and continue to receive broker-ref messages.
        if body
            .get("preloopAzdo")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            inner.azdo_sessions.insert(session_id.to_string());
        }
        crate::store::StoreSnapshot::from_inner(&inner)
    };
    shared
        .state
        .store
        .store_inner(&snapshot)
        .await
        .map_err(|error| ApiError::internal(format!("failed to persist session: {error}")))?;

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

pub(crate) async fn delete_session(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, session_id)): Path<(i64, String)>,
) -> StatusCode {
    let snapshot = {
        let mut inner = shared.state.inner.lock().await;
        inner.sessions.remove(&session_id);
        inner.broker_session_runners.remove(&session_id);
        crate::store::StoreSnapshot::from_inner(&inner)
    };
    if let Err(error) = shared.state.store.store_inner(&snapshot).await {
        tracing::warn!(?error, "failed to persist deleted runner session");
    }
    StatusCode::NO_CONTENT
}

/// DELETE /runner/server/_apis/distributedtask/pools/:pool_id/agents/:agent_id
/// Idempotent agent deregistration — the runner calls this on clean exit.
/// Purges everything the runner's identity was good for: OAuth client id,
/// RSA key, sessions, and any job assignments it still held, so a stolen
/// identity cannot mint tokens or receive work after the machine is gone.
/// Jobs it was assigned but never claimed go back to pool-pending so the
/// pool provisions a replacement machine for them.
/// Returns null response body in JSON to match official.
pub(crate) async fn delete_agent(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, _agent_id)): Path<(i64, i64)>,
) -> (StatusCode, Json<serde_json::Value>) {
    purge_runner_identity(&shared, _agent_id).await;
    (StatusCode::NO_CONTENT, Json(serde_json::Value::Null))
}

/// Remove every trace of a runner identity: keys, client ids, sessions and
/// assignments. Shared by agent deregistration and pool machine teardown.
pub(crate) async fn purge_runner_identity(shared: &Arc<SharedState>, runner_id: i64) {
    let mut inner = shared.state.inner.lock().await;
    if inner.runners.remove(&runner_id).is_none()
        && inner.runner_client_ids.values().all(|id| *id != runner_id)
    {
        // Unknown runner — keep behavior idempotent.
    }
    inner.runner_client_ids.retain(|_, id| *id != runner_id);
    inner.runner_public_keys.remove(&runner_id);
    inner.runner_rsa_public_keys.remove(&runner_id);
    // Sessions claiming this runner: drop them so subsequent polls stop.
    let doomed_sessions: Vec<String> = inner
        .broker_session_runners
        .iter()
        .filter(|(_, id)| **id == runner_id)
        .map(|(session, _)| session.clone())
        .chain(
            inner
                .sessions
                .iter()
                .filter(|(_, session)| session.runner_id == runner_id)
                .map(|(id, _)| id.clone()),
        )
        .collect();
    for session_id in doomed_sessions {
        let active_request = inner.session_active_requests.remove(&session_id);
        inner.sessions.remove(&session_id);
        inner.broker_session_runners.remove(&session_id);
        inner.session_keys.remove(&session_id);
        inner.azdo_sessions.remove(&session_id);
        inner.inflight_messages.remove(&session_id);
        // A job this session claimed but never finished goes back on the
        // queue for another runner right away, instead of sitting for the
        // lease reaper to fail tens of minutes later.
        if let Some(request_id) = active_request {
            let pending = inner
                .job_requests
                .get(&request_id)
                .filter(|request| request.result.is_none())
                .map(|request| (request.run_id, request.job_id.clone()));
            if let Some((run_id, job_id)) = pending {
                {
                    let key = (run_id, job_id.clone());
                    if let Some(job) = inner.claimed_jobs.remove(&key) {
                        if let Some(run) = inner.runs.get_mut(&run_id) {
                            run.jobs.insert(job_id.clone(), ExecutionStatus::Queued);
                            run.status =
                                runtime_scheduling::summarize_run(run.jobs.values().copied());
                        }
                        info!(
                            runner_id,
                            %run_id,
                            job_id = %job_id.0,
                            "requeuing job of purged runner"
                        );
                        runtime_scheduling::on_job_enqueued(&mut inner, &job);
                        inner.queue.push_back(job);
                    }
                }
            }
        }
    }
    // Assignments it never claimed: release the jobs back to pool-pending so
    // a replacement machine can be provisioned for them.
    let orphaned: Vec<(RunId, JobId)> = inner
        .job_assignments
        .iter()
        .filter(|(_, record)| record.runner_id == runner_id)
        .map(|(key, _)| key.clone())
        .collect();
    for key in orphaned {
        if crate::runtime_scheduling::clear_assignment(&mut inner, key.0, &key.1)
            && inner.pool_assignments_enabled
        {
            // Requeue at the *back* of the waitlist with a fresh mark: a job
            // whose machines keep dying must not hold the front of the line
            // forever. `clear_assignment` removed the old mark, so this is a
            // fresh stamp (not the original one). The pairing path re-arms
            // stale marks and `claim_permitted` opens the hold once a mark
            // ages past the binding window, so a fresh mark cannot wedge the
            // job — it simply waits its turn again.
            inner
                .pool_pending
                .entry(key)
                .or_insert_with(std::time::SystemTime::now);
        }
    }
    shared
        .state
        .queue_depth
        .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
    runtime_scheduling::sync_next_job_labels(&inner, &shared.state.next_job_runs_on);
    drop(inner);
    shared.state.message_notify.notify_waiters();
}

/// DELETE /runner/server/_apis/distributedtask/pools/:pool_id/sessions (no session_id)
/// Broker-side session teardown: the runner deletes the session-less path on the broker host.
/// Return 204 unconditionally; the concrete session was already cleaned up individually.
/// Returns null response body in JSON to match official.
pub(crate) async fn delete_sessions_for_pool(
    Path(_pool_id): Path<i64>,
) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NO_CONTENT, Json(serde_json::Value::Null))
}

pub(crate) fn rsa_public_key_xml_from_value(value: &serde_json::Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_owned());
    }
    let modulus = value.get("modulus").and_then(|v| v.as_str())?;
    let exponent = value.get("exponent").and_then(|v| v.as_str())?;
    Some(format!(
        "<RSAKeyValue><Modulus>{modulus}</Modulus><Exponent>{exponent}</Exponent></RSAKeyValue>"
    ))
}

pub(crate) fn task_agent_public_key(request: &serde_json::Value) -> Option<String> {
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
pub(crate) async fn agent_lookup(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let agent_name = params.get("agentName").cloned().unwrap_or_default();
    let inner = shared.state.inner.lock().await;
    for runner in inner.runners.values() {
        if runner.name == agent_name {
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
                    "clientId": "",
                    "publicKey": {"exponent": "AQAB", "modulus": ""}
                }
            }]}));
        }
    }
    // Return empty collection (not 404) — runner expects VssJsonCollectionWrapper format
    Json(json!({"count": 0, "value": []}))
}

/// GET /_apis/v1/Agent/:pool_id/:agent_id — look up runner by agentId in path.
/// The runner constructs URLs from the service definition template `{poolId}/{agentId}`.
/// For lookups it uses agentId=0; for registration it POSTs.
pub(crate) async fn agent_lookup_by_id(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, _agent_id)): Path<(i64, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    agent_lookup(State(shared), Path(_pool_id), Query(params)).await
}

pub(crate) async fn runner_pools() -> Json<serde_json::Value> {
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
pub(crate) async fn register_runner_compat(
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
    let result = register_runner_inner(&shared, reg_request).await?;
    let client_id = uuid::Uuid::new_v4().to_string();
    let provision_token = headers
        .get("x-preloop-provision-token")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    {
        let mut inner = shared.state.inner.lock().await;
        // The OAuth client id must be in the store before it is persisted:
        // the runner's next token request is rejected as an unknown client
        // if a restart happens between registration and persist.
        inner.runner_client_ids.insert(client_id.clone(), result.id);
        // Pair the fresh runner with the job its machine was provisioned
        // for. Pairing is gated on the one-time provision token the pool
        // generated host-side for exactly this machine — a rogue process on
        // another machine cannot mint it, so it cannot steal pairings.
        if let Some(token) = provision_token {
            let accepted = shared
                .state
                .pending_registrations
                .write()
                .map(|mut pending| pending.remove(&token).is_some())
                .unwrap_or(false);
            if accepted {
                crate::runtime_scheduling::pair_registered_runner(&mut inner, result.id);
            }
        }
    }
    // One persist after every identity-bearing mutation, so client_id and any
    // pairing land in the same transaction as the runner row.
    persist_full_state(&shared).await?;
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
        "properties": {
            "RequireFipsCryptography": {"$type": "System.Boolean", "$value": false},
            "ServerUrl": {"$type": "System.String", "$value": runner_server_url()},
            "ServerUrlV2": {"$type": "System.String", "$value": runner_server_url()},
            "UseV2Flow": {"$type": "System.Boolean", "$value": true}
        }
    })))
}

/// Compat handler: register runner via `/_apis/v1/Agent/:pool_id` (no agent_id in path).
pub(crate) async fn register_runner_compat_pool_only(
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
pub(crate) async fn create_session_compat(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, _session_id)): Path<(i64, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runner_id = body
        .get("agent")
        .and_then(|a| a.get("id"))
        .and_then(|v| v.as_i64())
        .unwrap_or(1);
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
pub(crate) async fn next_message_compat(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> (StatusCode, Json<Option<azdo::TaskAgentMessage>>) {
    next_message(State(shared), identity, Query(params)).await
}
/// POST /api/v1/runners/purge — orchestrator-facing runner deregistration:
/// purges the identity AND requeues any claimed-but-unfinished job, so a
/// machine torn down mid-job stops stalling the job until the lease reaper.
pub(crate) async fn purge_runners_by_name(
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
    let id_or_ids: Vec<i64> = {
        let inner = shared.state.inner.lock().await;
        inner
            .runners
            .iter()
            .filter(|(_, runner)| runner.name == name)
            .map(|(id, _)| *id)
            .collect()
    };
    for id in &id_or_ids {
        purge_runner_identity(&shared, *id).await;
    }
    (
        StatusCode::OK,
        Json(json!({ "purged": id_or_ids.len(), "ids": id_or_ids })),
    )
}
