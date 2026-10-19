use super::*;

pub async fn require_protocol_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let bearer = bearer_token(&request).map(str::to_owned);
    let authorized = bearer.as_deref().is_some_and(|token| {
        token == shared.state.system_token || shared.state.verify_local_jwt_claims(token).is_some()
    });
    if !authorized {
        return Err(ApiError::unauthorized(
            "runner or job protocol token required",
        ));
    }
    // A job runtime token is a bearer that workflow code can read (the runner
    // exports it as `ACTIONS_RUNTIME_TOKEN`), so it must stop authenticating
    // once its attempt is no longer live. Final-reporting routes are the one
    // exception: they settle or idempotently re-report the attempt that the
    // token already owns, and denying them would strand a timed-out job.
    if let Some(job) = bearer
        .as_deref()
        .and_then(|token| shared.state.job_uuid_from_token(token))
        && !is_final_reporting_route(request.method(), request.uri().path())
    {
        require_live_job(&shared.state, job).await?;
    }
    // Debug-worker tokens name a job too, and the same revocation rule
    // applies: a completed attempt must not keep driving its debug session.
    if let Some(job) = bearer
        .as_deref()
        .and_then(|token| shared.state.job_uuid_from_debug_token(token))
    {
        require_live_job(&shared.state, job).await?;
    }
    Ok(next.run(request).await)
}

/// Whether a route is the attempt's final-reporting path.
///
/// These routes are authenticated by the attempt's own token but must keep
/// working after the attempt is settled or timed out: the runner may report
/// completion late, and a duplicate report must be idempotent. Every other
/// job-token route is denied once the attempt is no longer live.
fn is_final_reporting_route(method: &axum::http::Method, path: &str) -> bool {
    if *method == axum::http::Method::POST {
        return (path.starts_with("/broker/") && path.ends_with("/completejob"))
            || path.contains("/_apis/v1/FinishJob/")
            || ((path.starts_with("/_apis/v1/plans/")
                || path.starts_with("/runner/server/_apis/v1/plans/"))
                && path.ends_with("/events"));
    }
    // Legacy runner `PATCH .../hubs/actions/plans/{run}/jobs/{job}` completion.
    *method == axum::http::Method::PATCH
        && path.starts_with("/runner/server/_apis/distributedtask/hubs/actions/plans/")
        && path.contains("/jobs/")
}
/// Require the system credential or a Results job runtime token.
///
/// This is narrower than [`require_protocol_bearer`]: action-resolution
/// endpoints must not admit runner-listen, registration, debug-worker, blob,
/// or other locally signed JWT classes as bearer credentials.
pub async fn require_results_protocol_bearer(
    State(shared): State<Arc<SharedState>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let bearer = bearer_token(&request)
        .ok_or_else(|| ApiError::unauthorized("system or job runtime token required"))?;
    let identity = results_identity(&shared.state, bearer)
        .map_err(|_| ApiError::unauthorized("system or job runtime token required"))?;
    if let ResultsIdentity::Job(job) = &identity {
        require_live_job(&shared.state, job.job_id).await?;
    }
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}
/// Require that a protocol reporting request is authenticated by the system
/// credential or by the runtime token minted for the exact job request.
///
/// The generic protocol layer intentionally accepts several local JWT classes
/// because it fronts both runner and results-service routes. Reporting is
/// narrower: workflow code can read its own runtime token, and a listen or
/// manage token must not be able to finish another job.
pub fn authorize_reporting_request(
    state: &AppState,
    headers: &HeaderMap,
    request: Option<&TaskAgentJobRequestRecord>,
) -> Result<(), ApiError> {
    let token = bearer_from_headers(headers)
        .ok_or_else(|| ApiError::unauthorized("runner or job protocol token required"))?;
    if token == state.system_token {
        return Ok(());
    }

    // Authenticate before authorizing: a token that fails verification is a
    // 401 regardless of whether the target resolves; only a verified token
    // that names a different job earns the 403.
    let claims = state
        .verify_local_jwt_claims(token)
        .ok_or_else(|| ApiError::unauthorized("job runtime token required"))?;
    let Some(request) = request else {
        return Err(ApiError::forbidden(
            "job runtime token cannot resolve the reporting target",
        ));
    };
    let authorized = {
        let subject_job = claims
            .get("sub")
            .and_then(|value| value.as_str())
            .and_then(|subject| subject.strip_prefix("preloop-job-"))
            .unwrap_or("");
        let scope = claims
            .get("scp")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        subject_job == request.agent_job_id.to_string()
            && scope
                == format!(
                    "Actions.Results:{}:{}",
                    request.plan_id, request.agent_job_id
                )
    };
    if authorized {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "job runtime token does not own the reporting target",
        ))
    }
}

/// [`authorize_reporting_request`] plus the liveness rule: the caller must own
/// the target *and* the attempt must still be live.
///
/// A settled attempt's runtime credential is spent — the rule the Results
/// operations ([`require_live_results_job`]), the cache and artifact writes,
/// the forge relay, blob URLs, and the live-log paths all apply — so it must
/// not keep reading or mutating the run's records (timeline records, logs,
/// console logs) after the attempt has settled. The credential is handed to
/// workflow code and outlives the attempt, so without this a finished job
/// could rewrite its own step records and annotations, or keep reading the
/// run's retained output, for the rest of the token's lifetime. The system
/// identity bypasses exactly as it does everywhere else, and final-reporting
/// completion routes deliberately do not use this helper.
pub async fn authorize_live_reporting_request(
    state: &AppState,
    headers: &HeaderMap,
    request: Option<&TaskAgentJobRequestRecord>,
) -> Result<(), ApiError> {
    authorize_reporting_request(state, headers, request)?;
    if system_bearer_authorized(state, headers) {
        return Ok(());
    }
    let Some(record) = request else {
        // Unresolvable targets were refused above for a non-system caller.
        return Ok(());
    };
    require_live_job(state, record.agent_job_id).await
}

pub async fn require_results_bearer(
    State(shared): State<Arc<SharedState>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let path = request.uri().path();
    if path.starts_with("/replay/results/") {
        // Blob uploads are bearerless by design — the official runner PUTs
        // step logs and summaries to the SAS-style URLs its Twirp handlers
        // minted, and so does the in-VM runner. The URL *is* the credential,
        // so the ticket must hold: a guest inside a runner VM can reach this
        // route through the mounted control socket, and an unsigned (or
        // forged) upload would let workflow code overwrite another job's
        // stored logs. Only PUT is a write; other methods fall through to
        // normal routing (404 for unregistered methods).
        if request.method() == axum::http::Method::PUT {
            if !verify_replay_upload_ticket(&shared.state, request.uri()) {
                return Err(ApiError::unauthorized(
                    "replay blob upload requires a signed URL ticket",
                ));
            }
            // The ticket is a bearerless upload capability reachable from
            // inside the job VM. It is bound to the job segment of the signed
            // path, and stops working when that attempt stops being live
            // instead of surviving until the ticket's one-hour expiry.
            let job = replay_ticket_job_id(request.uri().path())
                .ok_or_else(|| ApiError::unauthorized("replay blob ticket does not name a job"))?;
            require_live_job(&shared.state, job).await?;
        }
        return Ok(next.run(request).await);
    }
    if path.starts_with("/twirp-blob/") {
        // This route used to fall through the `!/twirp/` check below
        // with zero authentication, and blob_put/blob_get joined the raw
        // segments into the filesystem. The gate now owns both problems.
        let path = path.to_owned();
        return authorize_blob_request(&shared.state, path, request, next).await;
    }
    if !path.starts_with("/twirp/") {
        return Ok(next.run(request).await);
    }
    let Some(identity) =
        bearer_token(&request).and_then(|token| results_identity(&shared.state, token).ok())
    else {
        return Err(ApiError::unauthorized("results-service job token required"));
    };
    // The typed identity is authenticated before body extraction. Handlers
    // then compare decoded plan/job ids against this same identity instead of
    // re-parsing a bearer string independently. The middleware also enforces
    // liveness uniformly: every Twirp route is job-scoped, and a settled,
    // timed-out, cancelled, or lease-expired attempt's runtime token must not
    // keep minting upload/download URLs or reading results metadata.
    if let ResultsIdentity::Job(job) = &identity {
        require_live_job(&shared.state, job.job_id).await?;
    }
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

/// Authorize `/twirp-blob/{kind}/{token}` requests.
///
/// The blob endpoints are bearerless by protocol design: the Azure SDK in
/// `actions/upload-artifact` / `actions/cache` PUTs to the signed upload URL
/// without attaching the job bearer, so the gate cannot simply require a
/// bearer without breaking wire compatibility with the official toolkit.
/// Instead it enforces four properties:
///   1. the kind is allowlisted and the token is decode-then-validated, so no
///      request can address outside `<state_dir>/blobs/{kind}/`;
///   2. the token must be a server-signed blob JWT (`sub: preloop-blob`,
///      `kind` matching the path) - arbitrary tokens address nothing, which
///      kills the unauthenticated PUT-to-anything repro, and minted URLs
///      survive engine restarts since validity is the signature, not a
///      registration map;
///   3. the token's `op` claim pins the exact operation and `job` names the
///      minting owner (never empty): a download URL (`read`) rejects PUT
///      with 403 and an upload URL (`write`) rejects GET with 403 - a
///      bearerless PUT to a signed download URL can no longer overwrite the
///      finalized blob (round-3 finding 3);
///   4. the owning job must still be live for BOTH operations: the JWT's
///      `job` claim names the owner recorded at mint time, so a completed,
///      cancelled, timed-out, or lease-expired job's upload and download URLs
///      stop working immediately instead of at TTL sweep. A presented job
///      bearer is verified and must match the `job` claim — a live job cannot
///      spend a URL another job minted. The `system` owner
///      (control-plane-minted URLs) skips the liveness check, as it did when
///      empty owners meant "system".
///   5. diag uploads additionally require the token's `jti` to still be
///      registered in `diag_upload_tokens`: the per-job cap evicts the
///      oldest token, and eviction must invalidate — a signature alone
///      cannot see eviction, so without this a live job could re-mint
///      forever and accumulate unbounded staging directories. (Consequence:
///      diag URLs no longer survive an engine restart; the map is
///      in-memory only. Diag uploads happen immediately after minting, so a
///      restart in between was already a lost upload.)
///
/// Reads stay bearer-optional - the unguessable, server-minted URL is the
/// credential, the same SAS-style model as `/replay/results/*` - because the
/// download clients are equally bearerless.
async fn authorize_blob_request(
    state: &AppState,
    path: String,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let Some((kind, token)) = crate::blob_store::parse_blob_path(&path) else {
        return Err(ApiError::bad_request("invalid blob path"));
    };
    let is_write = request.method() == axum::http::Method::PUT;

    // The signed token is the credential: `op` pins the exact operation and
    // `job` names the minting owner (never empty).
    let Some(claims) = crate::blob_store::verify_blob_token(state, &kind, &token) else {
        return Err(ApiError::not_found("blob not found"));
    };
    let op = crate::blob_store::blob_token_op(&claims);
    let owner = crate::blob_store::blob_token_job(&claims).unwrap_or_default();

    // Operation binding (round-3 finding 3): a download URL must never
    // accept a PUT, an upload URL must never serve a GET. Tokens minted
    // before op binding (no `op` claim) or with an empty owner are rejected
    // the same way.
    let op_ok = if is_write {
        op == Some(crate::blob_store::BLOB_OP_WRITE)
    } else {
        op == Some(crate::blob_store::BLOB_OP_READ)
    };
    if !op_ok || owner.is_empty() {
        return Err(ApiError::forbidden(
            "blob token is not valid for this operation",
        ));
    }

    // Diag upload tokens are bearerless single-job reservations: the mint
    // handler records the token's `jti` in `diag_upload_tokens` and evicts
    // the oldest per job at the cap. A signature alone cannot see eviction,
    // so an evicted (or never-registered) diag token's PUT must not upload —
    // otherwise a live job could re-mint forever and accumulate unbounded
    // staging directories. Membership in the live map is the credential for
    // diag PUTs, on top of the signature and owner-job liveness checks.
    if is_write && kind == "diag" {
        let jti = crate::blob_store::blob_token_jti(&claims)
            .ok_or_else(|| ApiError::bad_request("malformed blob token"))?;
        let registered = state
            .inner
            .lock()
            .await
            .diag_upload_tokens
            .contains_key(&jti);
        if !registered {
            warn!(jti, "rejected diag PUT for evicted or unknown upload token");
            return Err(ApiError::not_found("blob not found"));
        }
    }

    // The control-plane credential is not a guest credential and may inspect
    // any stored blob (it can also mint fresh URLs). No job-liveness rule is
    // applied to it.
    if bearer_from_headers(request.headers()) == Some(state.system_token.as_str()) {
        return Ok(next.run(request).await);
    }
    match bearer_from_headers(request.headers()) {
        Some(bearer) => {
            let job_id = match results_identity(state, bearer) {
                Ok(ResultsIdentity::Job(identity)) => identity.job_id.to_string(),
                _ => return Err(ApiError::unauthorized("results-service job token required")),
            };
            // Ownership binds both operations, not just writes: a live job
            // must not use its own bearer to read a URL minted for another
            // job. (A bearerless request is indistinguishable from the URL's
            // legitimate holder, so only the owner-liveness rule applies
            // there.)
            if owner != job_id {
                return Err(ApiError::forbidden("blob token is not owned by this job"));
            }
            let job_uuid = owner
                .parse::<uuid::Uuid>()
                .map_err(|_| ApiError::forbidden("blob token owner is not a job"))?;
            require_live_job(state, job_uuid).await?;
            Ok(next.run(request).await)
        }
        // Bearerless Azure-SDK-style flow: the unguessable, server-minted URL
        // is the credential. Both reads and writes stop working once the
        // owning attempt is no longer live; the URL is a derived credential
        // and must not outlive the job that minted it. The `system` owner
        // (control-plane-minted URLs) is not a job and skips this check.
        None => {
            if owner != crate::blob_store::BLOB_SYSTEM_OWNER {
                let job_uuid = owner
                    .parse::<uuid::Uuid>()
                    .map_err(|_| ApiError::forbidden("blob token owner is not a job"))?;
                require_live_job(state, job_uuid).await?;
            }
            Ok(next.run(request).await)
        }
    }
}

/// Require the calling job to be live before a Results operation.
///
/// Resolves the job's request record and rejects when the job is unknown
/// (purged), has settled, was cancelled, exceeded its deadline, or lost its
/// runner lease — the stale credential replay window. Callers use it for
/// reads as well as writes: a runtime token is handed to workflow code and
/// must not keep minting URLs or reading results after the attempt ends. The
/// system identity (engine) bypasses: it manages the lifecycle itself.
pub async fn require_live_results_job(
    state: &AppState,
    identity: &ResultsIdentity,
) -> Result<(), ApiError> {
    let job_uuid = match identity {
        ResultsIdentity::Job(job) => job.job_id,
        ResultsIdentity::System => return Ok(()),
    };
    require_live_job(state, job_uuid).await
}

/// Liveness predicate on a job, resolved from the control backend.
///
/// A job is live only while its attempt can still do work:
///   * the request record exists (a purged or unknown job is not live);
///   * it has not settled to a terminal result;
///   * the reaper has not flagged its deadline (`timeout_triggered`);
///   * its runner has renewed within [`HUNG_WORKER_LEASE_SECONDS`] (or, when
///     no renewal has ever landed, the lock the runner was given has not
///     expired); and
///   * its logical job has not been settled terminal — cancellation projects
///     `jobs` to Cancelled immediately while the request row stays unsettled
///     until the runner reports, and the old rule admitted that whole window.
///
/// The lease check is direct and matches the reaper's
/// [`HUNG_WORKER_LEASE_SECONDS`] window. The timeout check is the persisted
/// `timeout_triggered` flag, which the reaper sets on its sweep: it is the
/// engine's observed deadline, not a fresh wall-clock computation at request
/// time. (The request record does not carry `job_timeout_s`; the reaper's
/// tick is the authority, and a stricter dispatch predicate that reads it is
/// compatible.) Final completion routes bypass this predicate (see
/// [`is_final_reporting_route`]) so late and duplicate reports remain
/// possible.
pub async fn job_is_live(state: &AppState, job_uuid: uuid::Uuid) -> Result<bool, ApiError> {
    let record = match state
        .backend
        .request(crate::control::backend::RequestKey::AgentJobId(job_uuid))
        .await
    {
        Ok(record) => record,
        Err(crate::control::ControlError::NotFound(_)) => return Ok(false),
        Err(error) => return Err(ApiError::from(error)),
    };
    if matches!(record.result, Some(status) if status.is_terminal()) {
        return Ok(false);
    }
    // The reaper has decided the attempt exceeded its `timeout-minutes`.
    // Cancellation is queued but the request stays open for the final report;
    // every non-reporting credential is spent at this point.
    if record.timeout_triggered {
        return Ok(false);
    }
    // Match the reaper's lease rule so delegation of a live session's token
    // does not outlive its runner by more than one reaper window. A request
    // with no renewal yet is admitted: it predates the first renewjob, and
    // `locked_until` (when present) still bounds it.
    if let Some(renewed_at) = record.last_renewed_at {
        let elapsed = std::time::SystemTime::now()
            .duration_since(renewed_at)
            .unwrap_or_default();
        if elapsed
            >= std::time::Duration::from_secs(crate::distributed_task::HUNG_WORKER_LEASE_SECONDS)
        {
            return Ok(false);
        }
    } else if !record.locked_until.is_empty() {
        if let Ok(deadline) = chrono::DateTime::parse_from_rfc3339(&record.locked_until) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            if deadline.timestamp() <= now {
                return Ok(false);
            }
        }
    }
    // Cancellation settles the logical job immediately, before the runner has
    // reported and before the reaper's lease timeout. A stolen runtime token
    // must not keep reading or writing through that window.
    match state
        .backend
        .job_queue_state(record.run_id, &record.job_id)
        .await
    {
        Ok(Some((_, status)))
            if matches!(
                status.as_str(),
                "success" | "failure" | "skipped" | "cancelled" | "timed_out"
            ) =>
        {
            Ok(false)
        }
        Ok(_) => Ok(true),
        Err(error) => Err(ApiError::from(error)),
    }
}

/// Require a job UUID to be live before a write, for handlers that
/// authenticate from headers rather than a typed [`ResultsIdentity`]
/// (the legacy `/_apis/artifactcache` cache write path). Same rule as
/// [`require_live_results_job`]: the system identity bypasses, so callers
/// must skip this helper for the system bearer themselves.
pub async fn require_live_job(state: &AppState, job_uuid: uuid::Uuid) -> Result<(), ApiError> {
    if job_is_live(state, job_uuid).await? {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "job is not live; job-scoped credentials are rejected for completed or unknown jobs",
        ))
    }
}

pub async fn require_test_api_token(
    State(expected): State<Arc<str>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token == expected.as_ref());
    if !authorized {
        warn!(path = %request.uri().path(), "rejected privileged test API request");
        return Err(ApiError::unauthorized("missing or invalid test API token"));
    }
    warn!(path = %request.uri().path(), "privileged test API request");
    Ok(next.run(request).await)
}

pub fn system_bearer_authorized(state: &AppState, headers: &HeaderMap) -> bool {
    bearer_from_headers(headers).is_some_and(|token| token == state.system_token)
}

fn request_system_bearer_authorized(shared: &Arc<SharedState>, request: &Request) -> bool {
    bearer_token(request).is_some_and(|token| token == shared.state.system_token)
}

pub async fn require_system_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if request_system_bearer_authorized(&shared, &request) {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized("missing or invalid system token"))
    }
}

/// Who a session/agent administration request acts as.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminCaller {
    System,
    /// A runner-registration credential used by configure/replace.
    RunnerManager,
    /// A registered runner's own listen token may act only on itself.
    Runner(i64),
}

/// Guard for runner/session/agent administration.
///
/// these routes used to sit behind [`require_protocol_bearer`], which
/// accepts *any* valid local JWT — including a job's `ACTIONS_RUNTIME_TOKEN`,
/// which the runner exports to every step. Arbitrary workflow code could
/// therefore deregister runners and delete other runners' sessions.
///
/// System-token-only would close that, but it would also break the runner:
/// the AzDO listener deletes its own session on shutdown
/// (`crates/preloop-runner/src/client/azdo.rs::delete_session`) and
/// [`crate::runner_lifecycle::delete_agent`] is documented as the call the
/// runner makes on clean exit — and a configured runner holds nothing but its
/// listen token. So admit the system token *or* a registered runner's listen
/// token here, reject job runtime tokens, and let the handlers enforce
/// self-ownership via [`admin_caller`].
pub async fn require_runner_admin_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if request_system_bearer_authorized(&shared, &request) {
        return Ok(next.run(request).await);
    }
    let manager_token = bearer_token(&request)
        .and_then(|token| shared.state.verify_local_jwt_claims(token))
        .is_some_and(|claims| {
            runner_manager_claims(&claims)
                // A grant minted from a one-time VM provision token is scoped
                // to registering that VM, never to deleting other runners.
                && claims.get("pvt_hash").is_none()
        });
    if manager_token {
        return Ok(next.run(request).await);
    }
    // `resolve_runner_identity` is an outer layer, so the extension is
    // already present here. Reuse it instead of re-deriving the runner from
    // the token: it also resolves mock-flow subjects
    // (`preloop-runner-listen-mock-{client_id}`) and drops the identity when
    // the registration behind the token is gone.
    let verified = request
        .extensions()
        .get::<RunnerIdentity>()
        .and_then(|identity| identity.runner_id)
        .is_some();
    if verified {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized(
            "runner administration requires the system token or a runner listen token",
        ))
    }
}

/// Resolve the [`AdminCaller`] behind a request that passed
/// [`require_runner_admin_bearer`].
///
/// Fail-closed: anything that is neither the system token nor a verified
/// runner identity is an error, never silently treated as the administrator.
pub fn admin_caller(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    identity: Option<&RunnerIdentity>,
) -> Result<AdminCaller, ApiError> {
    let Some(token) = bearer_from_headers(headers) else {
        return Err(ApiError::unauthorized(
            "runner administration requires a bearer token",
        ));
    };
    if token == state.system_token {
        return Ok(AdminCaller::System);
    }
    if state
        .verify_local_jwt_claims(token)
        .is_some_and(|claims| runner_manager_claims(&claims) && claims.get("pvt_hash").is_none())
    {
        return Ok(AdminCaller::RunnerManager);
    }
    identity
        .and_then(|identity| identity.runner_id)
        .map(AdminCaller::Runner)
        .ok_or_else(|| {
            ApiError::unauthorized(
                "runner administration requires the system token or a runner listen token",
            )
        })
}

pub async fn require_native_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if request_system_bearer_authorized(&shared, &request) {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized(
            "missing or invalid native API token",
        ))
    }
}

pub async fn require_runner_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if let Some(token) = bearer_token(&request)
        && registered_runner_id(&shared, token).await.is_some()
    {
        return Ok(next.run(request).await);
    }
    Err(ApiError::unauthorized("runner listen token required"))
}
fn runner_manager_claims(claims: &serde_json::Value) -> bool {
    claims
        .get("scp")
        .and_then(|value| value.as_str())
        .is_some_and(|scope| {
            scope
                .split_whitespace()
                .any(|value| value == "ActionsRuntime.RunnerManage")
        })
}

fn runner_registration_claims(claims: &serde_json::Value) -> bool {
    claims.get("sub").and_then(|value| value.as_str()) == Some("preloop-runner-registration")
        && runner_manager_claims(claims)
}

/// A registration grant is either the operator's system/RunnerManage
/// credential or a grant bound to a pending one-time provision token. A bound
/// grant stops authorizing the moment that provision token is consumed, so a
/// copy of the exchanged bearer cannot keep registering runners.
pub fn runner_registration_bearer_authorized(state: &AppState, token: &str) -> bool {
    if token == state.system_token.as_str() {
        return true;
    }
    let Some(claims) = state.verify_local_jwt_claims(token) else {
        return false;
    };
    if !runner_registration_claims(&claims) {
        return false;
    }
    match claims.get("pvt_hash").and_then(|value| value.as_str()) {
        Some(hash) => pending_provision_token_by_hash(state, hash).is_some(),
        None => true,
    }
}

/// Require the credential that is allowed to create a runner identity through
/// the legacy AzDO registration aliases. The GitHub-compatible registration
/// endpoint mints this local RunnerManage token; a pool provision token is the
/// other supported path for the host-side configure flow.
pub async fn require_runner_registration_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let bearer_authorized = bearer_token(&request)
        .is_some_and(|token| runner_registration_bearer_authorized(&shared.state, token));
    let provision_authorized = request
        .headers()
        .get("x-preloop-provision-token")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|token| pending_provision_token(&shared.state, token));
    if bearer_authorized || provision_authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized(
            "runner registration credential required",
        ))
    }
}

/// Require a credential that identifies a registered runner on the legacy
/// session/message surface. The system token remains valid for trusted
/// control-plane tests and operators; runner traffic must prove a listen JWT.
pub async fn require_legacy_runner_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(token) = bearer_token(&request) else {
        return Err(ApiError::unauthorized("runner listen token required"));
    };
    if token == shared.state.system_token || registered_runner_id(&shared, token).await.is_some() {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized("runner listen token required"))
    }
}

const PROVISION_TOKEN_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(600);

pub(crate) fn pending_provision_token(state: &AppState, token: &str) -> bool {
    state
        .pending_registrations
        .read()
        .ok()
        .and_then(|pending| pending.get(token).copied())
        .is_some_and(provision_token_is_fresh)
}

fn provision_token_is_fresh(issued_at: std::time::SystemTime) -> bool {
    std::time::SystemTime::now()
        .duration_since(issued_at)
        .is_ok_and(|age| age <= PROVISION_TOKEN_MAX_AGE)
}

/// Hash a provision token for embedding in a registration grant. The raw
/// token never enters the JWT, so a copied grant reveals no reusable
/// registration credential.
pub fn provision_token_hash(token: &str) -> String {
    preloop_gha_protocol::crypto::sha256_hex(token)
}

fn pending_provision_token_by_hash(state: &AppState, hash: &str) -> Option<String> {
    state.pending_registrations.read().ok().and_then(|pending| {
        pending
            .iter()
            .find(|(token, issued_at)| {
                provision_token_is_fresh(**issued_at)
                    && provision_token_hash(token.as_str()) == hash
            })
            .map(|(token, _)| token.clone())
    })
}

/// The raw pending provision token a bound registration grant names.
///
/// Used by the agent-registration handler to consume the same one-time
/// credential it was minted from, so the grant and the header path cannot
/// authorize separate registrations.
pub fn bound_provision_token(state: &AppState, token: &str) -> Option<String> {
    let claims = state.verify_local_jwt_claims(token)?;
    if !runner_registration_claims(&claims) {
        return None;
    }
    let hash = claims.get("pvt_hash")?.as_str()?;
    pending_provision_token_by_hash(state, hash)
}

/// Atomically consume a pool-issued registration credential. The middleware
/// performs the fast authorization check; the handler consumes the token
/// under the write lock so concurrent registration attempts cannot both pass.
/// The issuance time is returned so a validation failure can restore the
/// credential without extending its lifetime.
pub fn consume_pending_provision_token(
    state: &AppState,
    token: &str,
) -> Option<std::time::SystemTime> {
    state
        .pending_registrations
        .write()
        .ok()
        .and_then(|mut pending| pending.remove(token))
        .filter(|issued_at| provision_token_is_fresh(*issued_at))
}

pub fn restore_pending_provision_token(
    state: &AppState,
    token: &str,
    issued_at: std::time::SystemTime,
) {
    if !provision_token_is_fresh(issued_at) {
        return;
    }
    if let Ok(mut pending) = state.pending_registrations.write() {
        pending.insert(token.to_owned(), issued_at);
    }
}

/// Resolve a listen JWT to a currently registered runner.
///
/// Both production PS256 tokens and local JSON-OAuth compatibility tokens use
/// this single mapping path. The request middleware and broker handlers call
/// the same helper so token identity cannot drift between surfaces.
pub async fn registered_runner_id(shared: &Arc<SharedState>, token: &str) -> Option<i64> {
    let runner_id = if let Some(runner_id) = shared.state.runner_id_from_token(token) {
        Some(runner_id)
    } else {
        let client_id = shared
            .state
            .verify_local_jwt_claims(token)
            .and_then(|claims| {
                claims
                    .get("sub")
                    .and_then(|value| value.as_str())
                    .and_then(|sub| sub.strip_prefix("preloop-runner-listen-mock-"))
                    .map(str::to_owned)
            });
        let client_id = client_id?;
        shared
            .state
            .backend
            .runner_for_client(&client_id)
            .await
            .ok()
            .flatten()
    };
    let runner_id = runner_id?;
    runner_registered(shared, runner_id)
        .await
        .then_some(runner_id)
}

pub fn bearer_token(request: &Request) -> Option<&str> {
    bearer_from_headers(request.headers())
}

pub fn bearer_from_headers(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

/// Whether this runner's credentials are still valid. Agent deregistration and
/// machine teardown remove the registration, but the authoritative rule is
/// [`crate::control::ControlBackend::runner_credentials_usable`]: a single-use (`--ephemeral`)
/// runner whose attempt has settled is spent even if its row still exists, so
/// its listen token stops authenticating before (or without) cleanup. A JWT
/// that outlives its runner's usable window must not create sessions or pull
/// work.
async fn runner_registered(shared: &Arc<SharedState>, runner_id: i64) -> bool {
    shared
        .state
        .backend
        .runner_credentials_usable(runner_id)
        .await
        .unwrap_or(false)
}

/// Signed replay blob upload tickets expire after one hour.
pub const REPLAY_TICKET_TTL_SECS: u64 = 3600;

pub fn replay_ticket_expiry() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_add(REPLAY_TICKET_TTL_SECS)
}

/// Sign a replay blob upload URL for `path` (e.g. `/replay/results/{plan}/{job}/step-1.txt`).
///
/// The upload route is deliberately bearerless — the runner PUTs bytes to the
/// URL its own Twirp handler returned — and it is reachable from inside every
/// runner VM, where workflow code runs. Unsigned, the route let a guest
/// overwrite another job's stored logs and summaries by guessing the URL
/// shape. Binding the signature to the exact path and expiry makes a URL
/// minted for one job worthless against any other, or after its lifetime.
pub fn sign_replay_upload_ticket(state: &AppState, path: &str, expires_at: u64) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(&state.local_jwt_key)
        .expect("HMAC accepts keys of any length");
    mac.update(replay_ticket_payload(path, expires_at).as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

/// Whether the `sig` query parameter of `uri` authorises an upload to exactly
/// that replay path before the signed Unix expiry (`se`).
pub fn verify_replay_upload_ticket(state: &AppState, uri: &axum::http::Uri) -> bool {
    let Some(query) = uri.query() else {
        return false;
    };
    let params: std::collections::HashMap<String, String> =
        serde_urlencoded::from_str(query).unwrap_or_default();
    let Some(signature) = params.get("sig").map(String::as_str) else {
        return false;
    };
    let Some(expires_at) = params.get("se").and_then(|value| value.parse::<u64>().ok()) else {
        return false;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if now >= expires_at {
        return false;
    }
    let Ok(provided) = URL_SAFE_NO_PAD.decode(signature) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(&state.local_jwt_key) else {
        return false;
    };
    mac.update(replay_ticket_payload(uri.path(), expires_at).as_bytes());
    mac.verify_slice(&provided).is_ok()
}

fn replay_ticket_payload(path: &str, expires_at: u64) -> String {
    format!("replay-blob\n{expires_at}\n{path}")
}

/// The agent job a signed replay upload path belongs to.
///
/// Every mint uses `/replay/results/{plan}/{job}/{file}` (see the
/// `Get*SignedBlobURL` handlers), and the signature covers the exact path, so
/// the segment before the file name is the owner the server signed. Parsing
/// from the right keeps this correct even when an opaque plan segment itself
/// contains a slash. A path that does not carry a UUID owner is rejected by
/// the caller — a valid signature alone must not authorize a ticket that names
/// no live job.
fn replay_ticket_job_id(path: &str) -> Option<uuid::Uuid> {
    let rest = path.strip_prefix("/replay/results/")?;
    let mut segments = rest.rsplit('/');
    let _file = segments.next()?;
    segments.next()?.parse::<uuid::Uuid>().ok()
}

/// Marker extension: the request arrived through the mounted control socket.
///
/// Handlers use it to distinguish the untrusted VM surface from the operator's
/// TCP surface. Workflow code inside a runner VM can reach the socket, so a
/// handler that mints anything (e.g. runner-management JWTs) may require a
/// stronger credential there than on TCP, where GitHub-compatible clients
/// (official runner, conformance replays) present credentials only they hold.
pub struct SocketSurface;

impl Clone for SocketSurface {
    fn clone(&self) -> Self {
        SocketSurface
    }
}

/// The authenticated identity carried by a Results bearer is used by every
/// Results handler, including signed-URL minting and metadata mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultsJobIdentity {
    pub plan_id: String,
    pub job_id: uuid::Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultsIdentity {
    /// The engine credential is allowed to address every Results target.
    System,
    /// A runtime credential is bound to one plan/job pair.
    Job(ResultsJobIdentity),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultsIdentityError {
    /// The bearer is signed locally but does not identify a Results job.
    NotJob,
    /// A job-shaped bearer has a valid job subject but malformed Results claims.
    MalformedJob,
    /// A job-shaped bearer has an invalid job subject.
    MalformedJobSubject,
    /// A valid job subject is paired with a non-Results scope.
    MalformedScope,
    /// The bearer is not a valid local JWT.
    Invalid,
}

/// Parse the Results bearer into the identity handlers must authorize against.
///
/// Requiring both the `sub` and the full Results scope to agree prevents a
/// valid local JWT minted for one protocol surface from being repurposed as a
/// different job's Results credential. The error distinguishes a malformed
/// job-shaped token so cache writes can retain their fail-closed behavior.
pub fn results_identity(
    state: &AppState,
    bearer: &str,
) -> Result<ResultsIdentity, ResultsIdentityError> {
    if bearer == state.system_token {
        return Ok(ResultsIdentity::System);
    }

    let claims = state
        .verify_local_jwt_claims(bearer)
        .ok_or(ResultsIdentityError::Invalid)?;
    let job_shaped = claims
        .get("sub")
        .and_then(|value| value.as_str())
        .is_some_and(|subject| subject.starts_with("preloop-job-"))
        && claims
            .get("scp")
            .and_then(|value| value.as_str())
            .is_some_and(|scope| scope.starts_with("Actions.Results:"));
    let parsed = AppState::results_job_from_payload(&claims);
    let Some((plan_id, job_id)) = parsed else {
        let subject_is_job = claims
            .get("sub")
            .and_then(|value| value.as_str())
            .and_then(|subject| subject.strip_prefix("preloop-job-"))
            .and_then(|job| job.parse::<uuid::Uuid>().ok())
            .is_some();
        return Err(match (job_shaped, subject_is_job) {
            (true, true) => ResultsIdentityError::MalformedJob,
            (true, false) => ResultsIdentityError::MalformedJobSubject,
            (false, true) => ResultsIdentityError::MalformedScope,
            (false, false) => ResultsIdentityError::NotJob,
        });
    };
    Ok(ResultsIdentity::Job(ResultsJobIdentity { plan_id, job_id }))
}

/// Whether a Results path's plan segment names the token's plan. The bare plan
/// id does, and so does the runner's `system.orchestrationId`
/// (`{planId}.{jobId}.{suffix}`): the runner sends that value as the plan
/// segment when it addresses its own job, with the plan id as its prefix.
fn path_names_token_plan(path_plan: &str, token_plan: &str) -> bool {
    !token_plan.is_empty()
        && (path_plan == token_plan
            || path_plan
                .strip_prefix(token_plan)
                .is_some_and(|rest| rest.starts_with('.')))
}

/// Whether a job token addresses the Results target `plan_id` / `job_id`. The
/// job UUID must match exactly, so a token never reaches another job, and the
/// plan segment must name the token's own plan (see [`path_names_token_plan`]).
pub fn results_identity_binds_job(identity: &ResultsIdentity, plan_id: &str, job_id: &str) -> bool {
    match identity {
        ResultsIdentity::System => true,
        ResultsIdentity::Job(identity) => {
            path_names_token_plan(plan_id, &identity.plan_id)
                && job_id
                    .parse::<uuid::Uuid>()
                    .is_ok_and(|job| job == identity.job_id)
        }
    }
}

/// Require the typed Results identity to name the exact plan/job target.
pub fn require_results_job(
    identity: &ResultsIdentity,
    plan_id: &str,
    job_id: &str,
) -> Result<(), ApiError> {
    if results_identity_binds_job(identity, plan_id, job_id) {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "results-service token is not bound to that job",
        ))
    }
}

/// Authorize a Results plan/job target and return its storage representation.
///
/// Runtime job identities only pass UUID-parsable job ids, so alternate
/// spellings collapse to the lower-case hyphenated form used by the runner's
/// lookup path. The system identity keeps accepting opaque backend ids for
/// compatibility, while still canonicalizing valid UUIDs.
pub fn require_canonical_results_job_id(
    identity: &ResultsIdentity,
    plan_id: &str,
    job_id: &str,
) -> Result<String, ApiError> {
    require_results_job(identity, plan_id, job_id)?;
    Ok(job_id
        .parse::<uuid::Uuid>()
        .map(|job| job.to_string())
        .unwrap_or_else(|_| job_id.to_owned()))
}

/// The plan a Results handler must key storage by: a job token's own plan.
///
/// A runner may address its own job with the plan segment spelled as its
/// `system.orchestrationId` (`{planId}.{jobId}.__default`), which
/// `path_names_token_plan` accepts for authorization. A replay path or
/// metadata key built from that spelling names a namespace no reader resolves
/// (logs are read back under the plan id, `runs.rs`), so handlers that write
/// storage use the token's own plan instead. The system identity keeps the
/// requested segment: engine callers address opaque backend ids.
pub fn results_storage_plan_id(identity: &ResultsIdentity, requested_plan_id: &str) -> String {
    match identity {
        ResultsIdentity::System => requested_plan_id.to_owned(),
        ResultsIdentity::Job(job) => job.plan_id.clone(),
    }
}

/// Runner identity proven by a listen token on this request.
///
/// `runner_id` is `Some` only when the bearer verifies as a runner-listen JWT
/// *and* names a registered runner. Anything else — missing header, engine
/// token, job runtime token, unresolvable mock subject — leaves it `None`,
/// which handlers must treat as unverified (never as a runner).
#[derive(Clone, Debug, Default)]
pub struct RunnerIdentity {
    pub runner_id: Option<i64>,
}

/// Non-rejecting resolver: tags every request with the [`RunnerIdentity`] its
/// bearer proves, if any. The runner protocol predates authentication on
/// several endpoints and external runners still rely on that, so enforcement
/// decisions belong to the handlers; this layer only makes the identity
/// available so handlers cannot be talked into trusting request bodies.
pub async fn resolve_runner_identity(
    State(shared): State<Arc<SharedState>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let bearer = bearer_token(&request).map(str::to_owned);
    let runner_id = match bearer.as_deref() {
        Some(token) => registered_runner_id(&shared, token).await,
        None => None,
    };
    request
        .extensions_mut()
        .insert(RunnerIdentity { runner_id });
    Ok(next.run(request).await)
}

/// Compute the runner a session may claim jobs for, reconciling the session's
/// stored binding with the token proven on the request.
///
/// - verified identity conflicting with the binding → `None` (no claims)
/// - verified identity otherwise → that runner
/// - unverified request → `None` (legacy permissive claims only; the claim
///   filter then bars assigned and pool-pending jobs)
pub fn effective_claim_runner(
    identity: Option<&RunnerIdentity>,
    bound: Option<i64>,
) -> Option<i64> {
    match (identity.and_then(|id| id.runner_id), bound) {
        (Some(verified), Some(bound)) if verified != bound => None,
        (Some(verified), _) => Some(verified),
        (None, _) => None,
    }
}

/// Worker-authenticated debug operations that the trusted runner performs
/// through the guest's mounted control socket.
fn is_worker_debug_route(method: &axum::http::Method, path: &str) -> bool {
    use axum::http::Method;

    if method == Method::POST {
        if matches!(
            path,
            crate::routes::DEBUG_WORKER_TOKEN_PATH | crate::routes::DEBUG_SESSIONS_PATH
        ) {
            return true;
        }
        return debug_session_member(path, crate::routes::DEBUG_SESSION_CLOSE_SUFFIX);
    }
    method == Method::GET && debug_session_member(path, crate::routes::DEBUG_SESSION_VERDICT_SUFFIX)
}

/// The two read-only repository shapes a job's checkout issues against
/// `/api/v3` when it cannot run Git: the default-branch lookup
/// (`/repos/{o}/{r}`) and the archive download (`/repos/{o}/{r}/tarball/{ref}`,
/// ref may contain `/`). Both handlers pin the caller to a job runtime token
/// (`authorize_forge_relay` / snapshot token), so the socket surface can carry
/// them without exposing the runner-management API — every other `/api/v3/*`
/// shape stays refused.
fn job_scoped_repo_read(method: &axum::http::Method, path: &str) -> bool {
    if *method != axum::http::Method::GET {
        return false;
    }
    let Some(rest) = path.strip_prefix("/api/v3/repos/") else {
        return false;
    };
    let mut segments = rest.split('/');
    let (Some(owner), Some(repo)) = (segments.next(), segments.next()) else {
        return false;
    };
    if owner.is_empty() || repo.is_empty() {
        return false;
    }
    match segments.next() {
        None => true,
        Some("tarball") => segments.next().is_some(),
        Some(_) => false,
    }
}

fn debug_session_member(path: &str, suffix: &str) -> bool {
    path.strip_prefix(crate::routes::DEBUG_SESSIONS_PATH)
        .and_then(|rest| rest.strip_prefix('/'))
        .and_then(|rest| rest.strip_suffix(suffix))
        .is_some_and(|session_id| !session_id.is_empty() && !session_id.contains('/'))
}

/// Socket-surface guard: the mounted control socket is reachable from inside
/// every runner VM, so anything not part of the runner/broker protocol is
/// refused there. Native management and GUI API prefixes have no legitimate
/// use from a guest.
pub async fn runner_surface_only(mut request: Request, next: Next) -> Result<Response, ApiError> {
    // `/setup/*` is the operator's browser-facing App setup. A job VM that
    // reached it could only ever see 404s, but the socket stays fail-closed.
    const DENIED_PREFIXES: &[&str] = &["/internal/", "/runs/", "/repos/", "/setup/"];
    let path = request.uri().path();
    let worker_debug_route = is_worker_debug_route(request.method(), path);
    let denied = DENIED_PREFIXES
        .iter()
        .any(|prefix| path.starts_with(prefix))
        // `/api/v1/actions/*` is the runner's own action-archive download
        // (sanitized GitHub tarball paths), which the runner executes inside
        // the VM and therefore must be able to reach through the mounted
        // control socket. `/replay/*` is the other half of the same class:
        // the in-VM runner uploads its step logs and summaries to the signed
        // blob URLs its own Twirp handlers minted. Every other native prefix
        // stays off the guest surface except the worker half of live
        // debugging: token exchange, session open, verdict poll, and close.
        // The mounted socket deliberately excludes `/api/v3/*`, which mints
        // runner-management JWTs (`RunnerManage` scope) for the
        // GitHub-compatible registration flow. That service is engine-facing;
        // untrusted workflow code must not reach it through the socket because
        // it could mint runner-management credentials. The one exception is
        // the runner's own registration: its handler requires the system
        // credential on this surface even when TCP registration is explicitly
        // permissive, so workflow code cannot use the carve-out to mint anything.
        || (path.starts_with("/api/v3/") && path != "/api/v3/actions/runner-registration"
            && !job_scoped_repo_read(request.method(), path))
        || (path.starts_with("/api/v1/")
            && !path.starts_with("/api/v1/actions/")
            && !worker_debug_route);
    if denied {
        return Err(ApiError::not_found(format!(
            "{path} not available on this endpoint"
        )));
    }
    request.extensions_mut().insert(SocketSurface);
    Ok(next.run(request).await)
}

/// The job a worker request speaks for, proven by its debug-worker token.
///
/// Carried in request extensions so handlers authorize against the job rather
/// than against mere token validity. Without it every worker route is
/// reachable by any live job's token, which makes session ids the only thing
/// standing between one job and another's debug session.
#[derive(Debug, Clone, Copy)]
pub struct WorkerJob(pub uuid::Uuid);

/// Require a job debug-worker token and record which job it names.
///
/// Deliberately not the job runtime token: that one is injected into the job
/// as `GITHUB_TOKEN`, so any workflow step could read it and drive debug
/// surfaces on its own behalf. The debug-worker token is minted per job
/// (`sub: preloop-debug-worker-{uuid}`) and delivered only to the trusted runner
/// process, so it identifies the caller precisely; neither a runtime token nor
/// a runner listen token is accepted here.
pub async fn require_worker_bearer(
    State(shared): State<Arc<SharedState>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let job =
        bearer_token(&request).and_then(|token| shared.state.job_uuid_from_debug_token(token));
    match job {
        Some(job) => {
            require_live_job(&shared.state, job).await?;
            request.extensions_mut().insert(WorkerJob(job));
            Ok(next.run(request).await)
        }
        None => Err(ApiError::unauthorized("job debug-worker token required")),
    }
}

/// The job a request speaks for, proven by its runtime token.
///
/// Distinct type from [`WorkerJob`] on purpose: a runtime token is weaker —
/// the runner exports it to steps as `ACTIONS_RUNTIME_TOKEN` — so the two must
/// not be interchangeable in a handler signature. Only the credential
/// *exchange* accepts this identity; the debug-session routes themselves still
/// demand a [`WorkerJob`].
#[derive(Debug, Clone, Copy)]
pub struct JobRuntimeIdentity(pub uuid::Uuid);

/// Require a job runtime token and record which job it names.
///
/// The runtime token is the only credential a worker process holds that the
/// server can tie to a single job, so it is what the debug-worker token
/// exchange authenticates with. It buys nothing on its own: the handler still
/// has to find a live, pause-enabled job request for that exact job, and the
/// exchange is one-shot. A runner listen token, the native system token and a
/// debug-worker token are all rejected here — each names something other than
/// one job.
pub async fn require_job_runtime_bearer(
    State(shared): State<Arc<SharedState>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let job = bearer_token(&request).and_then(|token| shared.state.job_uuid_from_token(token));
    match job {
        Some(job) => {
            require_live_job(&shared.state, job).await?;
            request.extensions_mut().insert(JobRuntimeIdentity(job));
            Ok(next.run(request).await)
        }
        None => Err(ApiError::unauthorized("job runtime token required")),
    }
}

/// Resolve the repository authorized by a job runtime bearer.
pub async fn job_repository_from_headers(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<String>, ApiError> {
    let Some(token) = bearer_from_headers(headers) else {
        return Err(ApiError::unauthorized("job runtime token required"));
    };
    if token == state.system_token.as_str() {
        return Ok(None);
    }
    let job_id = state
        .job_uuid_from_token(token)
        .ok_or_else(|| ApiError::unauthorized("job runtime token required"))?;
    let repository = state
        .backend
        .attempt_repository(job_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError::forbidden("job runtime token is not bound to a live workflow run")
        })?;
    Ok(Some(repository))
}

/// Resolve the git ref of the job behind a job runtime bearer, from
/// the job → run → submission chain. Mirrors `job_repository_from_headers`.
/// Returns `None` for the system token (the engine itself has no job).
pub async fn job_git_ref_from_headers(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<String>, ApiError> {
    let Some(token) = bearer_from_headers(headers) else {
        return Err(ApiError::unauthorized("job runtime token required"));
    };
    if token == state.system_token.as_str() {
        return Ok(None);
    }
    let job_id = state
        .job_uuid_from_token(token)
        .ok_or_else(|| ApiError::unauthorized("job runtime token required"))?;
    let run_id = state
        .backend
        .run_for_attempt(job_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError::forbidden("job runtime token is not bound to a live workflow run")
        })?;
    let git_ref = state
        .backend
        .run_record(run_id)
        .await
        .map_err(ApiError::from)?
        .submission
        .git_ref
        .clone();
    Ok(Some(git_ref))
}

/// Cache-scoping context resolved server-side from the job → run →
/// submission chain. Every field comes from the run record the token's job
/// belongs to — never from request bodies — so a compromised job cannot
/// move its cache namespace across repositories or branches.
#[derive(Debug, Clone)]
pub struct JobCacheContext {
    /// Repository the run belongs to (`owner/repo`).
    pub repository: String,
    /// Git ref the run executes on (`refs/heads/feature`,
    /// `refs/pull/42/merge`, `refs/tags/v1`, …).
    pub git_ref: String,
    /// The repository's default branch as a full ref, taken from the event
    /// payload's `repository.default_branch`. Falls back to
    /// `refs/heads/main` only when the payload does not carry it (native
    /// submissions, pre-payload fixtures).
    pub default_branch_ref: String,
    /// For `pull_request`/`pull_request_target` runs, the PR's base branch
    /// as a full ref (`refs/heads/release`). `None` for non-PR events.
    pub base_ref: Option<String>,
}

/// Resolve the cache-scoping context of the job behind a job runtime
/// bearer. Returns `None` for the system token (the engine itself has no
/// job); a job token that resolves to no live run fails closed.
pub async fn job_cache_context_from_headers(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<JobCacheContext>, ApiError> {
    let Some(token) = bearer_from_headers(headers) else {
        return Err(ApiError::unauthorized("job runtime token required"));
    };
    if token == state.system_token.as_str() {
        return Ok(None);
    }
    let job_id = state
        .job_uuid_from_token(token)
        .ok_or_else(|| ApiError::unauthorized("job runtime token required"))?;
    let run_id = state
        .backend
        .run_for_attempt(job_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError::forbidden("job runtime token is not bound to a live workflow run")
        })?;
    let run = state
        .backend
        .run_record(run_id)
        .await
        .map_err(ApiError::from)?;
    let submission = &run.submission;
    let default_branch = submission
        .payload
        .get("repository")
        .and_then(|repo| repo.get("default_branch"))
        .and_then(serde_json::Value::as_str)
        .filter(|branch| !branch.is_empty())
        .unwrap_or("main");
    let base_ref = submission
        .payload
        .get("pull_request")
        .and_then(|pr| pr.get("base"))
        .and_then(|base| base.get("ref"))
        .and_then(serde_json::Value::as_str)
        .filter(|branch| !branch.is_empty())
        .map(|branch| format!("refs/heads/{branch}"));
    Ok(Some(JobCacheContext {
        repository: submission.repository.clone(),
        git_ref: submission.git_ref.clone(),
        default_branch_ref: format!("refs/heads/{default_branch}"),
        base_ref,
    }))
}

pub fn job_runtime_claims_from_headers(
    state: &AppState,
    headers: &HeaderMap,
) -> Option<JobRuntimeClaims> {
    let token = bearer_from_headers(headers)?;
    state.job_runtime_claims_from_token(token)
}

#[derive(Debug, Clone)]
pub struct JobRuntimeClaims {
    pub plan_id: String,
    pub job_id: uuid::Uuid,
}

#[cfg(test)]
mod job_scoped_repo_read_tests {
    use super::*;

    #[test]
    fn job_scoped_repo_read_allows_only_the_checkout_shapes() {
        // The two shapes a redirected checkout issues.
        assert!(job_scoped_repo_read(
            &axum::http::Method::GET,
            "/api/v3/repos/valkey-io/valkey"
        ));
        assert!(job_scoped_repo_read(
            &axum::http::Method::GET,
            "/api/v3/repos/snapshots/7b116f3a-6973-4e4e-bcd1-213de9518ad1/tarball/de2d615e"
        ));
        assert!(job_scoped_repo_read(
            &axum::http::Method::GET,
            "/api/v3/repos/o/r/tarball/refs/heads/main"
        ));
        // Anything else under /api/v3 stays off the socket surface: the
        // runner-management endpoints mint credentials.
        assert!(!job_scoped_repo_read(
            &axum::http::Method::GET,
            "/api/v3/actions/runners/registration-token"
        ));
        assert!(!job_scoped_repo_read(
            &axum::http::Method::GET,
            "/api/v3/repos/o"
        ));
        assert!(!job_scoped_repo_read(
            &axum::http::Method::GET,
            "/api/v3/repos//r"
        ));
        assert!(!job_scoped_repo_read(
            &axum::http::Method::GET,
            "/api/v3/repos/o/r/tarball"
        ));
        assert!(!job_scoped_repo_read(
            &axum::http::Method::GET,
            "/api/v3/repos/o/r/actions/runners/registration-token"
        ));
        // Reads only: nothing may be created or mutated through the socket.
        assert!(!job_scoped_repo_read(
            &axum::http::Method::POST,
            "/api/v3/repos/o/r/tarball/main"
        ));
    }
}

#[cfg(test)]
mod runner_surface_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::StatusCode;
    use tower::ServiceExt as _;

    #[tokio::test]
    async fn the_guest_socket_refuses_the_browser_setup_pages() {
        let app = axum::Router::new()
            .route("/setup/github", axum::routing::get(|| async { "form" }))
            .route(
                "/setup/github/callback",
                axum::routing::get(|| async { "callback" }),
            )
            .route(
                "/runner/message",
                axum::routing::get(|| async { "message" }),
            )
            .layer(axum::middleware::from_fn(runner_surface_only));
        for path in ["/setup/github", "/setup/github/callback"] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{path} must not be reachable from a job VM"
            );
        }
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/runner/message")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "the runner protocol still passes"
        );
    }
}
