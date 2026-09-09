use super::*;

pub(crate) async fn require_protocol_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let authorized = bearer_token(&request).is_some_and(|token| {
        token == shared.state.system_token || shared.state.verify_local_jwt_claims(token).is_some()
    });
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized(
            "runner or job protocol token required",
        ))
    }
}
/// Require that a protocol reporting request is authenticated by the system
/// credential or by the runtime token minted for the exact job request.
///
/// The generic protocol layer intentionally accepts several local JWT classes
/// because it fronts both runner and results-service routes. Reporting is
/// narrower: workflow code can read its own runtime token, and a listen or
/// manage token must not be able to finish another job.
pub(crate) fn authorize_reporting_request(
    state: &AppState,
    headers: &HeaderMap,
    request: Option<&TaskAgentJobRequestRecord>,
) -> Result<(), ApiError> {
    let token = bearer_from_headers(headers)
        .ok_or_else(|| ApiError::unauthorized("runner or job protocol token required"))?;
    if token == state.system_token {
        return Ok(());
    }

    let Some(request) = request else {
        return Err(ApiError::forbidden(
            "job runtime token cannot resolve the reporting target",
        ));
    };
    let authorized = state.verify_local_jwt_claims(token).is_some_and(|claims| {
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
    });
    if authorized {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "job runtime token does not own the reporting target",
        ))
    }
}

pub(crate) async fn require_results_bearer(
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
        if request.method() == axum::http::Method::PUT
            && !verify_replay_upload_ticket(&shared.state, request.uri())
        {
            return Err(ApiError::unauthorized(
                "replay blob upload requires a signed URL ticket",
            ));
        }
        return Ok(next.run(request).await);
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
    // re-parsing a bearer string independently.
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

pub(crate) async fn require_test_api_token(
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

pub(crate) fn system_bearer_authorized(state: &AppState, headers: &HeaderMap) -> bool {
    bearer_from_headers(headers).is_some_and(|token| token == state.system_token)
}

fn request_system_bearer_authorized(shared: &Arc<SharedState>, request: &Request) -> bool {
    bearer_token(request).is_some_and(|token| token == shared.state.system_token)
}

pub(crate) async fn require_system_bearer(
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
pub(crate) enum AdminCaller {
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
pub(crate) async fn require_runner_admin_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if request_system_bearer_authorized(&shared, &request) {
        return Ok(next.run(request).await);
    }
    let manager_token = bearer_token(&request)
        .and_then(|token| shared.state.verify_local_jwt_claims(token))
        .and_then(|claims| {
            claims
                .get("scp")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        })
        .is_some_and(|scope| {
            scope
                .split_whitespace()
                .any(|value| value == "ActionsRuntime.RunnerManage")
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
pub(crate) fn admin_caller(
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
        .and_then(|claims| {
            claims
                .get("scp")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        })
        .is_some_and(|scope| {
            scope
                .split_whitespace()
                .any(|value| value == "ActionsRuntime.RunnerManage")
        })
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

pub(crate) async fn require_native_bearer(
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

pub(crate) async fn require_runner_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if let Some(token) = bearer_token(&request) {
        if registered_runner_id(&shared, token).await.is_some() {
            return Ok(next.run(request).await);
        }
    }
    Err(ApiError::unauthorized("runner listen token required"))
}
pub(crate) fn runner_registration_bearer_authorized(state: &AppState, token: &str) -> bool {
    token == state.system_token.as_str()
        || state.verify_local_jwt_claims(token).is_some_and(|claims| {
            claims.get("sub").and_then(|value| value.as_str())
                == Some("preloop-runner-registration")
                && claims
                    .get("scp")
                    .and_then(|value| value.as_str())
                    .is_some_and(|scope| {
                        scope
                            .split_whitespace()
                            .any(|value| value == "ActionsRuntime.RunnerManage")
                    })
        })
}

/// Require the credential that is allowed to create a runner identity through
/// the legacy AzDO registration aliases. The GitHub-compatible registration
/// endpoint mints this local RunnerManage token; a pool provision token is the
/// other supported path for the host-side configure flow.
pub(crate) async fn require_runner_registration_bearer(
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
pub(crate) async fn require_legacy_runner_bearer(
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

fn pending_provision_token(state: &AppState, token: &str) -> bool {
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

/// Atomically consume a pool-issued registration credential. The middleware
/// performs the fast authorization check; the handler consumes the token
/// under the write lock so concurrent registration attempts cannot both pass.
/// The issuance time is returned so a validation failure can restore the
/// credential without extending its lifetime.
pub(crate) fn consume_pending_provision_token(
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

pub(crate) fn restore_pending_provision_token(
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
pub(crate) async fn registered_runner_id(shared: &Arc<SharedState>, token: &str) -> Option<i64> {
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
            .inner
            .lock()
            .await
            .runner_client_ids
            .get(&client_id)
            .copied()
    };
    let runner_id = runner_id?;
    runner_registered(shared, runner_id)
        .await
        .then_some(runner_id)
}

pub(crate) fn bearer_token(request: &Request) -> Option<&str> {
    bearer_from_headers(request.headers())
}

pub(crate) fn bearer_from_headers(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

/// Whether the runner still has a live registration. Agent deregistration and
/// machine teardown both route through [`crate::runner_lifecycle::purge_runner_identity`],
/// which removes the registration entry — that removal is what revokes every
/// listen token previously issued to the runner. A JWT that outlives its
/// runner must not authenticate or resolve to an identity, or a stolen token
/// would keep creating sessions and pulling work after teardown.
async fn runner_registered(shared: &Arc<SharedState>, runner_id: i64) -> bool {
    shared
        .state
        .inner
        .lock()
        .await
        .runners
        .contains_key(&runner_id)
}

/// Signed replay blob upload tickets expire after one hour.
pub(crate) const REPLAY_TICKET_TTL_SECS: u64 = 3600;

pub(crate) fn replay_ticket_expiry() -> u64 {
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
pub(crate) fn sign_replay_upload_ticket(state: &AppState, path: &str, expires_at: u64) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(&state.local_jwt_key)
        .expect("HMAC accepts keys of any length");
    mac.update(replay_ticket_payload(path, expires_at).as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

/// Whether the `sig` query parameter of `uri` authorises an upload to exactly
/// that replay path before the signed Unix expiry (`se`).
pub(crate) fn verify_replay_upload_ticket(state: &AppState, uri: &axum::http::Uri) -> bool {
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

/// Marker extension: the request arrived through the mounted control socket.
///
/// Handlers use it to distinguish the untrusted VM surface from the operator's
/// TCP surface. Workflow code inside a runner VM can reach the socket, so a
/// handler that mints anything (e.g. runner-management JWTs) may require a
/// stronger credential there than on TCP, where GitHub-compatible clients
/// (official runner, conformance replays) present credentials only they hold.
pub(crate) struct SocketSurface;

impl Clone for SocketSurface {
    fn clone(&self) -> Self {
        SocketSurface
    }
}

/// The authenticated identity carried by a Results bearer is used by every
/// Results handler, including signed-URL minting and metadata mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResultsJobIdentity {
    pub(crate) plan_id: String,
    pub(crate) job_id: uuid::Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResultsIdentity {
    /// The engine credential is allowed to address every Results target.
    System,
    /// A runtime credential is bound to one plan/job pair.
    Job(ResultsJobIdentity),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResultsIdentityError {
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
pub(crate) fn results_identity(
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

pub(crate) fn results_identity_binds_job(
    identity: &ResultsIdentity,
    plan_id: &str,
    job_id: &str,
) -> bool {
    match identity {
        ResultsIdentity::System => true,
        ResultsIdentity::Job(identity) => {
            identity.plan_id == plan_id
                && job_id
                    .parse::<uuid::Uuid>()
                    .is_ok_and(|job| job == identity.job_id)
        }
    }
}

/// Require the typed Results identity to name the exact plan/job target.
pub(crate) fn require_results_job(
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
pub(crate) fn require_canonical_results_job_id(
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

/// Runner identity proven by a listen token on this request.
///
/// `runner_id` is `Some` only when the bearer verifies as a runner-listen JWT
/// *and* names a registered runner. Anything else — missing header, engine
/// token, job runtime token, unresolvable mock subject — leaves it `None`,
/// which handlers must treat as unverified (never as a runner).
#[derive(Clone, Debug, Default)]
pub(crate) struct RunnerIdentity {
    pub(crate) runner_id: Option<i64>,
}

/// Non-rejecting resolver: tags every request with the [`RunnerIdentity`] its
/// bearer proves, if any. The runner protocol predates authentication on
/// several endpoints and external runners still rely on that, so enforcement
/// decisions belong to the handlers; this layer only makes the identity
/// available so handlers cannot be talked into trusting request bodies.
pub(crate) async fn resolve_runner_identity(
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
pub(crate) fn effective_claim_runner(
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
pub(crate) async fn runner_surface_only(
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    const DENIED_PREFIXES: &[&str] = &["/internal/", "/runs/", "/repos/"];
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
        || (path.starts_with("/api/v3/") && path != "/api/v3/actions/runner-registration")
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
pub(crate) struct WorkerJob(pub(crate) uuid::Uuid);

/// Require a job debug-worker token and record which job it names.
///
/// Deliberately not the job runtime token: that one is injected into the job
/// as `GITHUB_TOKEN`, so any workflow step could read it and drive debug
/// surfaces on its own behalf. The debug-worker token is minted per job
/// (`sub: preloop-debug-worker-{uuid}`) and delivered only to the trusted runner
/// process, so it identifies the caller precisely; neither a runtime token nor
/// a runner listen token is accepted here.
pub(crate) async fn require_worker_bearer(
    State(shared): State<Arc<SharedState>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let job =
        bearer_token(&request).and_then(|token| shared.state.job_uuid_from_debug_token(token));
    match job {
        Some(job) => {
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
pub(crate) struct JobRuntimeIdentity(pub(crate) uuid::Uuid);

/// Require a job runtime token and record which job it names.
///
/// The runtime token is the only credential a worker process holds that the
/// server can tie to a single job, so it is what the debug-worker token
/// exchange authenticates with. It buys nothing on its own: the handler still
/// has to find a live, pause-enabled job request for that exact job, and the
/// exchange is one-shot. A runner listen token, the native system token and a
/// debug-worker token are all rejected here — each names something other than
/// one job.
pub(crate) async fn require_job_runtime_bearer(
    State(shared): State<Arc<SharedState>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let job = bearer_token(&request).and_then(|token| shared.state.job_uuid_from_token(token));
    match job {
        Some(job) => {
            request.extensions_mut().insert(JobRuntimeIdentity(job));
            Ok(next.run(request).await)
        }
        None => Err(ApiError::unauthorized("job runtime token required")),
    }
}

/// Resolve the repository authorized by a job runtime bearer.
pub(crate) async fn job_repository_from_headers(
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
    let inner = state.inner.lock().await;
    let repository = inner
        .agent_job_requests
        .get(&job_id)
        .and_then(|request_id| inner.job_requests.get(request_id))
        .and_then(|record| inner.runs.get(&record.run_id))
        .map(|run| run.submission.repository.clone())
        .ok_or_else(|| {
            ApiError::forbidden("job runtime token is not bound to a live workflow run")
        })?;
    Ok(Some(repository))
}

pub(crate) fn job_runtime_claims_from_headers(
    state: &AppState,
    headers: &HeaderMap,
) -> Option<JobRuntimeClaims> {
    let token = bearer_from_headers(headers)?;
    state.job_runtime_claims_from_token(token)
}


#[derive(Debug, Clone)]
pub(crate) struct JobRuntimeClaims {
    pub(crate) plan_id: String,
    pub(crate) job_id: uuid::Uuid,
}
