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

pub(crate) async fn require_results_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
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
    let authorized = bearer_token(&request).is_some_and(|token| {
        token == shared.state.system_token
            || shared
                .state
                .verify_local_jwt_claims(token)
                .is_some_and(|claims| {
                    claims
                        .get("scp")
                        .and_then(|value| value.as_str())
                        .is_some_and(|scope| scope.starts_with("Actions.Results:"))
                })
    });
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized("results-service job token required"))
    }
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

pub(crate) async fn require_native_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let authorized = bearer_token(&request).is_some_and(|token| token == shared.state.system_token);
    if authorized {
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
    let runner_id =
        bearer_token(&request).and_then(|token| shared.state.runner_id_from_token(token));
    let authorized = match runner_id {
        Some(runner_id) => runner_registered(&shared, runner_id).await,
        None => false,
    };
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized("runner listen token required"))
    }
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

/// Sign a replay blob upload URL for `path` (e.g. `/replay/results/{plan}/{job}/step-1.txt`).
///
/// The upload route is deliberately bearerless — the runner PUTs bytes to the
/// URL its own Twirp handler returned — and it is reachable from inside every
/// runner VM, where workflow code runs. Unsigned, the route let a guest
/// overwrite another job's stored logs and summaries by guessing the URL
/// shape. Binding the signature to the exact path makes a URL minted for one
/// job worthless against any other, and only holders of the per-instance HMAC
/// key (the mint handlers, gated on a job-scoped token) can produce one.
pub(crate) fn sign_replay_upload_ticket(state: &AppState, path: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(&state.local_jwt_key)
        .expect("HMAC accepts keys of any length");
    mac.update(replay_ticket_payload(path).as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

/// Whether the `sig` query parameter of `uri` authorises an upload to exactly
/// that replay path. Constant-time comparison via `verify_slice`.
pub(crate) fn verify_replay_upload_ticket(state: &AppState, uri: &axum::http::Uri) -> bool {
    let Some(query) = uri.query() else {
        return false;
    };
    let params: std::collections::HashMap<String, String> =
        serde_urlencoded::from_str(query).unwrap_or_default();
    let Some(signature) = params.get("sig").map(String::as_str) else {
        return false;
    };
    let Ok(provided) = URL_SAFE_NO_PAD.decode(signature) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(&state.local_jwt_key) else {
        return false;
    };
    mac.update(replay_ticket_payload(uri.path()).as_bytes());
    mac.verify_slice(&provided).is_ok()
}

fn replay_ticket_payload(path: &str) -> String {
    format!("replay-blob\n{path}")
}

/// Whether a caller may mint replay blob URLs for this exact plan/job pair.
///
/// The engine token may mint for any job (it is the administrator credential).
/// A job runtime token is weaker — the runner exports it to steps as
/// `ACTIONS_RUNTIME_TOKEN` — so it must only mint for the one job it names:
/// both the subject and the `Actions.Results:{plan}:{job}` scope have to
/// match the requested backend ids. Without this, workflow code holding its
/// own runtime token could ask the mint handler for *another* job's signed
/// URL and then overwrite that job's logs through it.
pub(crate) fn results_token_binds_job(
    state: &AppState,
    bearer: Option<&str>,
    plan_id: &str,
    job_id: &str,
) -> bool {
    match bearer {
        Some(token) if token == state.system_token => true,
        Some(token) => state.verify_local_jwt_claims(token).is_some_and(|claims| {
            let subject_job = claims
                .get("sub")
                .and_then(|value| value.as_str())
                .and_then(|subject| subject.strip_prefix("aksh-job-"))
                .unwrap_or("");
            let scope = claims
                .get("scp")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            subject_job == job_id && scope == format!("Actions.Results:{plan_id}:{job_id}")
        }),
        None => false,
    }
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
    let mut runner_id = bearer
        .as_deref()
        .and_then(|token| shared.state.runner_id_from_token(token));
    if runner_id.is_none() {
        let client_id = bearer
            .as_deref()
            .and_then(|token| shared.state.verify_local_jwt_claims(token))
            .and_then(|claims| {
                claims
                    .get("sub")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            })
            .and_then(|sub| {
                sub.strip_prefix("aksh-runner-listen-mock-")
                    .map(str::to_owned)
            });
        if let Some(client_id) = client_id {
            // Mock-token subjects name the client id instead of the runner
            // id; resolve through the registration table so mock-flow clients
            // get the same identity enforcement as real PS256 clients.
            runner_id = shared
                .state
                .inner
                .lock()
                .await
                .runner_client_ids
                .get(&client_id)
                .copied();
        }
    }
    // A listen token is only as good as the registration that backs it:
    // purge removes the runner entry, which revokes every token previously
    // issued to it. An unregistered runner must not resolve to an identity,
    // or a stolen token could keep creating verified sessions after teardown.
    if let Some(id) = runner_id {
        if !runner_registered(&shared, id).await {
            runner_id = None;
        }
    }
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

/// Socket-surface guard: the mounted control socket is reachable from inside
/// every runner VM, so anything not part of the runner/broker protocol is
/// refused there. Native management and GUI API prefixes have no legitimate
/// use from a guest.
pub(crate) async fn runner_surface_only(
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    const DENIED_PREFIXES: &[&str] = &["/internal/", "/runs/"];
    let path = request.uri().path();
    let denied = DENIED_PREFIXES
        .iter()
        .any(|prefix| path.starts_with(prefix))
        // `/api/v1/actions/*` is the runner's own action-archive download
        // (sanitized GitHub tarball paths), which the runner executes inside
        // the VM and therefore must be able to reach through the mounted
        // control socket. `/replay/*` is the other half of the same class:
        // the in-VM runner uploads its step logs and summaries to the signed
        // blob URLs its own Twirp handlers minted. Every other native prefix
        // stays off the guest surface: workflow code is untrusted.
        // `/api/v3/*` mints runner-management JWTs (`RunnerManage` scope) for
        // the GitHub-compatible registration flow — an engine-facing service
        // that untrusted workflow code must never reach through the mounted
        // control socket, or it could mint runner-management credentials. The
        // one exception is the runner's own registration: the engine itself
        // initiates it at provision time, and the handler now requires the
        // system credential, which workflow code never holds — so the carve
        // out cannot be used to mint anything.
        || (path.starts_with("/api/v3/") && path != "/api/v3/actions/runner-registration")
        || (path.starts_with("/api/v1/") && !path.starts_with("/api/v1/actions/"));
    if denied {
        return Err(ApiError::not_found(format!(
            "{path} not available on this endpoint"
        )));
    }
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
/// (`sub: aksh-debug-worker-{uuid}`) and delivered only to the trusted runner
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
