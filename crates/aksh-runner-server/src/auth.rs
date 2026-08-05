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
    if !request.uri().path().starts_with("/twirp/") {
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
    let authorized = bearer_token(&request)
        .and_then(|token| shared.state.runner_id_from_token(token))
        .is_some();
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized("runner listen token required"))
    }
}

pub(crate) fn bearer_token(request: &Request) -> Option<&str> {
    request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
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
