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
