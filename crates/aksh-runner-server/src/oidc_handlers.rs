use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct OidcTokenQuery {
    pub(crate) audience: Option<String>,
    #[serde(rename = "api-version")]
    pub(crate) _api_version: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct OidcTokenResponse {
    pub(crate) value: String,
}

/// `GET /runner/server/_apis/distributedtask/hubs/actions/plans/:plan_id/jobs/:job_id/oidctoken`
///
/// Mints a GitHub-compatible RS256-signed OIDC id-token JWT. Looks up the
/// originating workflow run to populate claims, and enforces `id-token: write`.
pub(crate) async fn oidc_token_run_service(
    State(shared): State<Arc<SharedState>>,
    Path((_orchestration_id, plan_id, job_id)): Path<(String, String, String)>,
    Query(query): Query<OidcTokenQuery>,
    headers: HeaderMap,
) -> Result<Json<OidcTokenResponse>, ApiError> {
    oidc_token(
        State(shared),
        Path((plan_id, job_id)),
        Query(query),
        headers,
    )
    .await
}

pub(crate) async fn oidc_token(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, job_id)): Path<(String, String)>,
    Query(query): Query<OidcTokenQuery>,
    headers: HeaderMap,
) -> Result<Json<OidcTokenResponse>, ApiError> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError::unauthorized("OIDC bearer token required"))?;
    let expected_scope = format!("Actions.Results:{plan_id}:{job_id}");
    if !shared.state.verify_local_jwt_scope(bearer, &expected_scope) {
        return Err(ApiError::forbidden(
            "OIDC runtime token is not bound to this job",
        ));
    }
    let inner = shared.state.inner.lock().await;
    let request_id = inner
        .plan_requests
        .get(&plan_id)
        .copied()
        .ok_or_else(|| ApiError::not_found("OIDC: plan not found"))?;
    let request = inner
        .job_requests
        .get(&request_id)
        .ok_or_else(|| ApiError::not_found("OIDC: job request not found"))?;
    if request.agent_job_id.to_string() != job_id {
        return Err(ApiError::not_found("OIDC: plan and job do not match"));
    }
    let run_id = request.run_id;
    let resolved_job_id = request.job_id.clone();

    // Permission enforcement: id-token:write must be granted.
    let granted = inner
        .id_token_grants
        .get(&(run_id, resolved_job_id.clone()))
        .copied()
        .unwrap_or(false);
    if !granted {
        return Err(ApiError::forbidden(
            "id-token: write permission is required to request an OIDC token",
        ));
    }

    // Get the OIDC signing keypair.
    let oidc_kp = inner
        .oidc_keypair
        .as_ref()
        .ok_or_else(|| ApiError::internal("OIDC signing keypair not available"))?
        .clone();

    let oidc_context = inner
        .oidc_job_contexts
        .get(&(run_id, resolved_job_id.clone()))
        .cloned()
        .ok_or_else(|| ApiError::internal("OIDC context missing for dispatched job"))?;

    // Build claims from the run's submission and parser-resolved job context.
    let run = inner
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("OIDC: run not found"))?;
    let submission = &run.submission;
    let repository_owner = submission
        .repository
        .split('/')
        .next()
        .unwrap_or("owner")
        .to_string();

    // Use sha from submission first-class field, fallback to payload extraction.
    let sha = if submission.sha != "0000000000000000000000000000000000000000" {
        submission.sha.clone()
    } else {
        submission
            .payload
            .get("after")
            .and_then(|v| v.as_str())
            .or_else(|| {
                submission
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("head"))
                    .and_then(|h| h.get("sha"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("0000000000000000000000000000000000000000")
            .to_string()
    };

    // Use actor from submission first-class field, fallback to payload extraction.
    let actor = if submission.actor != "aksh-system" {
        submission.actor.clone()
    } else {
        submission
            .payload
            .get("pusher")
            .and_then(|p| p.get("name"))
            .and_then(|v| v.as_str())
            .or_else(|| {
                submission
                    .payload
                    .get("sender")
                    .and_then(|s| s.get("login"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("aksh-system")
            .to_string()
    };

    // Extract actor_id from payload if available.
    let actor_id = submission
        .payload
        .get("sender")
        .and_then(|s| s.get("id"))
        .and_then(|v| v.as_u64())
        .map(|id| id.to_string())
        .unwrap_or_default();

    // Extract repository_id and repository_owner_id from payload.
    let repository_id = submission
        .payload
        .get("repository")
        .and_then(|r| r.get("id"))
        .and_then(|v| v.as_u64())
        .map(|id| id.to_string())
        .unwrap_or_default();
    let repository_owner_id = submission
        .payload
        .get("repository")
        .and_then(|r| r.get("owner"))
        .and_then(|o| o.get("id"))
        .and_then(|v| v.as_u64())
        .map(|id| id.to_string())
        .unwrap_or_default();
    let repository_visibility = submission
        .payload
        .get("repository")
        .and_then(|repository| repository.get("visibility"))
        .and_then(|value| value.as_str())
        .unwrap_or("private")
        .to_owned();

    let workflow_name = parse_workflow(&submission.workflow_yaml)
        .ok()
        .and_then(|w| w.name)
        .unwrap_or_default();

    // Derive the workflow filename: explicit > parsed from YAML > "workflow.yml"
    let workflow_file = submission
        .workflow_file
        .clone()
        .unwrap_or_else(|| "workflow.yml".to_owned());

    let head_ref = submission
        .payload
        .get("pull_request")
        .and_then(|pr| pr.get("head"))
        .and_then(|h| h.get("ref"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let base_ref = submission
        .payload
        .get("pull_request")
        .and_then(|pr| pr.get("base"))
        .and_then(|b| b.get("ref"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let check_run_id = run.job_check_run_ids.get(&resolved_job_id).copied();
    let workflow_ref = format!(
        "{}/.github/workflows/{}@{}",
        submission.repository, workflow_file, submission.git_ref
    );

    let job_workflow_ref = oidc_context.job_workflow_ref.as_deref().map(|reference| {
        format_reusable_workflow_ref(&submission.repository, reference, &submission.git_ref)
    });
    let job_workflow_sha = job_workflow_ref
        .as_ref()
        .and_then(|reference| {
            reference
                .rsplit_once('@')
                .map(|(_, git_ref)| git_ref)
                .filter(|git_ref| {
                    git_ref.len() == 40
                        && git_ref
                            .chars()
                            .all(|character| character.is_ascii_hexdigit())
                })
                .map(str::to_owned)
        })
        .or_else(|| job_workflow_ref.as_ref().map(|_| sha.clone()));

    let claims_input = oidc::OidcClaimsInput {
        repository: submission.repository.clone(),
        repository_owner,
        git_ref: submission.git_ref.clone(),
        event_name: submission.event.clone(),
        sha: sha.clone(),
        actor,
        actor_id,
        workflow: workflow_name,
        run_id: run_id.to_string(),
        run_number: "1".to_string(),
        run_attempt: "1".to_string(),
        head_ref,
        base_ref,
        environment: oidc_context.environment,
        repository_visibility,
        repository_id,
        repository_owner_id,
        workflow_ref: Some(workflow_ref),
        workflow_sha: Some(sha),
        job_workflow_ref,
        job_workflow_sha,
    };

    let audience = query
        .audience
        .unwrap_or_else(|| oidc::default_audience(&claims_input.repository_owner));

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ApiError::bad_request(format!("system clock before epoch: {error}")))?
        .as_secs();

    let issuer = oidc_issuer_url(&inner);
    drop(inner);
    let mut claims = oidc::build_claims(&claims_input, &audience, &issuer, now);
    if let Some(check_run_id) = check_run_id {
        claims["check_run_id"] = json!(check_run_id.to_string());
    }

    let jwt = oidc_kp
        .sign_jwt(&claims)
        .map_err(|e| ApiError::internal(format!("OIDC token signing failed: {e}")))?;

    Ok(Json(OidcTokenResponse { value: jwt }))
}

/// `GET /.well-known/openid-configuration` — OIDC discovery document.
pub(crate) async fn oidc_discovery(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let issuer = oidc_issuer_url(&inner);
    let jwks_uri = format!("{issuer}/.well-known/jwks");
    Ok(Json(oidc::discovery_document(&issuer, &jwks_uri)))
}

/// `GET /.well-known/jwks` — JSON Web Key Set for OIDC token verification.
pub(crate) async fn oidc_jwks(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let kp = inner
        .oidc_keypair
        .as_ref()
        .ok_or_else(|| ApiError::internal("OIDC keypair not available"))?;
    Ok(Json(kp.jwks()))
}

pub(crate) fn base64_url_json(value: &serde_json::Value) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| ApiError::bad_request(format!("failed to encode jwt json: {error}")))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
