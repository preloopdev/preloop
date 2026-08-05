use super::*;

/// GitHub-compatible runner registration token endpoint.
/// The official `actions/runner` config.sh calls this to get a registration token.
/// Matches the ChristopherHX/runner.server format: `GitHubAuthResult` with
/// `token`, `token_schema`, and `tenant_url`.
pub(crate) async fn github_registration_token(
    State(shared): State<Arc<SharedState>>,
    request: axum::http::Request<axum::body::Body>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // The runner sends `Authorization: RemoteAuth <token>` (the official
    // runner does the same against GitHub, where the token is one GitHub
    // issued). GitHub validates because it issued the token; this control
    // plane cannot validate third-party credentials, so any non-empty one is
    // accepted — that is what keeps the official runner and the conformance
    // replays working (the golden sends a real GitHub registration token).
    //
    // The mounted control socket is different: workflow code inside a runner
    // VM can reach it, and accepting any credential there would let a
    // malicious step mint a RunnerManage JWT and register a rogue runner.
    // The pool injects the system credential into its own configure
    // invocation and nothing in a job's environment carries it, so the
    // socket requires it.
    let auth = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let provided = auth
        .strip_prefix("RemoteAuth ")
        .or_else(|| auth.strip_prefix("Bearer "));
    let on_socket = request
        .extensions()
        .get::<crate::auth::SocketSurface>()
        .is_some();
    let missing = provided.is_none_or(|token| token.is_empty());
    if missing || (on_socket && provided != Some(shared.state.system_token.as_str())) {
        return Err(ApiError::unauthorized("invalid registration credential"));
    }

    let bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid registration body: {error}")))?;
    let payload: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| ApiError::bad_request(format!("invalid registration body: {error}")))?;

    let token = shared.state.local_jwt(json!({
        "sub": "aksh-runner-registration",
        "scp": "ActionsRuntime.RunnerManage Framework.GenericRead Identity.ReadRefs LocationService.Connect",
        "jti": uuid::Uuid::new_v4().to_string()
    }))?;
    let _requested_url = payload
        .get("url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://127.0.0.1")
        .to_owned();
    Ok(Json(json!({
        "token": token,
        "token_schema": "OAuthAccessToken",
        "url": runner_server_url()
    })))
}

#[derive(Serialize)]
pub(crate) struct TokenResponse {
    pub(crate) access_token: String,
    pub(crate) token_type: String,
    pub(crate) expires_in: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FormOAuth2Request {
    // serde: accepted from the runner's form payload but not inspected.
    #[allow(dead_code)]
    pub(crate) client_assertion_type: Option<String>,
    pub(crate) client_assertion: Option<String>,
    // serde: accepted from the runner's form payload but not inspected.
    #[allow(dead_code)]
    pub(crate) grant_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct JsonOAuth2Request {
    // serde: accepted from the runner's JSON payload but not inspected.
    #[allow(dead_code)]
    pub(crate) grant_type: String,
    pub(crate) client_id: String,
    // serde: accepted from the runner's JSON payload but not inspected.
    #[allow(dead_code)]
    pub(crate) client_secret: String,
}

pub(crate) fn decode_jwt_segment(segment: &str) -> Option<serde_json::Value> {
    let bytes = BASE64_STANDARD
        .decode(segment.as_bytes())
        .or_else(|_| URL_SAFE_NO_PAD.decode(segment.as_bytes()))
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Token TTL in seconds. Override with AKSH_TOKEN_TTL_SECS for testing
/// short-lived tokens (e.g. =1 triggers RLIS-02 proactive refresh immediately).
pub(crate) fn token_ttl_secs() -> u64 {
    std::env::var("AKSH_TOKEN_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2999)
}

pub(crate) async fn oauth2_token(
    State(shared): State<Arc<SharedState>>,
    _headers: axum::http::HeaderMap,
    body: bytes::Bytes,
) -> Result<Json<TokenResponse>, ApiError> {
    // Try JSON first (mock flow from existing tests)
    if let Ok(req) = serde_json::from_slice::<JsonOAuth2Request>(&body) {
        let token = shared.state.local_jwt(json!({
            "sub": format!("aksh-runner-listen-mock-{}", req.client_id),
            "scp": "ActionsRuntime.RunnerListen Framework.GenericRead Identity.ReadRefs LocationService.Connect",
            "jti": uuid::Uuid::new_v4().to_string()
        }))?;
        return Ok(Json(TokenResponse {
            access_token: token,
            token_type: "JWT".to_owned(),
            expires_in: token_ttl_secs(),
        }));
    }

    // Try urlencoded form (production runner flow with client assertion)
    let form: FormOAuth2Request = serde_urlencoded::from_bytes(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid urlencoded OAuth body: {e}")))?;

    let assertion = form
        .client_assertion
        .ok_or_else(|| ApiError::bad_request("missing client_assertion in OAuth request"))?;

    // Parse the client_assertion JWT (header.payload.signature)
    let parts: Vec<&str> = assertion.split('.').collect();
    if parts.len() != 3 {
        return Err(ApiError::bad_request(
            "invalid JWT format in client_assertion",
        ));
    }

    let header_val = decode_jwt_segment(parts[0])
        .ok_or_else(|| ApiError::bad_request("failed to decode JWT header"))?;
    let _claims_val = decode_jwt_segment(parts[1])
        .ok_or_else(|| ApiError::bad_request("failed to decode JWT claims"))?;

    let client_id = _claims_val
        .get("sub")
        .or_else(|| _claims_val.get("iss"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::bad_request("client_assertion claims missing sub/iss"))?;

    let signature = URL_SAFE_NO_PAD
        .decode(parts[2].as_bytes())
        .map_err(|e| ApiError::bad_request(format!("invalid JWT signature encoding: {e}")))?;

    let signing_input = format!("{}.{}", parts[0], parts[1]);

    // Look up the runner and its public key
    let (runner_id, pubkey) = {
        let inner = shared.state.inner.lock().await;
        let id = inner
            .runner_client_ids
            .get(client_id)
            .copied()
            .ok_or_else(|| {
                ApiError::unauthorized(format!("client ID not registered: {client_id}"))
            })?;
        let pubkey = inner
            .runner_rsa_public_keys
            .get(&id)
            .cloned()
            .ok_or_else(|| {
                ApiError::unauthorized(format!("runner {id} missing registered public key"))
            })?;
        (id, pubkey)
    };

    // Verify signature
    let alg = header_val
        .get("alg")
        .and_then(|v| v.as_str())
        .unwrap_or("RS256");
    match alg {
        "RS256" => {
            pubkey
                .verify_signature_rs256(signing_input.as_bytes(), &signature)
                .map_err(|e| {
                    ApiError::unauthorized(format!(
                        "JWT signature verification failed (RS256): {e}"
                    ))
                })?;
        }
        "PS256" => {
            pubkey
                .verify_signature_ps256(signing_input.as_bytes(), &signature)
                .map_err(|e| {
                    ApiError::unauthorized(format!(
                        "JWT signature verification failed (PS256): {e}"
                    ))
                })?;
        }
        other => {
            return Err(ApiError::unauthorized(format!(
                "unsupported JWT algorithm: {other}"
            )));
        }
    }

    let token = shared.state.local_jwt(json!({
        "sub": format!("aksh-runner-listen-{runner_id}"),
        "scp": "ActionsRuntime.RunnerListen Framework.GenericRead Identity.ReadRefs LocationService.Connect",
        "jti": uuid::Uuid::new_v4().to_string()
    }))?;

    Ok(Json(TokenResponse {
        access_token: token,
        token_type: "JWT".to_owned(),
        expires_in: token_ttl_secs(),
    }))
}
