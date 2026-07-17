use super::*;

/// GitHub-compatible runner registration token endpoint.
/// The official `actions/runner` config.sh calls this to get a registration token.
/// Matches the ChristopherHX/runner.server format: `GitHubAuthResult` with
/// `token`, `token_schema`, and `tenant_url`.
pub(crate) async fn github_registration_token(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // The runner sends `Authorization: RemoteAuth <token>`
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !auth.starts_with("RemoteAuth ") && !auth.starts_with("Bearer ") {
        return Err(ApiError::unauthorized("missing Authorization header"));
    }

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

    let _header_val = decode_jwt_segment(parts[0])
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
    pubkey
        .verify_signature_ps256(signing_input.as_bytes(), &signature)
        .map_err(|e| ApiError::unauthorized(format!("JWT signature verification failed: {e}")))?;

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
