//! OAuth token acquisition via client-credentials JWT.
//!
//! The official runner sends a RS256-signed JWT as a client assertion to
//! obtain an OAuth access token. We reuse the runner's RSA keypair.

use anyhow::{Context, Result};
use tracing::info;

use crate::client::http::HttpClient;
use crate::settings::RunnerConfig;

/// Token response from the OAuth endpoint.
#[derive(Debug, serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    token_type: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

/// Obtain an OAuth access token using the runner's credentials.
pub async fn get_oauth_token(http: &HttpClient, config: &RunnerConfig) -> Result<String> {
    let auth_url = config
        .credentials
        .authorization_url()
        .context("no authorizationUrl in credentials.data")?;
    let client_id = config
        .credentials
        .client_id()
        .context("no clientId in credentials.data")?;

    // Build the JWT client assertion
    let jwt = build_client_assertion(client_id, auth_url, &config.rsa_params)?;

    // Exchange JWT for access token
    let resp: TokenResponse = http
        .post_form_json(
            auth_url,
            &[
                ("grant_type", "client_credentials"),
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", &jwt),
            ],
        )
        .await
        .context("OAuth token exchange")?;

    info!("OAuth token acquired (type: {})", resp.token_type);
    Ok(resp.access_token)
}

/// Build a PS256-signed JWT for client-credentials authentication.
fn build_client_assertion(
    client_id: &str,
    audience: &str,
    rsa_params: &crate::settings::RsaParameters,
) -> Result<String> {
    use aksh_gha_protocol::crypto::sign_jwt_ps256;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let header = serde_json::json!({
        "typ": "JWT",
        "alg": "PS256"
    });

    let claims = serde_json::json!({
        "sub": client_id,
        "iss": client_id,
        "aud": audience,
        "jti": uuid::Uuid::new_v4().to_string(),
        "nbf": now,
        "exp": now + 300,
    });

    sign_jwt_ps256(&header, &claims, rsa_params)
}
