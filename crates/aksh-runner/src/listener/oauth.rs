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
    // F053: Prefer authorizationUrlV2 when available (auth migration)
    let auth_url_v2 = config
        .credentials
        .data
        .get("authorizationUrlV2")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let enable_migration = config
        .credentials
        .data
        .get("enableAuthMigrationByDefault")
        .and_then(|v| v.as_str())
        .is_some_and(|s| s == "true" || s == "True");

    let base_auth_url = config
        .credentials
        .authorization_url()
        .context("no authorizationUrl in credentials.data")?;

    let auth_url = if enable_migration {
        auth_url_v2.unwrap_or(base_auth_url)
    } else {
        base_auth_url
    };

    // F053: oauthEndpointUrl fallback (for back-compat with older .credentials)
    let oauth_endpoint = config
        .credentials
        .data
        .get("oauthEndpointUrl")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(auth_url);

    let client_id = config
        .credentials
        .client_id()
        .context("no clientId in credentials.data")?;

    // Build the JWT client assertion (audience = authorizationUrl)
    let jwt = build_client_assertion(client_id, auth_url, &config.rsa_params)?;

    // Exchange JWT for access token (POST to oauthEndpointUrl)
    let resp: TokenResponse = http
        .post_form_json(
            oauth_endpoint,
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
