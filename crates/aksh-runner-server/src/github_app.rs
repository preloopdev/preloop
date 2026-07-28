//! GitHub App installation-token minting.
//!
//! The server mints a short-lived, permission-scoped installation token for
//! each dispatched job so workflow code sees a `GITHUB_TOKEN` carrying the same
//! authority a hosted Actions job would get. Every part of this is optional:
//! with no App configured the caller falls back to a PAT and then to the local
//! HMAC JWT.
//!
//! Only the runner's `GITHUB_TOKEN` is affected. The `AccessToken` in
//! `endpoint.authorization.parameters` stays the local HMAC JWT, because that
//! credential authenticates the runner to *this* server, not to GitHub.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde_json::json;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::shared_http::CLIENT;

/// Private-key environment variables, highest precedence first. The flag marks
/// a variable that holds a path to a PEM file rather than the PEM itself.
///
/// Two naming pairs are accepted: `AKSH_GITHUB_APP_PEM{,_FILE}` and the older
/// `AKSH_GITHUB_APP_PRIVATE_KEY{,_PATH}` already published in
/// `docs/github-app-webhook.md`.
const PRIVATE_KEY_ENV: [(&str, bool); 4] = [
    ("AKSH_GITHUB_APP_PEM", false),
    ("AKSH_GITHUB_APP_PEM_FILE", true),
    ("AKSH_GITHUB_APP_PRIVATE_KEY", false),
    ("AKSH_GITHUB_APP_PRIVATE_KEY_PATH", true),
];

/// Upper bound on `/app/installations` pages walked during discovery, so a
/// paging bug cannot spin forever on an App installed across many accounts.
const MAX_INSTALLATION_PAGES: u32 = 10;

/// Page size for installation discovery; 100 is GitHub's maximum.
const INSTALLATIONS_PER_PAGE: usize = 100;

/// Workflow permission scopes with no GitHub App counterpart. `id-token`
/// controls Actions OIDC issuance and `models` controls GitHub Models access;
/// both are runtime-only, and forwarding either makes the installation-token
/// request fail with HTTP 422 — which would silently downgrade the job to a
/// broader fallback credential.
const ACTIONS_ONLY_SCOPES: [&str; 2] = ["id-token", "models"];

/// GitHub App credentials for minting installation tokens.
#[derive(Clone)]
pub(crate) struct GitHubAppCredentials {
    /// Numeric App ID, used as the `iss` claim of the App JWT.
    pub app_id: String,
    /// App private key, used to sign the App JWT with RS256.
    pub private_key: rsa::RsaPrivateKey,
    /// Lowercased account login to installation id.
    ///
    /// Only the installation id is cached, never a token: token scope follows
    /// each job's `permissions:` block, and a reused token could expire
    /// mid-job or outlive a revoked installation. Discovery is the expensive
    /// call, so caching it leaves one API request per job in steady state.
    installation_cache: Arc<RwLock<HashMap<String, u64>>>,
}

/// Read GitHub App credentials from the environment.
///
/// Returns `Ok(None)` when the App is not configured — including partial
/// configuration, which is logged rather than fatal so a server with a stale
/// half-set environment still boots. A private key that is present but
/// unreadable or malformed *is* fatal: the operator plainly meant to configure
/// an App, and starting without one would silently downgrade every job token.
pub(crate) fn load_from_env() -> anyhow::Result<Option<GitHubAppCredentials>> {
    let app_id = env_non_empty("AKSH_GITHUB_APP_ID");
    let key_source = PRIVATE_KEY_ENV
        .iter()
        .find_map(|&(name, is_path)| env_non_empty(name).map(|value| (name, is_path, value)));

    let (app_id, source, is_path, value) = match (app_id, key_source) {
        (Some(app_id), Some((source, is_path, value))) => (app_id, source, is_path, value),
        (None, None) => return Ok(None),
        (Some(_), None) => {
            warn!(
                "AKSH_GITHUB_APP_ID is set but no App private key is; \
                 GitHub App token minting is disabled"
            );
            return Ok(None);
        }
        (None, Some((source, ..))) => {
            warn!(
                "{source} is set but AKSH_GITHUB_APP_ID is not; \
                 GitHub App token minting is disabled"
            );
            return Ok(None);
        }
    };

    let pem = if is_path {
        std::fs::read_to_string(&value)
            .with_context(|| format!("reading the GitHub App private key from {source}={value}"))?
    } else {
        value
    };
    let private_key = parse_private_key(&pem)
        .with_context(|| format!("parsing the GitHub App private key from {source}"))?;
    debug!(app_id, source, "GitHub App token minting enabled");
    Ok(Some(GitHubAppCredentials {
        app_id,
        private_key,
        installation_cache: Arc::new(RwLock::new(HashMap::new())),
    }))
}

/// Mint an installation token for `owner`, scoped to `permissions`.
///
/// Resolves (and caches) the installation id for the account, then mints a
/// fresh token. Never panics and never touches `AppState::inner`, so it is
/// safe to call from the dispatch path.
pub(crate) async fn get_or_mint_token(
    creds: &GitHubAppCredentials,
    owner: &str,
    permissions: Option<&BTreeMap<String, String>>,
) -> anyhow::Result<String> {
    mint_for_owner(&api_base(), creds, owner, permissions).await
}

/// [`get_or_mint_token`] against an explicit API base.
async fn mint_for_owner(
    api_base: &str,
    creds: &GitHubAppCredentials,
    owner: &str,
    permissions: Option<&BTreeMap<String, String>>,
) -> anyhow::Result<String> {
    // Callers hold repository slugs far more often than bare logins, so accept
    // either `owner` or `owner/repo`.
    let owner = owner.split('/').next().unwrap_or(owner).trim();
    if owner.is_empty() {
        bail!("cannot mint a GitHub App token without a repository owner");
    }
    let app_jwt = sign_app_jwt(&creds.app_id, &creds.private_key)?;
    let installation_id = installation_id_for(api_base, creds, &app_jwt, owner).await?;
    let (token, expires_at) =
        mint_installation_token(api_base, &app_jwt, installation_id, permissions).await?;
    debug!(
        owner,
        installation_id,
        expires_in_secs = expires_at
            .duration_since(SystemTime::now())
            .map(|remaining| remaining.as_secs())
            .unwrap_or(0),
        "minted GitHub App installation token"
    );
    Ok(token)
}

/// Sign a JWT authenticating as the GitHub App itself (RS256).
///
/// The token is backdated 60s so a slightly fast local clock cannot land `iat`
/// in GitHub's future, and expires 10 minutes out — GitHub's maximum.
pub(crate) fn sign_app_jwt(app_id: &str, key: &rsa::RsaPrivateKey) -> anyhow::Result<String> {
    use rsa::pkcs1v15::SigningKey;
    use rsa::signature::{RandomizedSigner, SignatureEncoding};
    use sha2::Sha256;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the unix epoch")?
        .as_secs();
    let header = json!({ "alg": "RS256", "typ": "JWT" });
    let claims = json!({
        "iss": app_id,
        "iat": now.saturating_sub(60),
        "exp": now + 600,
    });
    let signing_input = format!("{}.{}", base64_json(&header)?, base64_json(&claims)?);
    let signature = SigningKey::<Sha256>::new(key.clone())
        .sign_with_rng(&mut rand::thread_rng(), signing_input.as_bytes());
    Ok(format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    ))
}

/// Resolve the id of this App's installation on `owner`'s account.
pub(crate) async fn find_installation(
    api_base: &str,
    app_jwt: &str,
    owner: &str,
) -> anyhow::Result<u64> {
    for page in 1..=MAX_INSTALLATION_PAGES {
        let url =
            format!("{api_base}/app/installations?per_page={INSTALLATIONS_PER_PAGE}&page={page}");
        let response = CLIENT
            .get(&url)
            .header("User-Agent", "aksh")
            .header("Authorization", format!("Bearer {app_jwt}"))
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("GET {url} failed with {status}: {body}");
        }
        let installations: Vec<serde_json::Value> = response
            .json()
            .await
            .with_context(|| format!("GET {url} returned an unexpected body"))?;
        let page_len = installations.len();
        for installation in installations {
            let login = installation
                .pointer("/account/login")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if login.eq_ignore_ascii_case(owner) {
                return installation
                    .get("id")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| anyhow!("the installation on {owner} has no numeric id"));
            }
        }
        if page_len < INSTALLATIONS_PER_PAGE {
            break;
        }
    }
    Err(anyhow!(
        "this GitHub App has no installation on {owner}; install it on that account"
    ))
}

/// Exchange an App JWT for an installation token scoped to `permissions`.
pub(crate) async fn mint_installation_token(
    api_base: &str,
    app_jwt: &str,
    installation_id: u64,
    permissions: Option<&BTreeMap<String, String>>,
) -> anyhow::Result<(String, SystemTime)> {
    let url = format!("{api_base}/app/installations/{installation_id}/access_tokens");
    let body = match permissions {
        // Omitting `permissions` grants the installation's full permission
        // set, which is what Actions does for a workflow that declares none.
        None => json!({}),
        Some(permissions) => json!({ "permissions": installation_permissions(permissions) }),
    };
    let response = CLIENT
        .post(&url)
        .header("User-Agent", "aksh")
        .header("Authorization", format!("Bearer {app_jwt}"))
        .header("Accept", "application/vnd.github+json")
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = response.status();
    // The success body carries the token, so it is parsed but never logged.
    let payload = response
        .text()
        .await
        .with_context(|| format!("POST {url} response body"))?;
    if !status.is_success() {
        bail!("POST {url} failed with {status}: {payload}");
    }
    let payload: serde_json::Value = serde_json::from_str(&payload)
        .with_context(|| format!("POST {url} returned a non-JSON body"))?;
    let token = payload
        .get("token")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("installation token response has no `token`"))?
        .to_owned();
    let raw_expiry = payload
        .get("expires_at")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("installation token response has no `expires_at`"))?;
    let expires_at = chrono::DateTime::parse_from_rfc3339(raw_expiry)
        .with_context(|| format!("unparsable installation token expiry {raw_expiry:?}"))?;
    Ok((token, SystemTime::from(expires_at)))
}

/// Installation id for `owner`, served from cache when already discovered.
async fn installation_id_for(
    api_base: &str,
    creds: &GitHubAppCredentials,
    app_jwt: &str,
    owner: &str,
) -> anyhow::Result<u64> {
    if let Some(installation_id) = installation_id_override()? {
        return Ok(installation_id);
    }
    let key = owner.to_ascii_lowercase();
    {
        let cache = creds.installation_cache.read().await;
        if let Some(installation_id) = cache.get(&key) {
            return Ok(*installation_id);
        }
    }
    let installation_id = find_installation(api_base, app_jwt, owner).await?;
    creds
        .installation_cache
        .write()
        .await
        .insert(key, installation_id);
    Ok(installation_id)
}

/// Explicit installation id from `AKSH_GITHUB_APP_INSTALLATION_ID`, which
/// bypasses discovery entirely for single-installation deployments.
fn installation_id_override() -> anyhow::Result<Option<u64>> {
    let Some(raw) = env_non_empty("AKSH_GITHUB_APP_INSTALLATION_ID") else {
        return Ok(None);
    };
    raw.parse()
        .map(Some)
        .with_context(|| format!("AKSH_GITHUB_APP_INSTALLATION_ID={raw} is not a number"))
}

/// Translate workflow `permissions:` scopes into installation-token scopes.
///
/// Workflow YAML uses kebab-case names and the value `none` to withhold a
/// scope; the installation-token API uses snake_case names, has no `none`
/// level, and rejects any key it does not recognise. Withheld and
/// Actions-only scopes are therefore dropped rather than forwarded.
fn installation_permissions(
    permissions: &BTreeMap<String, String>,
) -> serde_json::Map<String, serde_json::Value> {
    permissions
        .iter()
        .filter(|(scope, level)| {
            !level.eq_ignore_ascii_case("none") && !ACTIONS_ONLY_SCOPES.contains(&scope.as_str())
        })
        .map(|(scope, level)| (scope.replace('-', "_"), json!(level.to_ascii_lowercase())))
        .collect()
}

/// Parse a GitHub App private key in either PKCS#1 or PKCS#8 PEM form.
fn parse_private_key(pem: &str) -> anyhow::Result<rsa::RsaPrivateKey> {
    use rsa::pkcs1::DecodeRsaPrivateKey;
    use rsa::pkcs8::DecodePrivateKey;

    // Secret stores and container runtimes routinely flatten PEM newlines to
    // the two-character escape `\n`; restore them so an inline env var works.
    let unescaped;
    let pem = if pem.contains("\\n") && !pem.contains('\n') {
        unescaped = pem.replace("\\n", "\n");
        unescaped.as_str()
    } else {
        pem
    };
    let pem = pem.trim();
    // GitHub hands out PKCS#1; PKCS#8 shows up after an `openssl pkcs8`
    // conversion, which plenty of deployment guides recommend.
    if pem.contains("BEGIN RSA PRIVATE KEY") {
        rsa::RsaPrivateKey::from_pkcs1_pem(pem).context("invalid PKCS#1 RSA private key")
    } else if pem.contains("BEGIN PRIVATE KEY") {
        rsa::RsaPrivateKey::from_pkcs8_pem(pem).context("invalid PKCS#8 private key")
    } else {
        bail!("expected a PEM-encoded RSA private key (BEGIN RSA PRIVATE KEY or BEGIN PRIVATE KEY)")
    }
}

/// GitHub REST API base, overridable for GitHub Enterprise Server and tests.
fn api_base() -> String {
    env_non_empty("AKSH_GITHUB_API_URL")
        .map(|base| base.trim_end_matches('/').to_owned())
        .unwrap_or_else(|| "https://api.github.com".to_owned())
}

/// Read an environment variable, treating blank and whitespace-only as unset.
fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn base64_json(value: &serde_json::Value) -> anyhow::Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(value)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> rsa::RsaPrivateKey {
        rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).expect("generate test RSA key")
    }

    #[test]
    fn app_jwt_is_rs256_over_backdated_app_claims() {
        use rsa::pkcs1v15::VerifyingKey;
        use rsa::signature::Verifier;
        use sha2::Sha256;

        let key = test_key();
        let jwt = sign_app_jwt("123456", &key).expect("sign App JWT");
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT must be header.claims.signature");

        let decode = |part: &str| -> serde_json::Value {
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(part).expect("base64url")).expect("json")
        };
        let header = decode(parts[0]);
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["typ"], "JWT");

        let claims = decode(parts[1]);
        assert_eq!(claims["iss"], "123456", "iss must be the App ID");
        let iat = claims["iat"].as_u64().expect("iat");
        let exp = claims["exp"].as_u64().expect("exp");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs();
        assert!(iat < now, "iat must be backdated for clock skew");
        assert!(exp > now, "exp must be in the future");
        assert_eq!(exp - iat, 660, "60s backdate plus a 10 minute lifetime");

        let signature = rsa::pkcs1v15::Signature::try_from(
            URL_SAFE_NO_PAD
                .decode(parts[2])
                .expect("base64url signature")
                .as_slice(),
        )
        .expect("signature bytes");
        VerifyingKey::<Sha256>::new(key.to_public_key())
            .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
            .expect("RS256 signature must verify against the App public key");
    }

    #[test]
    fn workflow_permissions_become_installation_scopes() {
        let permissions = BTreeMap::from([
            ("contents".to_owned(), "read".to_owned()),
            ("pull-requests".to_owned(), "write".to_owned()),
            ("security-events".to_owned(), "write".to_owned()),
            // `none` withholds a scope; the API has no such level.
            ("packages".to_owned(), "none".to_owned()),
            // Actions-only scopes would make the whole request 422.
            ("id-token".to_owned(), "write".to_owned()),
        ]);
        assert_eq!(
            serde_json::Value::Object(installation_permissions(&permissions)),
            json!({
                "contents": "read",
                "pull_requests": "write",
                "security_events": "write",
            })
        );
    }

    #[test]
    fn private_key_accepts_pkcs1_pkcs8_and_escaped_newlines() {
        use rsa::pkcs1::EncodeRsaPrivateKey;
        use rsa::pkcs8::EncodePrivateKey;

        let key = test_key();
        let pkcs1 = key
            .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
            .expect("encode PKCS#1");
        let pkcs8 = key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .expect("encode PKCS#8");

        assert_eq!(parse_private_key(&pkcs1).expect("PKCS#1 parses"), key);
        assert_eq!(parse_private_key(&pkcs8).expect("PKCS#8 parses"), key);
        assert_eq!(
            parse_private_key(&pkcs1.replace('\n', "\\n")).expect("escaped newlines parse"),
            key,
            "env-flattened PEMs must round-trip"
        );
        assert!(parse_private_key("not a pem at all").is_err());
    }

    /// Records what the stub GitHub API was asked for.
    #[derive(Default)]
    struct StubCalls {
        discovery_pages: Vec<u32>,
        mint_installation_ids: Vec<u64>,
        mint_bodies: Vec<serde_json::Value>,
        mint_authorizations: Vec<String>,
    }

    #[tokio::test]
    async fn mints_a_scoped_token_per_job_and_caches_installation_discovery() {
        use axum::extract::{Path, Query};
        use axum::http::HeaderMap;
        use axum::routing::{get, post};
        use axum::{Json, Router};
        use std::sync::Mutex;

        let calls = Arc::new(Mutex::new(StubCalls::default()));
        let discovery_calls = Arc::clone(&calls);
        let mint_calls = Arc::clone(&calls);

        let stub = Router::new()
            .route(
                "/app/installations",
                get(move |Query(query): Query<HashMap<String, String>>| {
                    let calls = Arc::clone(&discovery_calls);
                    async move {
                        let page = query
                            .get("page")
                            .and_then(|page| page.parse::<u32>().ok())
                            .unwrap_or(1);
                        calls.lock().expect("stub state").discovery_pages.push(page);
                        // A brim-full first page is the only signal that more
                        // pages exist, so the match has to live on page two.
                        let installations: Vec<serde_json::Value> = match page {
                            1 => (0..INSTALLATIONS_PER_PAGE)
                                .map(|index| {
                                    json!({
                                        "id": 1000 + index,
                                        "account": { "login": format!("other-{index}") },
                                    })
                                })
                                .collect(),
                            2 => vec![json!({
                                "id": 424_242,
                                "account": { "login": "Preloop" },
                            })],
                            _ => Vec::new(),
                        };
                        Json(installations)
                    }
                }),
            )
            .route(
                "/app/installations/:installation_id/access_tokens",
                post(
                    move |Path(installation_id): Path<u64>,
                          headers: HeaderMap,
                          Json(body): Json<serde_json::Value>| {
                        let calls = Arc::clone(&mint_calls);
                        async move {
                            let mut calls = calls.lock().expect("stub state");
                            calls.mint_installation_ids.push(installation_id);
                            calls.mint_authorizations.push(
                                headers
                                    .get("authorization")
                                    .and_then(|value| value.to_str().ok())
                                    .unwrap_or_default()
                                    .to_owned(),
                            );
                            calls.mint_bodies.push(body);
                            Json(json!({
                                "token": "ghs_stub_installation_token",
                                "expires_at": "2999-01-01T00:00:00Z",
                            }))
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub API");
        let api_base = format!("http://{}", listener.local_addr().expect("stub address"));
        tokio::spawn(async move { axum::serve(listener, stub).await.expect("serve stub API") });

        let creds = GitHubAppCredentials {
            app_id: "424".to_owned(),
            private_key: test_key(),
            installation_cache: Arc::new(RwLock::new(HashMap::new())),
        };
        let permissions = BTreeMap::from([
            ("contents".to_owned(), "read".to_owned()),
            ("pull-requests".to_owned(), "write".to_owned()),
            ("packages".to_owned(), "none".to_owned()),
        ]);

        // A repository slug must resolve the same account as a bare login.
        let scoped = mint_for_owner(&api_base, &creds, "preloop/preloop", Some(&permissions))
            .await
            .expect("mint a scoped token");
        let unscoped = mint_for_owner(&api_base, &creds, "preloop", None)
            .await
            .expect("mint a default-scope token");
        assert_eq!(scoped, "ghs_stub_installation_token");
        assert_eq!(unscoped, "ghs_stub_installation_token");

        let calls = calls.lock().expect("stub state");
        assert_eq!(
            calls.discovery_pages,
            vec![1, 2],
            "discovery pages until the account matches, then serves the id from cache"
        );
        assert_eq!(
            calls.mint_installation_ids,
            vec![424_242, 424_242],
            "every job mints a fresh token against the discovered installation"
        );
        assert_eq!(
            calls.mint_bodies[0],
            json!({ "permissions": { "contents": "read", "pull_requests": "write" } }),
            "declared scopes are snake_cased and `none` is withheld"
        );
        assert_eq!(
            calls.mint_bodies[1],
            json!({}),
            "no declared permissions leaves the installation default in place"
        );
        for authorization in &calls.mint_authorizations {
            let jwt = authorization
                .strip_prefix("Bearer ")
                .expect("mint must present a bearer App JWT");
            let claims: serde_json::Value = serde_json::from_slice(
                &URL_SAFE_NO_PAD
                    .decode(jwt.split('.').nth(1).expect("JWT claims segment"))
                    .expect("base64url claims"),
            )
            .expect("JSON claims");
            assert_eq!(claims["iss"], "424", "must authenticate as the App itself");
        }
    }
}
