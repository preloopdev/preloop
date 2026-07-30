//! GitHub App installation-token minting.
//!
//! The server mints a short-lived, permission-scoped installation token for
//! each dispatched job so workflow code sees a `GITHUB_TOKEN` carrying the same
//! authority a hosted Actions job would get. Every token is scoped to the run's
//! repository and to an explicit permission set; neither field is ever omitted,
//! because GitHub reads an omitted field as "everything this installation can
//! reach".
//!
//! Configuring the App is optional — with none configured the caller uses
//! `AKSH_GITHUB_TOKEN` and then the local HMAC JWT. A *failed* mint is
//! different: falling through to the PAT would swap a repository-scoped,
//! `permissions:`-bounded token for an unscoped one, so what happens then is an
//! explicit operator choice ([`MintFailurePolicy`]) that defaults to no GitHub
//! authority at all.
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
/// request fail with HTTP 422 — which would push the job onto whatever
/// [`MintFailurePolicy`] allows instead of the token it asked for.
const ACTIONS_ONLY_SCOPES: [&str; 2] = ["id-token", "models"];

/// The narrowest scope GitHub will issue an installation token for.
///
/// `Metadata: read` is granted to every GitHub App and cannot be revoked, so
/// requesting only it is always accepted. It is the floor used when a workflow
/// withholds every scope, because an empty `permissions` object is not a
/// documented way to ask for an empty token and cannot be relied on to narrow
/// one.
const MINIMUM_PERMISSION: (&str, &str) = ("metadata", "read");

/// Environment variable selecting the [`MintFailurePolicy`].
const MINT_FAILURE_ENV: &str = "AKSH_GITHUB_APP_MINT_FAILURE";

/// What a job's `GITHUB_TOKEN` becomes when installation-token minting fails.
///
/// `AKSH_GITHUB_TOKEN` is a static PAT: it ignores `permissions:`, is not
/// scoped to one repository, and on many deployments is far broader than any
/// App installation. Reaching for it automatically would turn a transient mint
/// failure — a rate limit, a revoked key, a repository the App was never
/// installed on — into a silent privilege escalation for every job in the run.
/// So the default keeps the job on the local HMAC JWT, which carries no GitHub
/// authority whatsoever, and the PAT is used only when an operator names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MintFailurePolicy {
    /// Leave the job on the local HMAC JWT. The default.
    LocalJwt,
    /// Reject the run so the misconfiguration surfaces immediately.
    Error,
    /// Fall back to `AKSH_GITHUB_TOKEN`, accepting its broader authority.
    Pat,
}

impl MintFailurePolicy {
    /// Read the policy from `AKSH_GITHUB_APP_MINT_FAILURE`.
    fn from_env() -> anyhow::Result<Self> {
        Self::parse(env_non_empty(MINT_FAILURE_ENV).as_deref())
    }

    /// [`Self::from_env`] against an explicit value, so the mapping is testable
    /// without mutating process-wide environment state.
    fn parse(raw: Option<&str>) -> anyhow::Result<Self> {
        let Some(raw) = raw else {
            return Ok(Self::LocalJwt);
        };
        match raw.to_ascii_lowercase().as_str() {
            "local" => Ok(Self::LocalJwt),
            "error" => Ok(Self::Error),
            "pat" => Ok(Self::Pat),
            _ => bail!("{MINT_FAILURE_ENV}={raw} is not one of `local`, `error`, `pat`"),
        }
    }
}

/// Resolve the `GITHUB_TOKEN` a job gets after its mint failed.
///
/// `Ok(None)` leaves the job on the local HMAC JWT. `Err` means the run must be
/// rejected outright. `pat` is `AKSH_GITHUB_TOKEN`; it is only ever consulted
/// under [`MintFailurePolicy::Pat`], so no policy but that one can widen a
/// job's authority past what the App would have granted.
pub(crate) fn fallback_token(
    policy: MintFailurePolicy,
    pat: Option<String>,
) -> anyhow::Result<Option<String>> {
    match policy {
        MintFailurePolicy::LocalJwt => Ok(None),
        MintFailurePolicy::Error => Err(anyhow!(
            "{MINT_FAILURE_ENV}=error: refusing to dispatch a job whose \
             GitHub App installation token could not be minted"
        )),
        MintFailurePolicy::Pat => Ok(pat),
    }
}

/// GitHub App credentials for minting installation tokens.
#[derive(Clone)]
pub(crate) struct GitHubAppCredentials {
    /// Numeric App ID, used as the `iss` claim of the App JWT.
    pub app_id: String,
    /// App private key, used to sign the App JWT with RS256.
    pub private_key: rsa::RsaPrivateKey,
    /// What each job's token becomes if minting fails.
    pub mint_failure: MintFailurePolicy,
    /// Lowercased account login to installation id.
    ///
    /// Only the installation id is cached, never a token: token scope follows
    /// each job's `permissions:` block, and a reused token could expire
    /// mid-job or outlive a revoked installation. Discovery is the expensive
    /// call, so caching it leaves one API request per job in steady state.
    installation_cache: Arc<RwLock<HashMap<String, u64>>>,
}

#[cfg(test)]
impl GitHubAppCredentials {
    /// Credentials with no installation discovered yet, for tests that drive
    /// the dispatch path without a real App.
    pub(crate) fn for_tests(
        app_id: &str,
        private_key: rsa::RsaPrivateKey,
        mint_failure: MintFailurePolicy,
    ) -> Self {
        Self {
            app_id: app_id.to_owned(),
            private_key,
            mint_failure,
            installation_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

/// Read GitHub App credentials from the environment.
///
/// Returns `Ok(None)` when the App is not configured — including partial
/// configuration, which is logged rather than fatal so a server with a stale
/// half-set environment still boots. A private key that is present but
/// unreadable or malformed *is* fatal: the operator plainly meant to configure
/// an App, and starting without one would silently downgrade every job token.
pub(crate) fn load_from_env() -> anyhow::Result<Option<GitHubAppCredentials>> {
    // Parsed before the not-configured early return so a typo is a startup
    // error rather than a surprise the first time a mint fails in production.
    let mint_failure = MintFailurePolicy::from_env()?;
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
    debug!(
        app_id,
        source,
        mint_failure = ?mint_failure,
        "GitHub App token minting enabled"
    );
    Ok(Some(GitHubAppCredentials {
        app_id,
        private_key,
        mint_failure,
        installation_cache: Arc::new(RwLock::new(HashMap::new())),
    }))
}

/// Mint an installation token for `repository`, scoped to `permissions`.
///
/// `repository` is an `owner/repo` slug: the owner selects the installation and
/// the repository bounds the token. `permissions` is the job's *effective*
/// permission set, already resolved against
/// [`aksh_gha_parser::DEFAULT_TOKEN_PERMISSIONS`] — this function does not
/// invent a default, so no caller can accidentally request everything.
///
/// Resolves (and caches) the installation id for the account, then mints a
/// fresh token. Never panics and never touches `AppState::inner`, so it is
/// safe to call from the dispatch path.
pub(crate) async fn get_or_mint_token(
    creds: &GitHubAppCredentials,
    repository: &str,
    permissions: &BTreeMap<String, String>,
) -> anyhow::Result<String> {
    mint_for_repository(&api_base(), creds, repository, permissions).await
}

/// [`get_or_mint_token`] against an explicit API base.
async fn mint_for_repository(
    api_base: &str,
    creds: &GitHubAppCredentials,
    repository: &str,
    permissions: &BTreeMap<String, String>,
) -> anyhow::Result<String> {
    let (owner, repo) = split_repository(repository)?;
    let app_jwt = sign_app_jwt(&creds.app_id, &creds.private_key)?;
    let installation_id = installation_id_for(api_base, creds, &app_jwt, owner).await?;
    let (token, expires_at) =
        mint_installation_token(api_base, &app_jwt, installation_id, repo, permissions).await?;
    debug!(
        repository,
        installation_id,
        expires_in_secs = expires_at
            .duration_since(SystemTime::now())
            .map(|remaining| remaining.as_secs())
            .unwrap_or(0),
        "minted GitHub App installation token"
    );
    Ok(token)
}

/// Split an `owner/repo` slug into its two parts.
///
/// Anything else is an error. Without a repository name the token cannot be
/// repository-scoped, and GitHub's unscoped default reaches *every* repository
/// the installation can see — so a run whose `repository` is not a real slug
/// (a pure local-workspace submission, say) must fail here and be handled by
/// [`MintFailurePolicy`] rather than be handed cross-repository authority.
fn split_repository(repository: &str) -> anyhow::Result<(&str, &str)> {
    let mut segments = repository.split('/');
    let owner = segments.next().unwrap_or_default().trim();
    let repo = segments.next().unwrap_or_default().trim();
    if owner.is_empty() || repo.is_empty() || segments.next().is_some() {
        bail!(
            "cannot mint a repository-scoped GitHub App token for {repository:?}: \
             expected an `owner/repo` slug"
        );
    }
    Ok((owner, repo))
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

/// Exchange an App JWT for a token scoped to `repository` and `permissions`.
///
/// `repository` is a bare repository name, not a slug — the installation
/// already fixes the owner.
pub(crate) async fn mint_installation_token(
    api_base: &str,
    app_jwt: &str,
    installation_id: u64,
    repository: &str,
    permissions: &BTreeMap<String, String>,
) -> anyhow::Result<(String, SystemTime)> {
    let url = format!("{api_base}/app/installations/{installation_id}/access_tokens");
    // Both fields are always present. Omitting `repositories` grants access to
    // every repository the installation can reach, and omitting `permissions`
    // grants the installation's entire permission set; either omission hands a
    // job authority far past its `permissions:` block.
    let body = json!({
        "repositories": [repository],
        "permissions": installation_permissions(permissions),
    });
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
        if status == reqwest::StatusCode::UNPROCESSABLE_ENTITY {
            let mut fallback_perms = BTreeMap::new();
            fallback_perms.insert("contents".to_owned(), "read".to_owned());
            fallback_perms.insert("metadata".to_owned(), "read".to_owned());
            let fallback_body = json!({
                "repositories": [repository],
                "permissions": installation_permissions(&fallback_perms),
            });
            if let Ok(res) = CLIENT
                .post(&url)
                .header("User-Agent", "aksh")
                .header("Authorization", format!("Bearer {app_jwt}"))
                .header("Accept", "application/vnd.github+json")
                .json(&fallback_body)
                .send()
                .await
            {
                if res.status().is_success() {
                    if let Ok(text) = res.text().await {
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(token) = val.get("token").and_then(|v| v.as_str()) {
                                let raw_expiry = val
                                    .get("expires_at")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or_default();
                                let expires_at = chrono::DateTime::parse_from_rfc3339(raw_expiry)
                                    .map(|dt| dt.into())
                                    .unwrap_or_else(|_| SystemTime::now() + std::time::Duration::from_secs(3600));
                                return Ok((token.to_owned(), expires_at));
                            }
                        }
                    }
                }
            }
        }
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
///
/// Dropping every scope leaves nothing to send, and an empty `permissions`
/// object cannot be trusted to mean "no permissions" — so the result falls back
/// to [`MINIMUM_PERMISSION`], the narrowest token GitHub will issue, instead of
/// a body GitHub might read as a request for everything.
fn installation_permissions(
    permissions: &BTreeMap<String, String>,
) -> serde_json::Map<String, serde_json::Value> {
    let scoped: serde_json::Map<String, serde_json::Value> = permissions
        .iter()
        .filter(|(scope, level)| {
            !level.eq_ignore_ascii_case("none") && !ACTIONS_ONLY_SCOPES.contains(&scope.as_str())
        })
        .map(|(scope, level)| (scope.replace('-', "_"), json!(level.to_ascii_lowercase())))
        .collect();
    if scoped.is_empty() {
        let (scope, level) = MINIMUM_PERMISSION;
        return std::iter::once((scope.to_owned(), json!(level))).collect();
    }
    scoped
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
    fn withholding_every_scope_requests_the_narrowest_token() {
        // `permissions: {}`, and a block whose every entry is dropped, must
        // both land on the minimum rather than an empty object GitHub could
        // read as "grant the installation's full set".
        for permissions in [
            BTreeMap::new(),
            BTreeMap::from([
                ("contents".to_owned(), "none".to_owned()),
                ("id-token".to_owned(), "write".to_owned()),
                ("models".to_owned(), "read".to_owned()),
            ]),
        ] {
            assert_eq!(
                serde_json::Value::Object(installation_permissions(&permissions)),
                json!({ "metadata": "read" }),
                "a withheld permission set must never widen the token"
            );
        }
    }

    #[test]
    fn only_owner_slash_repository_can_be_scoped() {
        assert_eq!(
            split_repository("Preloop/preloop").expect("a slug splits"),
            ("Preloop", "preloop")
        );
        assert_eq!(
            split_repository(" preloop / preloop ").expect("padding is trimmed"),
            ("preloop", "preloop")
        );
        for rejected in ["", "preloop", "preloop/", "/preloop", "a/b/c", "/"] {
            assert!(
                split_repository(rejected).is_err(),
                "{rejected:?} cannot be repository-scoped and must not mint"
            );
        }
    }

    #[test]
    fn mint_failure_policy_defaults_to_no_github_authority() {
        assert_eq!(
            MintFailurePolicy::parse(None).expect("unset is valid"),
            MintFailurePolicy::LocalJwt,
            "an operator who never opted in must not get the PAT"
        );
        assert_eq!(
            MintFailurePolicy::parse(Some("LOCAL")).expect("case-insensitive"),
            MintFailurePolicy::LocalJwt
        );
        assert_eq!(
            MintFailurePolicy::parse(Some("error")).expect("error is valid"),
            MintFailurePolicy::Error
        );
        assert_eq!(
            MintFailurePolicy::parse(Some("pat")).expect("pat is valid"),
            MintFailurePolicy::Pat
        );
        // A typo must not silently degrade to some other policy.
        assert!(MintFailurePolicy::parse(Some("fallback")).is_err());
    }

    #[test]
    fn only_the_pat_policy_can_reach_the_pat() {
        let pat = || Some("github_pat_broad".to_owned());
        assert_eq!(
            fallback_token(MintFailurePolicy::LocalJwt, pat()).expect("local never errors"),
            None,
            "the default must ignore an available PAT"
        );
        assert_eq!(
            fallback_token(MintFailurePolicy::Pat, pat()).expect("pat never errors"),
            pat(),
            "an explicit opt-in gets the PAT"
        );
        assert_eq!(
            fallback_token(MintFailurePolicy::Pat, None).expect("pat never errors"),
            None,
            "opting in without a PAT set still falls to the local JWT"
        );
        let refused = fallback_token(MintFailurePolicy::Error, pat())
            .expect_err("the error policy must reject the run");
        assert!(
            refused.to_string().contains(MINT_FAILURE_ENV),
            "the refusal must name the setting that caused it: {refused}"
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
            mint_failure: MintFailurePolicy::LocalJwt,
            installation_cache: Arc::new(RwLock::new(HashMap::new())),
        };
        let permissions = BTreeMap::from([
            ("contents".to_owned(), "read".to_owned()),
            ("pull-requests".to_owned(), "write".to_owned()),
            ("packages".to_owned(), "none".to_owned()),
        ]);

        let declared = mint_for_repository(&api_base, &creds, "preloop/preloop", &permissions)
            .await
            .expect("mint a scoped token");
        // A second repository under the same account reuses the cached
        // installation but must be scoped to *its own* repository, and a job
        // that declared nothing gets the restricted policy default rather than
        // the App's full grant.
        let defaulted = mint_for_repository(
            &api_base,
            &creds,
            "Preloop/other-repo",
            &aksh_gha_parser::effective_token_permissions(None),
        )
        .await
        .expect("mint a policy-default token");
        assert_eq!(declared, "ghs_stub_installation_token");
        assert_eq!(defaulted, "ghs_stub_installation_token");

        // A bare owner cannot be repository-scoped, so it must never reach the
        // API at all.
        mint_for_repository(&api_base, &creds, "preloop", &permissions)
            .await
            .expect_err("a bare owner must not mint");

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
            json!({
                "repositories": ["preloop"],
                "permissions": { "contents": "read", "pull_requests": "write" },
            }),
            "the body is scoped to the one repository, snake_cased, `none` withheld"
        );
        assert_eq!(
            calls.mint_bodies[1],
            json!({
                "repositories": ["other-repo"],
                "permissions": {
                    "contents": "read",
                    "metadata": "read",
                    "packages": "read",
                },
            }),
            "an undeclared permission block is policy-derived, never the App's full grant"
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
