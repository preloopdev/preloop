//! GitHub-compatible dispatch authentication (D2).
//!
//! The dispatch endpoints (`POST /repos/{owner}/{repo}/actions/workflows/
//! {workflow_id}/dispatches`, `POST /repos/{owner}/{repo}/dispatches`, and the
//! read-only `/repos/.../actions/...` convenience routes) authenticate exactly
//! like github.com: a bearer token is mandatory, and its authority is proven
//! through one of five channels before the handler runs:
//!
//! 1. **System bearer** (`PRELOOP_SYSTEM_TOKEN`) — trusted operator.
//! 2. **PAT** (`PRELOOP_GITHUB_TOKEN` / config `github.pat`) — trusted
//!    operator; constant-time compare.
//! 3. **Own-App JWT** (RS256, `iss` = a registered App id) — verified against
//!    that App's PEM; offline-safe.
//! 4. **Installation tokens**:
//!    - minted by preloop itself: validated against the in-memory mint ledger
//!      (offline-safe, no round-trip);
//!    - third-party: validated with a github.com round-trip
//!      (`GET /installation`, `GET /installation/repositories`), cached with a
//!      short TTL, and failing **closed** on network errors.
//! 5. Anything else — 401.
//!
//! The middleware inserts a [`DispatchIdentity`] extension; handlers use it
//! for the `actions: write` and repository-access checks and for the
//! synthesized `sender` of dispatched runs.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap};
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;
use tracing::warn;

use crate::events::trust_tier::TrustTier;
use crate::github_app::{MintLedgerEntry, MintRejected};
use crate::state::SharedState;
use crate::ApiError;

/// How long validated installation-token facts stay cached. The token itself
/// lives an hour; 60s keeps github.com revocation effective within a minute
/// while bounding API pressure.
const TOKEN_CACHE_TTL: Duration = Duration::from_secs(60);

/// How long a PAT's `GET /user` login and an App JWT's slug stay cached.
const ACTOR_CACHE_TTL: Duration = Duration::from_secs(60);

/// Offline / unresolvable PAT actor. Distinct from the system-bearer identity
/// (`preloop-system`) so audit logs and `github.actor` checks never conflate a
/// PAT dispatch with native system auth when github.com cannot name the user.
const PRELOOP_PAT_ACTOR: &str = "preloop-pat";

/// The identity a dispatch request authenticated as (D2).
#[derive(Debug, Clone)]
pub(crate) struct DispatchIdentity {
    /// Resolved sender login for the synthesized `sender` / `github.actor`.
    pub(crate) actor: String,
    /// Trust tier stamped on dispatched runs. Installation-token dispatches
    /// get [`TrustTier::AppDispatch`]; everything else [`TrustTier::AdminManual`].
    pub(crate) tier: TrustTier,
    /// Which channel of the D2 chain authenticated the request.
    pub(crate) kind: DispatchAuthKind,
}

#[derive(Debug, Clone)]
pub(crate) enum DispatchAuthKind {
    /// Native `PRELOOP_SYSTEM_TOKEN`.
    SystemBearer,
    /// `PRELOOP_GITHUB_TOKEN` / config `github.pat`.
    Pat,
    /// A JWT signed by one of the registered Apps' keys.
    AppJwt { app_id: String },
    /// An installation token — preloop-minted (ledger) or third-party
    /// (github.com round-trip).
    InstallationToken(InstallationTokenAuth),
}

#[derive(Debug, Clone)]
pub(crate) struct InstallationTokenAuth {
    pub(crate) installation_id: u64,
    /// Numeric App id; `None` when github.com did not return one.
    pub(crate) app_id: Option<String>,
    /// The installation's account login.
    pub(crate) account_login: String,
    /// Permission scopes the token carries (snake_case keys, GitHub levels).
    pub(crate) permissions: BTreeMap<String, String>,
    /// Repos the token can access as `owner/repo` slugs. `None` means every
    /// repository (an "all repositories" installation); `Some(list)` means
    /// exactly those.
    pub(crate) accessible_repositories: Option<Vec<String>>,
}

impl DispatchIdentity {
    /// Whether the caller proved `actions: write` on the target repository.
    ///
    /// System bearer, PAT, and own-App JWT are operator credentials and hold
    /// every action; an installation token only holds what its installation
    /// granted it.
    pub(crate) fn has_actions_write(&self) -> bool {
        match &self.kind {
            DispatchAuthKind::InstallationToken(token) => {
                let level = token
                    .permissions
                    .get("actions")
                    .map(String::as_str)
                    .unwrap_or("none");
                matches!(level, "write" | "admin")
            }
            _ => true,
        }
    }

    /// Whether the caller proved `contents: write` on the target repository.
    ///
    /// System bearer, PAT, and own-App JWT are operator credentials and hold
    /// every action; an installation token only holds what its installation
    /// granted it.
    pub(crate) fn has_contents_write(&self) -> bool {
        match &self.kind {
            DispatchAuthKind::InstallationToken(token) => {
                let level = token
                    .permissions
                    .get("contents")
                    .map(String::as_str)
                    .unwrap_or("none");
                matches!(level, "write" | "admin")
            }
            _ => true,
        }
    }

    /// Whether an installation token may reach `owner/repo`. Operator
    /// credentials always may; an installation token only the repos it can
    /// access (all repositories when the installation selected "all").
    pub(crate) fn covers_repository(&self, owner: &str, repo: &str) -> bool {
        match &self.kind {
            DispatchAuthKind::InstallationToken(token) => match &token.accessible_repositories {
                None => true,
                Some(repositories) => repositories
                    .iter()
                    .any(|accessible| accessible.eq_ignore_ascii_case(&format!("{owner}/{repo}"))),
            },
            _ => true,
        }
    }
}

/// Auth middleware for the GitHub-compatible dispatch routes.
///
/// Mandatory (github.com returns 401 without a token). On success the
/// [`DispatchIdentity`] is inserted as a request extension for the handler.
pub(crate) async fn require_dispatch_auth(
    State(shared): State<Arc<SharedState>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let identity = authenticate(&shared, request.headers()).await?;
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

/// Run the D2 chain against the request's Authorization header.
async fn authenticate(
    shared: &Arc<SharedState>,
    headers: &HeaderMap,
) -> Result<DispatchIdentity, ApiError> {
    let bearer = dispatch_bearer(headers)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| ApiError::unauthorized("a bearer token is required"))?;

    // 1. System bearer — the operator's native credential.
    if constant_time_eq(bearer, &shared.state.system_token) {
        return Ok(DispatchIdentity {
            actor: "preloop-system".to_owned(),
            tier: TrustTier::AdminManual,
            kind: DispatchAuthKind::SystemBearer,
        });
    }

    // 2. PAT — the operator's static GitHub credential.
    if let Some(pat) = shared.state.static_github_pat() {
        if constant_time_eq(bearer, &pat) {
            let actor = resolve_pat_actor(shared, bearer).await;
            return Ok(DispatchIdentity {
                actor,
                tier: TrustTier::AdminManual,
                kind: DispatchAuthKind::Pat,
            });
        }
    }

    // 3. Own-App JWT — RS256, `iss` = one of the registered App ids.
    let mut verified_app_id: Option<String> = None;
    if let Some(registry) = &shared.state.github_apps {
        for app in &registry.apps {
            if verify_app_jwt(&app.app_id, bearer, &app.private_key.to_public_key()).is_ok() {
                verified_app_id = Some(app.app_id.clone());
                break;
            }
        }
    } else if let Some(app) = &shared.state.github_app {
        if verify_app_jwt(&app.app_id, bearer, &app.private_key.to_public_key()).is_ok() {
            verified_app_id = Some(app.app_id.clone());
        }
    }
    if let Some(app_id) = verified_app_id {
        let actor = resolve_app_actor(shared, &app_id).await;
        return Ok(DispatchIdentity {
            actor,
            tier: TrustTier::AdminManual,
            kind: DispatchAuthKind::AppJwt { app_id },
        });
    }
    // A JWT-shaped bearer that did not verify against a registered App is
    // never accepted: third-party App JWTs have no PEM to verify against
    // (D2.5), so reject locally instead of paying a network round-trip.
    if looks_like_app_jwt(bearer) {
        return Err(ApiError::unauthorized(
            "App JWTs must be issued by a registered preloop App",
        ));
    }

    // 4. Installation tokens.
    //
    // Fast path first: a token preloop itself minted is proven by the mint
    // ledger with no network traffic, which keeps dispatch working offline.
    // The token may have been minted by any registered App — not just the
    // legacy default — so the legacy ledger and every registry App's ledger
    // are consulted; the entry carries `app_id`, so actor resolution works
    // whichever ledger matched.
    let ledger_entry = shared
        .state
        .github_app
        .as_ref()
        .and_then(|app| app.mint_ledger.lookup(bearer))
        .or_else(|| {
            shared.state.github_apps.as_ref().and_then(|registry| {
                registry
                    .apps
                    .iter()
                    .find_map(|app| app.mint_ledger.lookup(bearer))
            })
        });
    if let Some(entry) = ledger_entry {
        // The bot identity is the App's slug, not the repository owner the
        // token was minted for.
        let actor = resolve_app_actor(shared, &entry.app_id).await;
        return Ok(DispatchIdentity {
            actor,
            tier: TrustTier::AppDispatch,
            kind: DispatchAuthKind::InstallationToken(installation_auth_from_ledger(entry)),
        });
    }

    // Online path: prove a third-party (or unledgered) token against
    // github.com. Fails closed — a network failure is never read as
    // "anonymous".
    let info = validate_installation_online(shared, bearer).await?;
    let bot = info
        .app_slug
        .as_deref()
        .or(Some(info.account_login.as_str()))
        .unwrap_or_default();
    Ok(DispatchIdentity {
        actor: format!("{bot}[bot]"),
        tier: TrustTier::AppDispatch,
        kind: DispatchAuthKind::InstallationToken(InstallationTokenAuth {
            installation_id: info.installation_id,
            app_id: info.app_id,
            account_login: info.account_login,
            permissions: info.permissions,
            accessible_repositories: info.accessible_repositories,
        }),
    })
}

/// Everything learned from a successful github.com installation round-trip.
#[derive(Debug, Clone)]
pub(crate) struct InstallationInfo {
    installation_id: u64,
    account_login: String,
    app_id: Option<String>,
    /// The App's slug (`{slug}[bot]` is its bot login). Absent on older
    /// responses; the account login is then used for the bot identity.
    app_slug: Option<String>,
    permissions: BTreeMap<String, String>,
    accessible_repositories: Option<Vec<String>>,
}

fn installation_auth_from_ledger(entry: MintLedgerEntry) -> InstallationTokenAuth {
    InstallationTokenAuth {
        installation_id: entry.installation_id,
        app_id: Some(entry.app_id),
        account_login: entry.account_login,
        permissions: entry.permissions,
        accessible_repositories: Some(vec![entry.repository]),
    }
}

/// Validate an installation token with a github.com round-trip
/// (`GET /installation`, `GET /installation/repositories`), consulting the
/// short-TTL cache first.
///
/// Fails **closed**: a transport failure (github.com unreachable) is a 502,
/// never a fall-through to an unauthenticated path; a refused token (401/403)
/// is a 401.
async fn validate_installation_online(
    shared: &Arc<SharedState>,
    token: &str,
) -> Result<InstallationInfo, ApiError> {
    let cache_key = sha256_hex(token);
    if let Some(info) = shared.state.dispatch_token_cache.get(&cache_key) {
        return Ok(info);
    }
    let api_base = crate::github::github_api_base();
    let info = match fetch_installation_info(&api_base, token).await {
        Ok(info) => info,
        Err(error) => {
            if let Some(rejected) = error.downcast_ref::<MintRejected>() {
                if rejected.status == reqwest::StatusCode::UNAUTHORIZED
                    || rejected.status == reqwest::StatusCode::FORBIDDEN
                {
                    return Err(ApiError::unauthorized(format!(
                        "github.com rejected the installation token ({}): {}",
                        rejected.status, rejected.message
                    )));
                }
            }
            warn!(
                ?error,
                "could not validate installation token against github.com — failing closed"
            );
            return Err(ApiError::bad_gateway(
                "unable to validate the installation token against github.com; \
                 only tokens preloop itself minted are accepted while github.com is unreachable",
            ));
        }
    };
    shared
        .state
        .dispatch_token_cache
        .put(cache_key, info.clone());
    Ok(info)
}

/// Fetch and assemble the installation facts for `token` from github.com.
async fn fetch_installation_info(api_base: &str, token: &str) -> anyhow::Result<InstallationInfo> {
    let installation = fetch_json(&format!("{api_base}/installation"), token)
        .await
        .with_context(|| "GET /installation failed")?;
    let installation_id = installation
        .get("id")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow::anyhow!("GET /installation has no numeric `id`"))?;
    let account_login = installation
        .pointer("/account/login")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let app_id = installation
        .get("app_id")
        .and_then(serde_json::Value::as_u64)
        .map(|id| id.to_string());
    let app_slug = installation
        .get("app_slug")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let permissions = installation
        .get("permissions")
        .and_then(serde_json::Value::as_object)
        .map(|permissions| {
            permissions
                .iter()
                .filter_map(|(name, level)| {
                    level.as_str().map(|level| (name.clone(), level.to_owned()))
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let repository_selection = installation
        .get("repository_selection")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("selected");
    // An "all repositories" installation can reach every repo; the list is
    // otherwise the source of truth for access (checked per dispatch target).
    let accessible_repositories = if repository_selection == "all" {
        None
    } else {
        Some(fetch_installation_repositories(api_base, token).await?)
    };
    Ok(InstallationInfo {
        installation_id,
        account_login,
        app_id,
        app_slug,
        permissions,
        accessible_repositories,
    })
}

/// Paginated `GET /installation/repositories` full-name list.
async fn fetch_installation_repositories(
    api_base: &str,
    token: &str,
) -> anyhow::Result<Vec<String>> {
    let mut repositories = Vec::new();
    // `total_count` (from the first response) is the authoritative stop point
    // — an installation with more than 1,000 repositories must not be
    // truncated — and a short page is the belt for a response that lies.
    // Pages past the end return an empty array, so the loop always
    // terminates.
    let mut total_count: Option<usize> = None;
    for page in 1u32.. {
        let body = fetch_json(
            &format!("{api_base}/installation/repositories?per_page=100&page={page}"),
            token,
        )
        .await
        .with_context(|| format!("GET /installation/repositories (page {page}) failed"))?;
        if total_count.is_none() {
            total_count = body
                .get("total_count")
                .and_then(serde_json::Value::as_u64)
                .map(|count| count as usize);
        }
        let batch = body
            .get("repositories")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let page_len = batch.len();
        repositories.extend(batch.into_iter().filter_map(|repository| {
            repository
                .get("full_name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        }));
        if page_len < 100 {
            break;
        }
        if total_count.is_some_and(|total| repositories.len() >= total) {
            break;
        }
    }
    Ok(repositories)
}

/// GET a JSON resource with `token` as the bearer, folding non-success
/// statuses into a [`MintRejected`] so the caller can distinguish a
/// credential refusal from a transport failure.
async fn fetch_json(url: &str, token: &str) -> anyhow::Result<serde_json::Value> {
    let response = crate::shared_http::CLIENT
        .get(url)
        .header("User-Agent", "preloop")
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    if !status.is_success() {
        let message = response.text().await.unwrap_or_default();
        return Err(MintRejected {
            status,
            message: message.chars().take(1024).collect(),
        }
        .into());
    }
    response
        .json()
        .await
        .with_context(|| format!("GET {url} returned a non-JSON body"))
}

/// Verify an RS256 GitHub App JWT: three dot-separated segments, `alg: RS256`
/// header, `iss` equal to `app_id`, unexpired, and the signature verifiable
/// against `public_key`. Returns the claims on success.
fn verify_app_jwt(
    app_id: &str,
    token: &str,
    public_key: &rsa::RsaPublicKey,
) -> Result<serde_json::Value, String> {
    let mut segments = token.splitn(3, '.');
    let (header_b64, claims_b64, signature_b64) =
        match (segments.next(), segments.next(), segments.next()) {
            (Some(header), Some(claims), Some(signature)) => (header, claims, signature),
            _ => return Err("not a JWT".to_owned()),
        };
    let decode = |encoded: &str| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
    };
    let header = decode(header_b64).ok_or_else(|| "JWT header is not JSON".to_owned())?;
    if header.get("alg").and_then(serde_json::Value::as_str) != Some("RS256") {
        return Err("JWT is not RS256".to_owned());
    }
    let claims = decode(claims_b64).ok_or_else(|| "JWT claims are not JSON".to_owned())?;
    if claims.get("iss").and_then(serde_json::Value::as_str) != Some(app_id) {
        return Err("JWT issuer is not this App".to_owned());
    }
    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_secs())
        .unwrap_or(u64::MAX);
    let expires_at = claims
        .get("exp")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "JWT has no `exp`".to_owned())?;
    if expires_at <= now {
        return Err("JWT is expired".to_owned());
    }
    if expires_at > now.saturating_add(600) {
        return Err("JWT expiration is beyond GitHub's ten-minute App-JWT lifetime".to_owned());
    }
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(signature_b64)
        .map_err(|_| "JWT signature is not base64url".to_owned())?;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::signature::Verifier;
    use sha2::Sha256;
    let signing_input = format!("{header_b64}.{claims_b64}");
    let signature = Signature::try_from(signature.as_slice())
        .map_err(|_| "JWT signature has the wrong shape".to_owned())?;
    VerifyingKey::<Sha256>::new(public_key.clone())
        .verify(signing_input.as_bytes(), &signature)
        .map_err(|_| "JWT signature does not verify".to_owned())?;
    Ok(claims)
}

/// The login a PAT maps to, from `GET /user` (cached). Falls back to the
/// dedicated [`PRELOOP_PAT_ACTOR`] placeholder when github.com cannot be
/// reached — auth already succeeded offline, so the dispatch must not fail on
/// actor resolution, but the label must not pretend to be the system bearer.
/// Failed lookups are **not** cached: a transient outage must not pin the
/// placeholder for the full actor TTL after github.com recovers.
async fn resolve_pat_actor(shared: &Arc<SharedState>, pat: &str) -> String {
    let key = format!("pat:{}", sha256_hex(pat));
    if let Some(actor) = shared.state.dispatch_actor_cache.get(&key) {
        return actor;
    }
    let api_base = crate::github::github_api_base();
    match fetch_json(&format!("{api_base}/user"), pat).await {
        Ok(user) => {
            let Some(actor) = user
                .get("login")
                .and_then(serde_json::Value::as_str)
                .filter(|login| !login.is_empty())
                .map(str::to_owned)
            else {
                warn!("PAT GET /user response had no login; using {PRELOOP_PAT_ACTOR}");
                return PRELOOP_PAT_ACTOR.to_owned();
            };
            shared.state.dispatch_actor_cache.put(key, actor.clone());
            actor
        }
        Err(error) => {
            warn!(
                ?error,
                "could not resolve the PAT's actor from github.com; using {PRELOOP_PAT_ACTOR}"
            );
            PRELOOP_PAT_ACTOR.to_owned()
        }
    }
}

/// The bot login of a registered App (`{slug}[bot]`), from `GET /app`
/// (cached). Falls back to `{app_id}[bot]` when github.com is unreachable —
/// the App JWT itself is offline-verifiable, so actor resolution must not
/// fail the dispatch.
async fn resolve_app_actor(shared: &Arc<SharedState>, app_id: &str) -> String {
    let key = format!("app:{app_id}");
    if let Some(actor) = shared.state.dispatch_actor_cache.get(&key) {
        return actor;
    }
    let actor = match find_app_by_id(shared, app_id) {
        Some(app) => {
            let jwt = match crate::github_app::sign_app_jwt(&app.app_id, &app.private_key) {
                Ok(jwt) => jwt,
                Err(_) => return format!("{app_id}[bot]"),
            };
            let api_base = crate::github::github_api_base();
            match fetch_json(&format!("{api_base}/app"), &jwt).await {
                Ok(app) => {
                    let slug = app
                        .get("slug")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| app_id.to_owned());
                    format!("{slug}[bot]")
                }
                Err(error) => {
                    warn!(
                        ?error,
                        app_id,
                        "could not resolve the App's bot login from github.com; using the App id"
                    );
                    format!("{app_id}[bot]")
                }
            }
        }
        None => format!("{app_id}[bot]"),
    };
    shared.state.dispatch_actor_cache.put(key, actor.clone());
    actor
}

/// The registered App with `app_id`, if any.
fn find_app_by_id<'a>(
    shared: &'a Arc<SharedState>,
    app_id: &str,
) -> Option<&'a crate::github_app::GitHubAppCredentials> {
    if let Some(registry) = &shared.state.github_apps {
        return registry.apps.iter().find(|app| app.app_id == app_id);
    }
    shared
        .state
        .github_app
        .as_ref()
        .filter(|app| app.app_id == app_id)
}

/// Authorization header value in either scheme GitHub accepts (`Bearer` or
/// `token`). HTTP auth-scheme names are case-insensitive, so `bearer`,
/// `BEARER`, `Token`, etc. all authenticate.
fn dispatch_bearer(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, rest) = value.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("Bearer") || scheme.eq_ignore_ascii_case("token") {
        Some(rest.trim())
    } else {
        None
    }
}

/// Whether `token` has the shape of a GitHub App JWT: three dot-separated
/// base64url segments whose header names `RS256`.
fn looks_like_app_jwt(token: &str) -> bool {
    let mut segments = token.splitn(3, '.');
    let (Some(header_b64), Some(_claims), Some(_signature)) =
        (segments.next(), segments.next(), segments.next())
    else {
        return false;
    };
    let Some(header) = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(header_b64)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
    else {
        return false;
    };
    header.get("alg").and_then(serde_json::Value::as_str) == Some("RS256")
}

/// Constant-time string comparison (HMAC-SHA256 with a fixed key, then
/// XOR accumulation over the fixed-size digests): never short-circuits on
/// the first differing byte, and the digest step means a length difference
/// cannot leak early either.
fn constant_time_eq(left: &str, right: &str) -> bool {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let digest = |value: &str| {
        let mut mac = HmacSha256::new_from_slice(b"preloop-dispatch-auth-compare")
            .expect("HMAC accepts keys of any length");
        mac.update(value.as_bytes());
        mac.finalize().into_bytes()
    };
    let left = digest(left);
    let right = digest(right);
    left.iter()
        .zip(right.iter())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// Hex SHA-256 of `value`.
fn sha256_hex(value: &str) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(value.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Short-TTL cache of validated installation tokens, keyed by token hash.
#[derive(Debug, Default)]
pub(crate) struct InstallationTokenCache {
    inner: parking_lot::Mutex<HashMap<String, CachedInstallation>>,
}

#[derive(Debug, Clone)]
struct CachedInstallation {
    validated_at: SystemTime,
    info: InstallationInfo,
}

impl InstallationTokenCache {
    pub(crate) fn get(&self, key: &str) -> Option<InstallationInfo> {
        let inner = self.inner.lock();
        inner.get(key).and_then(|cached| {
            cached
                .validated_at
                .elapsed()
                .ok()
                .filter(|age| *age < TOKEN_CACHE_TTL)
                .map(|_| cached.info.clone())
        })
    }

    pub(crate) fn put(&self, key: String, info: InstallationInfo) {
        let mut inner = self.inner.lock();
        let now = SystemTime::now();
        inner.retain(|_, cached| {
            cached
                .validated_at
                .elapsed()
                .map(|age| age < TOKEN_CACHE_TTL)
                .unwrap_or(false)
        });
        inner.insert(
            key,
            CachedInstallation {
                validated_at: now,
                info,
            },
        );
    }
}

/// Short-TTL cache of resolved actor logins (PAT `GET /user`, App `GET /app`).
#[derive(Debug, Default)]
pub(crate) struct DispatchActorCache {
    inner: parking_lot::Mutex<HashMap<String, (SystemTime, String)>>,
}

impl DispatchActorCache {
    pub(crate) fn get(&self, key: &str) -> Option<String> {
        let inner = self.inner.lock();
        inner.get(key).and_then(|(resolved_at, actor)| {
            resolved_at
                .elapsed()
                .ok()
                .filter(|age| *age < ACTOR_CACHE_TTL)
                .map(|_| actor.clone())
        })
    }

    pub(crate) fn put(&self, key: String, actor: String) {
        let mut inner = self.inner.lock();
        let now = SystemTime::now();
        inner.retain(|_, (resolved_at, _)| {
            resolved_at
                .elapsed()
                .map(|age| age < ACTOR_CACHE_TTL)
                .unwrap_or(false)
        });
        inner.insert(key, (now, actor));
    }
}
