//! Host-side GitHub self-hosted-runner administration.
//!
//! This client covers the repository and organization REST endpoint families:
//! registration/remove tokens (one-hour credentials), JIT configuration,
//! runner listing/deletion, and runner-version deprecations. The endpoint
//! paths are built from the configured API base, so GitHub Enterprise Server
//! works as well as `api.github.com`.
//!
//! Administration tokens are intentionally not obtained through
//! [`crate::github_app::get_or_mint_token`]. That path is for job tokens and
//! narrows an ungranted permission set on a 422; silently narrowing
//! `administration: write` would make runner administration fail later with an
//! unexplained 403. This client checks the installation grant first and fails
//! with [`RunnerAdminError::MissingGrant`] naming the required permission.
//!
//! The repository mint sends `repositories: [repo]` to keep the installation
//! token repository-scoped. The organization mint omits `repositories`: the
//! organization runner permission is not repository-scoped, and
//! [`crate::github_app::mint_installation_token`] always sends that field
//! because it is exclusively the job-token path. Registration and remove
//! tokens expire after one hour and must be minted for each provision or
//! teardown rather than persisted as static credentials.

use std::collections::BTreeMap;
use std::time::SystemTime;

use anyhow::{Context, anyhow, bail};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::github_app::{
    GitHubAppCredentials, MintRejected, api_base, installation_grants, installation_id_for,
    installation_permissions, sign_app_jwt, ungranted_scopes,
};
use crate::shared_http::CLIENT;

const MAX_RUNNER_PAGES: u32 = 10;
const RUNNERS_PER_PAGE: u32 = 100;

/// The account surface served by a runner-admin client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunnerAdminScope {
    /// Repository runners; requires repository `Administration: write`.
    Repo { owner: String, repo: String },
    /// Organization runners; requires organization `Self-hosted runners:
    /// write` (`organization_self_hosted_runners` in the App grant map).
    Org { org: String },
}

impl RunnerAdminScope {
    /// Construct repository scope from an `owner/repo` slug.
    pub fn repository(slug: &str) -> anyhow::Result<Self> {
        let mut parts = slug.split('/');
        let owner = parts.next().unwrap_or_default().trim();
        let repo = parts.next().unwrap_or_default().trim();
        if owner.is_empty() || repo.is_empty() || parts.next().is_some() {
            bail!("expected an `owner/repo` slug for runner administration, got {slug:?}");
        }
        Ok(Self::Repo {
            owner: owner.to_owned(),
            repo: repo.to_owned(),
        })
    }

    fn owner(&self) -> &str {
        match self {
            Self::Repo { owner, .. } => owner,
            Self::Org { org } => org,
        }
    }

    fn base_path(&self) -> String {
        match self {
            Self::Repo { owner, repo } => format!("/repos/{owner}/{repo}/actions/runners"),
            Self::Org { org } => format!("/orgs/{org}/actions/runners"),
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::Repo { owner, repo } => format!("{owner}/{repo}"),
            Self::Org { org } => format!("organization {org}"),
        }
    }

    fn permission_key(&self) -> &'static str {
        match self {
            Self::Repo { .. } => "administration",
            Self::Org { .. } => "organization_self_hosted_runners",
        }
    }

    fn permission_label(&self) -> &'static str {
        match self {
            Self::Repo { .. } => "repository `Administration` permission",
            Self::Org { .. } => "organization `Self-hosted runners` permission",
        }
    }

    fn mint_permissions(&self) -> BTreeMap<String, String> {
        BTreeMap::from([(self.permission_key().to_owned(), "write".to_owned())])
    }
}

/// A one-hour registration or remove credential returned by GitHub.
#[derive(Clone)]
pub struct RunnerAdminToken {
    pub token: String,
    pub expires_at: SystemTime,
}

impl std::fmt::Debug for RunnerAdminToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunnerAdminToken")
            .field("token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// A runner returned by GitHub's list or JIT endpoints.
#[derive(Clone, Debug, Deserialize)]
pub struct GithubRunner {
    pub id: u64,
    pub name: String,
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub busy: bool,
    #[serde(default)]
    pub labels: Vec<GithubRunnerLabel>,
    #[serde(default)]
    pub runner_group_id: Option<u64>,
}

/// A runner label from GitHub's response.
#[derive(Clone, Debug, Deserialize)]
pub struct GithubRunnerLabel {
    #[serde(default)]
    pub id: Option<u64>,
    pub name: String,
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
}

/// Inputs for `generate-jitconfig`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JitConfigRequest {
    pub name: String,
    pub runner_group_id: u64,
    pub labels: Vec<String>,
    pub work_folder: String,
}

impl Default for JitConfigRequest {
    fn default() -> Self {
        Self {
            name: String::new(),
            runner_group_id: 1,
            labels: Vec::new(),
            work_folder: "_work".to_owned(),
        }
    }
}

/// The encoded, single-use runner configuration returned by JIT generation.
pub struct JitConfig {
    pub encoded_jit_config: String,
    pub runner: GithubRunner,
}

impl std::fmt::Debug for JitConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JitConfig")
            .field("encoded_jit_config", &"<redacted>")
            .field("runner", &self.runner)
            .finish()
    }
}

/// The version deprecation response shared with the server's mirror route.
pub use crate::runner_deprecations::RunnerDeprecation;

/// A runner-admin failure with a typed missing-grant case.
#[derive(Debug)]
pub enum RunnerAdminError {
    /// GitHub rejected an operation with a status that is used for a missing
    /// grant. A 404 can also mean that the runner/version is gone; GitHub does
    /// not distinguish that from an installation lacking the permission.
    MissingGrant {
        status: Option<reqwest::StatusCode>,
        scope: RunnerAdminScope,
        permission: String,
        operation: String,
    },
    /// A non-permission GitHub API response.
    Api {
        status: reqwest::StatusCode,
        operation: String,
        message: String,
    },
    /// Signing, transport, or response-decoding failure.
    Other(anyhow::Error),
}

impl std::fmt::Display for RunnerAdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingGrant {
                status,
                scope,
                permission,
                operation,
            } => {
                let status = status
                    .map(|status| format!("GitHub answered {status} for "))
                    .unwrap_or_default();
                write!(
                    f,
                    "{status}{operation} on {}: the GitHub App installation lacks the {} ({permission}); grant it in the App's permissions and approve the pending permission review in the GitHub UI",
                    scope.describe(),
                    scope.permission_label(),
                )
            }
            Self::Api {
                status,
                operation,
                message,
            } => write!(f, "{operation} failed with {status}: {message}"),
            Self::Other(error) => write!(f, "{error:#}"),
        }
    }
}

impl std::error::Error for RunnerAdminError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Other(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<anyhow::Error> for RunnerAdminError {
    fn from(error: anyhow::Error) -> Self {
        Self::Other(error)
    }
}

impl RunnerAdminError {
    /// Whether this failure names a required App grant.
    pub fn is_missing_grant(&self) -> bool {
        matches!(self, Self::MissingGrant { .. })
    }
}

/// Narrow mockable contract consumed by a runner pool.
#[async_trait::async_trait]
pub trait RunnerAdmin: Send + Sync {
    /// Mint a registration token. GitHub expires it after one hour.
    async fn registration_token(&self) -> Result<RunnerAdminToken, RunnerAdminError>;
    /// Mint a remove token. GitHub expires it after one hour.
    async fn remove_token(&self) -> Result<RunnerAdminToken, RunnerAdminError>;
    /// Generate a JIT runner configuration.
    async fn generate_jitconfig(
        &self,
        request: &JitConfigRequest,
    ) -> Result<JitConfig, RunnerAdminError>;
    /// List all runners at this scope.
    async fn list_runners(&self) -> Result<Vec<GithubRunner>, RunnerAdminError>;
    /// Delete one runner by its GitHub id.
    async fn delete_runner(&self, runner_id: u64) -> Result<(), RunnerAdminError>;
    /// Read GitHub's runner-version deprecation schedule.
    async fn deprecation(&self, version: &str) -> Result<RunnerDeprecation, RunnerAdminError>;
}

/// GitHub REST implementation of [`RunnerAdmin`].
pub struct GithubRunnerAdmin {
    api_base: String,
    creds: GitHubAppCredentials,
    scope: RunnerAdminScope,
}

impl GithubRunnerAdmin {
    /// Construct a client using `PRELOOP_GITHUB_API_URL`, defaulting to
    /// `https://api.github.com`.
    pub fn new(creds: GitHubAppCredentials, scope: RunnerAdminScope) -> Self {
        Self::with_api_base(api_base(), creds, scope)
    }

    /// Construct a client against an explicit GitHub/GHES base, useful in
    /// tests and for deployments that set a non-default API endpoint.
    pub fn with_api_base(
        api_base: impl Into<String>,
        creds: GitHubAppCredentials,
        scope: RunnerAdminScope,
    ) -> Self {
        Self {
            api_base: api_base.into().trim_end_matches('/').to_owned(),
            creds,
            scope,
        }
    }

    async fn admin_token(&self) -> Result<String, RunnerAdminError> {
        let app_jwt = sign_app_jwt(&self.creds.app_id, &self.creds.private_key)
            .context("signing the GitHub App JWT")?;
        let installation_id =
            installation_id_for(&self.api_base, &self.creds, &app_jwt, self.scope.owner()).await?;
        let requested = self.scope.mint_permissions();
        let grants = installation_grants(&self.api_base, &app_jwt, installation_id)
            .await
            .context("reading the GitHub App installation permissions")?;
        if let Some(permission) = ungranted_scopes(&requested, &grants).into_iter().next() {
            return Err(RunnerAdminError::MissingGrant {
                status: None,
                scope: self.scope.clone(),
                permission,
                operation: "minting a runner-admin installation token".to_owned(),
            });
        }

        let body = match &self.scope {
            RunnerAdminScope::Repo { repo, .. } => json!({
                "repositories": [repo],
                "permissions": installation_permissions(&requested),
            }),
            RunnerAdminScope::Org { .. } => json!({
                "permissions": installation_permissions(&requested),
            }),
        };
        let url = format!(
            "{}/app/installations/{installation_id}/access_tokens",
            self.api_base
        );
        let response = CLIENT
            .post(&url)
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {app_jwt}"))
            .header("Accept", "application/vnd.github+json")
            .json(&body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = response.status();
        let payload = response
            .text()
            .await
            .with_context(|| format!("POST {url} response body"))?;
        if !status.is_success() {
            let message: String = payload.chars().take(1024).collect();
            if matches!(
                status,
                reqwest::StatusCode::FORBIDDEN
                    | reqwest::StatusCode::NOT_FOUND
                    | reqwest::StatusCode::UNPROCESSABLE_ENTITY
            ) {
                return Err(RunnerAdminError::MissingGrant {
                    status: Some(status),
                    scope: self.scope.clone(),
                    permission: format!("{}: write", self.scope.permission_key()),
                    operation: format!("POST {url}"),
                });
            }
            return Err(RunnerAdminError::Other(
                anyhow::Error::new(MintRejected { status, message })
                    .context("minting a runner-admin installation token"),
            ));
        }
        let payload: Value = serde_json::from_str(&payload)
            .with_context(|| format!("POST {url} returned a non-JSON body"))?;
        Ok(parse_token(&payload, &url)?.token)
    }

    async fn request(
        &self,
        method: reqwest::Method,
        suffix: &str,
        query: Option<(u32, u32)>,
        body: Option<Value>,
    ) -> Result<Value, RunnerAdminError> {
        let token = self.admin_token().await?;
        let url = format!("{}{}{}", self.api_base, self.scope.base_path(), suffix);
        let operation = format!("{method} {}{}", self.scope.base_path(), suffix);
        let mut request = CLIENT
            .request(method, &url)
            .bearer_auth(token)
            .header("User-Agent", "preloop")
            .header("Accept", "application/vnd.github+json");
        if let Some((per_page, page)) = query {
            request = request.query(&[("per_page", per_page), ("page", page)]);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.with_context(|| operation.clone())?;
        let status = response.status();
        let payload = response.text().await.with_context(|| operation.clone())?;
        if matches!(
            status,
            reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::NOT_FOUND
        ) {
            return Err(RunnerAdminError::MissingGrant {
                status: Some(status),
                scope: self.scope.clone(),
                permission: format!("{}: write", self.scope.permission_key()),
                operation,
            });
        }
        if !status.is_success() {
            return Err(RunnerAdminError::Api {
                status,
                operation,
                message: payload.chars().take(1024).collect(),
            });
        }
        if payload.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&payload)
            .with_context(|| format!("{operation} returned a non-JSON body"))
            .map_err(RunnerAdminError::Other)
    }
}

fn parse_token(payload: &Value, url: &str) -> anyhow::Result<RunnerAdminToken> {
    let token = payload
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("POST {url} returned no `token`"))?
        .to_owned();
    let expiry = payload
        .get("expires_at")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("POST {url} returned no `expires_at`"))?;
    let expires_at = chrono::DateTime::parse_from_rfc3339(expiry)
        .with_context(|| format!("unparsable token expiry {expiry:?}"))?;
    Ok(RunnerAdminToken {
        token,
        expires_at: SystemTime::from(expires_at),
    })
}

#[async_trait::async_trait]
impl RunnerAdmin for GithubRunnerAdmin {
    async fn registration_token(&self) -> Result<RunnerAdminToken, RunnerAdminError> {
        let suffix = "/registration-token";
        let payload = self
            .request(reqwest::Method::POST, suffix, None, None)
            .await?;
        parse_token(&payload, &self.api_base).map_err(RunnerAdminError::Other)
    }

    async fn remove_token(&self) -> Result<RunnerAdminToken, RunnerAdminError> {
        let suffix = "/remove-token";
        let payload = self
            .request(reqwest::Method::POST, suffix, None, None)
            .await?;
        parse_token(&payload, &self.api_base).map_err(RunnerAdminError::Other)
    }

    async fn generate_jitconfig(
        &self,
        request: &JitConfigRequest,
    ) -> Result<JitConfig, RunnerAdminError> {
        let suffix = "/generate-jitconfig";
        let payload = self
            .request(
                reqwest::Method::POST,
                suffix,
                None,
                Some(json!({
                    "name": request.name,
                    "runner_group_id": request.runner_group_id,
                    "labels": request.labels,
                    "work_folder": request.work_folder,
                })),
            )
            .await?;
        let encoded = payload
            .get("encoded_jit_config")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("generate-jitconfig returned no `encoded_jit_config`"))?
            .to_owned();
        let runner = serde_json::from_value(
            payload
                .get("runner")
                .cloned()
                .ok_or_else(|| anyhow!("generate-jitconfig returned no `runner`"))?,
        )
        .context("decoding the generated runner")?;
        Ok(JitConfig {
            encoded_jit_config: encoded,
            runner,
        })
    }

    async fn list_runners(&self) -> Result<Vec<GithubRunner>, RunnerAdminError> {
        let mut all = Vec::new();
        for page in 1..=MAX_RUNNER_PAGES {
            let payload = self
                .request(
                    reqwest::Method::GET,
                    "",
                    Some((RUNNERS_PER_PAGE, page)),
                    None,
                )
                .await?;
            let total = payload.get("total_count").and_then(Value::as_u64);
            let runners: Vec<GithubRunner> = serde_json::from_value(
                payload
                    .get("runners")
                    .cloned()
                    .ok_or_else(|| anyhow!("list runners response has no `runners`"))?,
            )
            .context("decoding the runner list")?;
            let count = runners.len();
            all.extend(runners);
            if count < RUNNERS_PER_PAGE as usize
                || total.is_some_and(|total| all.len() as u64 >= total)
            {
                break;
            }
        }
        Ok(all)
    }

    async fn delete_runner(&self, runner_id: u64) -> Result<(), RunnerAdminError> {
        self.request(
            reqwest::Method::DELETE,
            &format!("/{runner_id}"),
            None,
            None,
        )
        .await?;
        Ok(())
    }

    async fn deprecation(&self, version: &str) -> Result<RunnerDeprecation, RunnerAdminError> {
        let suffix = format!("/deprecations/{version}");
        let payload = self
            .request(reqwest::Method::GET, &suffix, None, None)
            .await?;
        serde_json::from_value(payload)
            .context("decoding the runner deprecation response")
            .map_err(RunnerAdminError::Other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Path, Query, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::{delete, get, post};
    use axum::{Json, Router};
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Default)]
    struct StubState {
        grants: Value,
        admin_status: Option<StatusCode>,
        access_bodies: Mutex<Vec<Value>>,
        admin_requests: Mutex<Vec<(String, String, String)>>,
    }

    async fn installation(State(state): State<Arc<StubState>>) -> Json<Value> {
        Json(json!({
            "account": { "login": "acme" },
            "permissions": state.grants.clone(),
        }))
    }

    async fn access_token(
        State(state): State<Arc<StubState>>,
        body: String,
    ) -> (StatusCode, Json<Value>) {
        let body: Value = serde_json::from_str(&body).expect("JSON access-token body");
        state.access_bodies.lock().push(body);
        (
            StatusCode::OK,
            Json(json!({
                "token": "admin-installation-token",
                "expires_at": "2099-01-01T00:00:00Z",
            })),
        )
    }

    async fn admin_response(
        State(state): State<Arc<StubState>>,
        headers: HeaderMap,
        body: String,
        path: String,
        method: &str,
        response: Value,
    ) -> axum::response::Response {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        state
            .admin_requests
            .lock()
            .push((method.to_owned(), path, authorization));
        if let Some(status) = state.admin_status {
            return status.into_response();
        }
        let _ = body;
        (StatusCode::OK, Json(response)).into_response()
    }

    async fn repo_registration(
        State(state): State<Arc<StubState>>,
        headers: HeaderMap,
        body: String,
    ) -> axum::response::Response {
        admin_response(
            State(state),
            headers,
            body,
            "/repos/acme/repo/actions/runners/registration-token".to_owned(),
            "POST",
            json!({ "token": "registration-token", "expires_at": "2099-01-01T00:00:00Z" }),
        )
        .await
    }

    async fn org_registration(
        State(state): State<Arc<StubState>>,
        headers: HeaderMap,
        body: String,
    ) -> axum::response::Response {
        admin_response(
            State(state),
            headers,
            body,
            "/orgs/acme/actions/runners/registration-token".to_owned(),
            "POST",
            json!({ "token": "org-registration-token", "expires_at": "2099-01-01T00:00:00Z" }),
        )
        .await
    }

    async fn repo_remove(
        State(state): State<Arc<StubState>>,
        headers: HeaderMap,
        body: String,
    ) -> axum::response::Response {
        admin_response(
            State(state),
            headers,
            body,
            "/repos/acme/repo/actions/runners/remove-token".to_owned(),
            "POST",
            json!({ "token": "remove-token", "expires_at": "2099-01-01T00:00:00Z" }),
        )
        .await
    }

    async fn repo_jit(
        State(state): State<Arc<StubState>>,
        headers: HeaderMap,
        body: String,
    ) -> axum::response::Response {
        admin_response(
            State(state),
            headers,
            body,
            "/repos/acme/repo/actions/runners/generate-jitconfig".to_owned(),
            "POST",
            json!({
                "encoded_jit_config": "encoded-jit",
                "runner": { "id": 42, "name": "vm-42", "labels": [] },
            }),
        )
        .await
    }

    async fn repo_list(
        State(state): State<Arc<StubState>>,
        headers: HeaderMap,
        Query(query): Query<HashMap<String, String>>,
    ) -> axum::response::Response {
        let path = format!(
            "/repos/acme/repo/actions/runners?page={}&per_page={}",
            query.get("page").map(String::as_str).unwrap_or_default(),
            query
                .get("per_page")
                .map(String::as_str)
                .unwrap_or_default(),
        );
        admin_response(
            State(state),
            headers,
            String::new(),
            path,
            "GET",
            json!({
                "total_count": 1,
                "runners": [{ "id": 42, "name": "vm-42", "status": "online", "busy": false, "labels": [] }],
            }),
        )
        .await
    }

    async fn repo_delete(
        State(state): State<Arc<StubState>>,
        headers: HeaderMap,
        Path(id): Path<u64>,
    ) -> axum::response::Response {
        admin_response(
            State(state),
            headers,
            String::new(),
            format!("/repos/acme/repo/actions/runners/{id}"),
            "DELETE",
            Value::Null,
        )
        .await
    }

    async fn repo_deprecation(
        State(state): State<Arc<StubState>>,
        headers: HeaderMap,
        Path(version): Path<String>,
    ) -> axum::response::Response {
        admin_response(
            State(state),
            headers,
            String::new(),
            format!("/repos/acme/repo/actions/runners/deprecations/{version}"),
            "GET",
            json!({
                "runner_version": version,
                "runtime_deprecates_at": null,
                "registration_deprecates_at": null,
            }),
        )
        .await
    }

    async fn start_stub(
        grants: Value,
        admin_status: Option<StatusCode>,
    ) -> (String, Arc<StubState>) {
        let state = Arc::new(StubState {
            grants,
            admin_status,
            ..Default::default()
        });
        let app = Router::new()
            .route("/app/installations/{id}", get(installation))
            .route("/app/installations/{id}/access_tokens", post(access_token))
            .route(
                "/repos/acme/repo/actions/runners/registration-token",
                post(repo_registration),
            )
            .route(
                "/orgs/acme/actions/runners/registration-token",
                post(org_registration),
            )
            .route(
                "/repos/acme/repo/actions/runners/remove-token",
                post(repo_remove),
            )
            .route(
                "/repos/acme/repo/actions/runners/generate-jitconfig",
                post(repo_jit),
            )
            .route("/repos/acme/repo/actions/runners", get(repo_list))
            .route("/repos/acme/repo/actions/runners/{id}", delete(repo_delete))
            .route(
                "/repos/acme/repo/actions/runners/deprecations/{version}",
                get(repo_deprecation),
            )
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub API");
        let base = format!("http://{}", listener.local_addr().expect("stub address"));
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve stub API");
        });
        (base, state)
    }

    fn credentials() -> GitHubAppCredentials {
        let mut credentials = GitHubAppCredentials::for_tests(
            "4242",
            rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).expect("test RSA key"),
            crate::github_app::MintFailurePolicy::Error,
        );
        credentials.installation_id = Some(101);
        credentials
    }

    fn client(base: &str, scope: RunnerAdminScope) -> GithubRunnerAdmin {
        GithubRunnerAdmin::with_api_base(base, credentials(), scope)
    }

    #[tokio::test]
    async fn repo_admin_calls_cover_each_endpoint_and_scope_the_mint() {
        let (base, state) = start_stub(json!({ "administration": "write" }), None).await;
        let admin = client(
            &base,
            RunnerAdminScope::Repo {
                owner: "acme".to_owned(),
                repo: "repo".to_owned(),
            },
        );
        assert_eq!(
            admin
                .registration_token()
                .await
                .expect("registration")
                .token,
            "registration-token"
        );
        assert_eq!(
            admin.remove_token().await.expect("remove").token,
            "remove-token"
        );
        let jit = admin
            .generate_jitconfig(&JitConfigRequest {
                name: "vm-42".to_owned(),
                runner_group_id: 1,
                labels: vec!["self-hosted".to_owned()],
                work_folder: "_work".to_owned(),
            })
            .await
            .expect("jit config");
        assert_eq!(jit.encoded_jit_config, "encoded-jit");
        assert_eq!(admin.list_runners().await.expect("list")[0].id, 42);
        admin.delete_runner(42).await.expect("delete");
        assert_eq!(
            admin
                .deprecation("2.336.0")
                .await
                .expect("deprecation")
                .runner_version,
            "2.336.0"
        );

        let bodies = state.access_bodies.lock();
        assert_eq!(bodies.len(), 6, "each admin operation mints a fresh token");
        assert!(
            bodies
                .iter()
                .all(|body| body["permissions"] == json!({ "administration": "write" }))
        );
        assert_eq!(bodies[0]["repositories"], json!(["repo"]));
        let requests = state.admin_requests.lock();
        assert!(
            requests
                .iter()
                .all(|(_, _, authorization)| authorization == "Bearer admin-installation-token")
        );
        assert!(
            requests
                .iter()
                .any(|(method, path, _)| method == "POST" && path.ends_with("registration-token"))
        );
        assert!(
            requests
                .iter()
                .any(|(method, path, _)| method == "POST" && path.ends_with("remove-token"))
        );
        assert!(
            requests
                .iter()
                .any(|(method, path, _)| method == "POST" && path.ends_with("generate-jitconfig"))
        );
        assert!(
            requests
                .iter()
                .any(|(method, path, _)| method == "GET" && path.contains("deprecations/2.336.0"))
        );
        assert!(
            requests
                .iter()
                .any(|(method, path, _)| method == "DELETE" && path.ends_with("/42"))
        );
    }

    #[tokio::test]
    async fn org_registration_uses_org_endpoint_and_omits_repositories() {
        let (base, state) =
            start_stub(json!({ "organization_self_hosted_runners": "write" }), None).await;
        let admin = client(
            &base,
            RunnerAdminScope::Org {
                org: "acme".to_owned(),
            },
        );
        assert_eq!(
            admin.registration_token().await.expect("org token").token,
            "org-registration-token"
        );
        let bodies = state.access_bodies.lock();
        assert_eq!(
            bodies[0]["permissions"],
            json!({ "organization_self_hosted_runners": "write" })
        );
        assert!(bodies[0].get("repositories").is_none());
        assert_eq!(
            state.admin_requests.lock()[0].1,
            "/orgs/acme/actions/runners/registration-token"
        );
    }

    #[tokio::test]
    async fn missing_repo_grant_is_reported_before_the_admin_mint() {
        let (base, state) = start_stub(json!({}), None).await;
        let admin = client(
            &base,
            RunnerAdminScope::Repo {
                owner: "acme".to_owned(),
                repo: "repo".to_owned(),
            },
        );
        let error = admin.registration_token().await.expect_err("missing grant");
        assert!(error.is_missing_grant());
        assert!(error.to_string().contains("administration: write"));
        assert!(error.to_string().contains("Administration"));
        assert!(state.access_bodies.lock().is_empty());
    }

    #[tokio::test]
    async fn forbidden_admin_response_names_org_grant() {
        let (base, _state) = start_stub(
            json!({ "organization_self_hosted_runners": "write" }),
            Some(StatusCode::FORBIDDEN),
        )
        .await;
        let admin = client(
            &base,
            RunnerAdminScope::Org {
                org: "acme".to_owned(),
            },
        );
        let error = admin
            .registration_token()
            .await
            .expect_err("forbidden response");
        assert!(error.is_missing_grant());
        assert!(
            error
                .to_string()
                .contains("organization_self_hosted_runners: write")
        );
        assert!(error.to_string().contains("Self-hosted runners"));
    }
}
