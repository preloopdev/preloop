//! Engine-hosted GitHub App setup.
//!
//! When a public URL is configured, `preloop setup github --via app` asks the
//! running engine to host the App-creation form, so the operator never needs a
//! listener on the machine they are sitting at:
//!
//! 1. `POST /api/v1/github/setup-sessions` (system token) opens a session and
//!    returns its one-time link, `{public_url}/setup/github?t=<token>`.
//! 2. Opening the link serves the App-creation form with a fresh state nonce
//!    and burns the token. The form posts the manifest to GitHub.
//! 3. GitHub redirects the browser to `{public_url}/setup/github/callback` with
//!    a one-time code and the nonce. The engine checks the nonce, converts the
//!    code itself, and stores the credentials with the same code the CLI uses.
//! 4. `GET /api/v1/github/setup-sessions/{id}` (system token) reports the
//!    session's status, which is how the CLI learns the outcome.
//!
//! The browser routes answer 404 unless a session is awaiting them, so an
//! engine with no setup in progress exposes nothing new. Only one session is
//! live at a time: opening another replaces it and invalidates its link.

use crate::errors::ApiError;
use crate::github_app_setup::{
    AppCredentials, FormPage, constant_time_eq, convert_manifest_code, creation_url, error_page,
    escape_html, form_page, manifest, random_nonce, save_app_credentials_at,
};
use crate::state::SharedState;
use axum::Json;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// How long a setup link works after its session opens.
pub const SETUP_LINK_TTL: Duration = Duration::from_secs(10 * 60);

/// How long the operator has to approve the App once the form is served.
pub const APPROVAL_TTL: Duration = Duration::from_secs(10 * 60);

/// Invalid tokens or states a live session tolerates before it is burned.
const MAX_BAD_ATTEMPTS: u32 = 5;

/// Requests the browser-facing setup routes answer per [`RATE_WINDOW`],
/// across every client. A single operator drives a setup, so one shared
/// budget is enough to slow a guesser down.
const RATE_LIMIT: u32 = 30;
const RATE_WINDOW: Duration = Duration::from_secs(60);

/// Explains where GitHub's result goes, on the form the engine serves.
const DELIVERY_NOTE: &str = "GitHub returns the credentials to this engine, which stores them \
     in its config. The private key is never shown in the browser.";

/// The engine's setup sessions. At most one is live.
#[derive(Default)]
pub struct SetupSessions {
    inner: Mutex<Table>,
}

#[derive(Default)]
struct Table {
    current: Option<Session>,
    window_started: Option<Instant>,
    window_requests: u32,
}

struct Session {
    id: String,
    /// SHA-256 of the one-time token. Cleared the moment the link is used.
    token_digest: Option<[u8; 32]>,
    /// Nonce GitHub must echo back. Set when the form is served.
    state: Option<String>,
    public_url: String,
    org: Option<String>,
    app_name: String,
    /// Link expiry while awaiting the link; approval expiry afterwards.
    deadline: Instant,
    phase: Phase,
    bad_attempts: u32,
}

#[derive(Clone)]
enum Phase {
    AwaitingLink,
    AwaitingApproval,
    Converting,
    Completed(CompletedApp),
    Failed(String),
}

/// What a finished setup tells the CLI. Carries no secrets: the private key
/// and webhook secret are stored on the engine and never leave it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedApp {
    pub app_id: u64,
    pub slug: Option<String>,
    pub install_url: Option<String>,
    pub webhook_secret_stored: bool,
}

/// A session's status as a poller sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    AwaitingLink,
    AwaitingApproval,
    Converting,
    Completed(CompletedApp),
    Failed(String),
    Expired,
}

/// A freshly opened session: its handle, and the one-time token the setup
/// link carries.
pub struct NewSession {
    pub id: String,
    pub token: String,
}

/// What the form needs once its link has been redeemed.
pub struct FormGrant {
    pub state: String,
    pub public_url: String,
    pub org: Option<String>,
    pub app_name: String,
}

/// A callback that passed the state check and may now be converted.
pub struct Conversion {
    pub id: String,
    pub code: String,
}

/// Why a browser-facing request was refused.
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    /// No session is awaiting this request, so the route answers 404.
    Inactive,
    /// A session is awaiting, but the request did not match it.
    Rejected,
}

impl SetupSessions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Admit one request against the route rate limit. `false` means the
    /// window is full and the caller should answer 429.
    pub fn admit_request(&self, now: Instant) -> bool {
        let mut table = self.inner.lock();
        let window_open = table
            .window_started
            .is_some_and(|start| now.duration_since(start) < RATE_WINDOW);
        if !window_open {
            table.window_started = Some(now);
            table.window_requests = 0;
        }
        if table.window_requests >= RATE_LIMIT {
            return false;
        }
        table.window_requests += 1;
        true
    }

    /// Start a session, replacing any earlier one.
    pub fn open(
        &self,
        now: Instant,
        public_url: String,
        org: Option<String>,
        app_name: String,
    ) -> NewSession {
        let token = random_token();
        let id = random_id();
        let session = Session {
            id: id.clone(),
            token_digest: Some(digest(&token)),
            state: None,
            public_url,
            org,
            app_name,
            deadline: now + SETUP_LINK_TTL,
            phase: Phase::AwaitingLink,
            bad_attempts: 0,
        };
        self.inner.lock().current = Some(session);
        NewSession { id, token }
    }

    /// Redeem the setup link for `token`. A live link with the right token is
    /// burned and returns what the form needs; anything else returns `None`,
    /// and a wrong token counts against the session.
    pub fn redeem_link(&self, now: Instant, token: &str) -> Option<FormGrant> {
        let mut table = self.inner.lock();
        let session = table.current.as_mut()?;
        if !matches!(session.phase, Phase::AwaitingLink) || now >= session.deadline {
            return None;
        }
        let stored = session.token_digest?;
        if !constant_time_eq(&stored, &digest(token)) {
            session.count_bad_attempt();
            return None;
        }
        let state = random_nonce();
        session.token_digest = None;
        session.state = Some(state.clone());
        session.phase = Phase::AwaitingApproval;
        session.deadline = now + APPROVAL_TTL;
        Some(FormGrant {
            state,
            public_url: session.public_url.clone(),
            org: session.org.clone(),
            app_name: session.app_name.clone(),
        })
    }

    /// Check GitHub's redirect against the live approval. On success the
    /// session moves to `Converting`, the state nonce is consumed, and the code
    /// is handed back once, so a repeated redirect finds nothing to convert.
    pub fn begin_conversion(
        &self,
        now: Instant,
        state: Option<&str>,
        code: Option<&str>,
    ) -> Result<Conversion, Refusal> {
        let mut table = self.inner.lock();
        let session = table.current.as_mut().ok_or(Refusal::Inactive)?;
        if !matches!(session.phase, Phase::AwaitingApproval) || now >= session.deadline {
            return Err(Refusal::Inactive);
        }
        let state_matches = match (session.state.as_deref(), state) {
            (Some(expected), Some(presented)) => {
                constant_time_eq(expected.as_bytes(), presented.as_bytes())
            }
            _ => false,
        };
        let code = code.filter(|code| !code.is_empty());
        let Some(code) = code.filter(|_| state_matches) else {
            session.count_bad_attempt();
            return Err(Refusal::Rejected);
        };
        session.state = None;
        session.phase = Phase::Converting;
        Ok(Conversion {
            id: session.id.clone(),
            code: code.to_owned(),
        })
    }

    /// Record how a conversion ended. Ignored unless `id` names the session
    /// that is converting.
    pub fn finish(&self, id: &str, outcome: Result<CompletedApp, String>) {
        let mut table = self.inner.lock();
        let Some(session) = table
            .current
            .as_mut()
            .filter(|session| session.id == id && matches!(session.phase, Phase::Converting))
        else {
            return;
        };
        session.phase = match outcome {
            Ok(app) => Phase::Completed(app),
            Err(reason) => Phase::Failed(reason),
        };
    }

    /// The status a poller sees for session `id`, or `None` when no such
    /// session is current.
    pub fn status(&self, now: Instant, id: &str) -> Option<Status> {
        let table = self.inner.lock();
        let session = table.current.as_ref().filter(|session| session.id == id)?;
        let pending = matches!(session.phase, Phase::AwaitingLink | Phase::AwaitingApproval);
        if pending && now >= session.deadline {
            return Some(Status::Expired);
        }
        Some(match &session.phase {
            Phase::AwaitingLink => Status::AwaitingLink,
            Phase::AwaitingApproval => Status::AwaitingApproval,
            Phase::Converting => Status::Converting,
            Phase::Completed(app) => Status::Completed(app.clone()),
            Phase::Failed(reason) => Status::Failed(reason.clone()),
        })
    }
}

impl Session {
    /// Count one bad token or state. The fifth burns the session, so a live
    /// link cannot be guessed at indefinitely.
    fn count_bad_attempt(&mut self) {
        self.bad_attempts += 1;
        if self.bad_attempts >= MAX_BAD_ATTEMPTS {
            self.token_digest = None;
            self.state = None;
            self.phase = Phase::Failed(
                "too many invalid setup requests; start again with `preloop setup github --via app`"
                    .to_owned(),
            );
        }
    }
}

impl Status {
    fn to_json(&self) -> Value {
        match self {
            Status::AwaitingLink => json!({ "status": "awaiting_link" }),
            Status::AwaitingApproval => json!({ "status": "awaiting_approval" }),
            Status::Converting => json!({ "status": "converting" }),
            Status::Completed(app) => json!({
                "status": "completed",
                "app_id": app.app_id,
                "slug": app.slug,
                "install_url": app.install_url,
                "webhook_secret_stored": app.webhook_secret_stored,
            }),
            Status::Failed(reason) => json!({ "status": "failed", "error": reason }),
            Status::Expired => json!({ "status": "expired" }),
        }
    }
}

/// Body of `POST /api/v1/github/setup-sessions`.
#[derive(Debug, Deserialize)]
pub struct OpenRequest {
    /// Public URL the operator's browser reaches this engine at. Becomes the
    /// manifest's redirect and webhook base.
    pub public_url: String,
    /// Create the App under this organization instead of the personal account.
    #[serde(default)]
    pub org: Option<String>,
    /// Name of the App GitHub creates.
    #[serde(default = "default_app_name")]
    pub app_name: String,
}

fn default_app_name() -> String {
    "preloop-local".to_owned()
}

/// Query of `GET /setup/github`.
#[derive(Debug, Default, Deserialize)]
pub struct LinkQuery {
    #[serde(default)]
    pub t: Option<String>,
}

/// Query of `GET /setup/github/callback`: GitHub's redirect.
#[derive(Debug, Default, Deserialize)]
pub struct CallbackQuery {
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
}

/// `POST /api/v1/github/setup-sessions`: open a session and return its link.
pub async fn open_session(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<OpenRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let public_url =
        normalize_public_url(&request.public_url).map_err(ApiError::bad_request)?;
    let org = match request.org.map(|org| org.trim().to_owned()) {
        Some(org) if !org.is_empty() => {
            if !valid_org(&org) {
                return Err(ApiError::bad_request(
                    "org must be a GitHub organization login",
                ));
            }
            Some(org)
        }
        _ => None,
    };
    let app_name = request.app_name.trim().to_owned();
    if app_name.is_empty() {
        return Err(ApiError::bad_request("app_name must not be empty"));
    }
    let opened =
        shared
            .state
            .github_setup
            .open(Instant::now(), public_url.clone(), org, app_name);
    info!(session = %opened.id, "opened a GitHub App setup session");
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id": opened.id,
            "setup_url": format!("{public_url}/setup/github?t={}", opened.token),
            "expires_at": unix_now_secs() + SETUP_LINK_TTL.as_secs(),
        })),
    ))
}

/// `GET /api/v1/github/setup-sessions/{session_id}`: the session's status.
pub async fn poll_session(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let status = shared
        .state
        .github_setup
        .status(Instant::now(), &session_id)
        .ok_or_else(|| ApiError::not_found("no such GitHub App setup session"))?;
    Ok(Json(status.to_json()))
}

/// `GET /setup/github?t=<token>`: serve the App-creation form. Browser-facing;
/// answers 404 unless the token redeems a live link.
///
/// The query is parsed by hand, so a malformed one gets the same 404 as any
/// other refusal rather than an extractor rejection that would reveal the
/// route is live.
pub async fn setup_form(
    State(shared): State<Arc<SharedState>>,
    RawQuery(raw): RawQuery,
) -> Response {
    let now = Instant::now();
    let sessions = &shared.state.github_setup;
    if !sessions.admit_request(now) {
        return refuse(StatusCode::TOO_MANY_REQUESTS);
    }
    let query: LinkQuery = parse_query(raw.as_deref());
    let Some(token) = query.t.filter(|token| !token.is_empty()) else {
        return refuse(StatusCode::NOT_FOUND);
    };
    let Some(grant) = sessions.redeem_link(now, &token) else {
        return refuse(StatusCode::NOT_FOUND);
    };
    let redirect_url = format!("{}/setup/github/callback", grant.public_url);
    let manifest = manifest(&grant.app_name, &redirect_url, Some(&grant.public_url), None);
    let action = creation_url(
        &shared.state.github_urls.server_url,
        grant.org.as_deref(),
        &grant.state,
    );
    browser_page(
        StatusCode::OK,
        form_page(&FormPage {
            name: &grant.app_name,
            org: grant.org.as_deref(),
            action: &action,
            manifest: &manifest,
            public_url: Some(&grant.public_url),
            delivery: DELIVERY_NOTE,
            auto_submit: true,
        }),
    )
}

/// `GET /setup/github/callback?code=&state=`: GitHub's redirect. Converts the
/// code, stores the credentials, and reports the outcome to the poller.
pub async fn setup_callback(
    State(shared): State<Arc<SharedState>>,
    RawQuery(raw): RawQuery,
) -> Response {
    let now = Instant::now();
    if !shared.state.github_setup.admit_request(now) {
        return refuse(StatusCode::TOO_MANY_REQUESTS);
    }
    let query: CallbackQuery = parse_query(raw.as_deref());
    let conversion = match shared.state.github_setup.begin_conversion(
        now,
        query.state.as_deref(),
        query.code.as_deref(),
    ) {
        Ok(conversion) => conversion,
        Err(Refusal::Inactive) => return refuse(StatusCode::NOT_FOUND),
        Err(Refusal::Rejected) => {
            return browser_page(
                StatusCode::BAD_REQUEST,
                error_page(
                    "Setup not accepted",
                    "This address did not come from the App-creation form this engine served.",
                ),
            );
        }
    };
    // The conversion runs as its own task, so it settles the session even if
    // the browser goes away while GitHub is answering.
    match tokio::spawn(complete_conversion(shared.clone(), conversion)).await {
        Ok((status, html)) => browser_page(status, html),
        Err(_) => browser_page(
            StatusCode::INTERNAL_SERVER_ERROR,
            error_page(
                "Setup could not be completed",
                "The engine stopped while finishing the App setup.",
            ),
        ),
    }
}

/// Convert the code, store the App's credentials exactly as the CLI does, and
/// settle the session. Returns the page to show the browser.
async fn complete_conversion(shared: Arc<SharedState>, conversion: Conversion) -> (StatusCode, String) {
    let mut settlement = Settlement {
        sessions: shared.state.github_setup.clone(),
        id: conversion.id.clone(),
        settled: false,
    };
    let api_base = shared.state.github_urls.api_url.clone();
    let credentials = match convert_manifest_code(&api_base, &conversion.code).await {
        Ok(credentials) => credentials,
        Err(error) => {
            warn!("GitHub App manifest conversion failed during engine-hosted setup");
            settlement.settle(Err(format!(
                "GitHub did not return the App credentials: {error:#}"
            )));
            return (
                StatusCode::BAD_GATEWAY,
                error_page(
                    "Setup could not be completed",
                    "GitHub did not return the App credentials.",
                ),
            );
        }
    };

    let app_id = credentials.id;
    let install_url = credentials.install_url();
    let completed = CompletedApp {
        app_id,
        slug: credentials.slug.clone(),
        install_url: install_url.clone(),
        webhook_secret_stored: credentials.webhook_secret.is_some(),
    };
    let config_path = shared.state.config_path.clone();
    let AppCredentials {
        pem,
        webhook_secret,
        ..
    } = credentials;
    // The keychain can block, so the write leaves the async workers.
    let saved = tokio::task::spawn_blocking(move || {
        save_app_credentials_at(&config_path, &app_id.to_string(), &pem, webhook_secret)
    })
    .await;
    match saved {
        Ok(Ok(notices)) => {
            for notice in notices {
                warn!("{notice}");
            }
            info!(app_id, "registered a GitHub App through engine-hosted setup");
            settlement.settle(Ok(completed));
            (StatusCode::OK, done_page(install_url.as_deref()))
        }
        Ok(Err(error)) => {
            warn!("GitHub App credentials could not be saved after engine-hosted conversion");
            settlement.settle(Err(format!(
                "GitHub created App {app_id}, but its credentials could not be saved on the \
                 engine ({error:#}); regenerate a private key under the App's settings to \
                 recover one"
            )));
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_page(
                    "Setup could not be completed",
                    "GitHub created the App, but the engine could not store its credentials.",
                ),
            )
        }
        Err(join_error) => {
            warn!("saving GitHub App credentials was interrupted");
            settlement.settle(Err(format!(
                "GitHub created App {app_id}, but saving its credentials was interrupted \
                 ({join_error}); regenerate a private key under the App's settings to recover one"
            )));
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_page(
                    "Setup could not be completed",
                    "GitHub created the App, but the engine could not store its credentials.",
                ),
            )
        }
    }
}

/// Settles a conversion. Dropped unsettled (the task panicked), it records a
/// failure, so the session never waits in `converting` forever.
struct Settlement {
    sessions: Arc<SetupSessions>,
    id: String,
    settled: bool,
}

impl Settlement {
    fn settle(&mut self, outcome: Result<CompletedApp, String>) {
        self.settled = true;
        self.sessions.finish(&self.id, outcome);
    }
}

impl Drop for Settlement {
    fn drop(&mut self) {
        if !self.settled {
            self.sessions.finish(
                &self.id,
                Err(
                    "the engine stopped before the App setup finished; check the App's settings, then run `preloop setup github --via app` again"
                        .to_owned(),
                ),
            );
        }
    }
}

/// Parse a raw query string into `T`. Anything that does not parse is treated
/// as an empty query, which the handlers refuse with a 404.
fn parse_query<T: Default + serde::de::DeserializeOwned>(raw: Option<&str>) -> T {
    serde_urlencoded::from_str(raw.unwrap_or("")).unwrap_or_default()
}

/// The page shown once the App's credentials are stored.
fn done_page(install_url: Option<&str>) -> String {
    let install = match install_url {
        Some(url) => format!(
            "<p>Install it on your repositories: <a href=\"{url}\">{url}</a></p>",
            url = escape_html(url)
        ),
        None => "<p>Install it on your repositories from the App's settings page.</p>".to_owned(),
    };
    format!(
        r#"<!DOCTYPE html>
<html><head><title>GitHub App created</title></head>
<body style="font-family: system-ui, sans-serif; padding: 40px; max-width: 640px; margin: auto;">
  <h1 style="color: #2da44e;">GitHub App created</h1>
  <p>Its credentials are stored on this engine. Restart the engine (<code>preloop serve</code>) to use them.</p>
  {install}
  <p>You can close this tab.</p>
</body></html>"#
    )
}

/// A browser-facing page. Never cached, and never leaking its URL through
/// `Referer`: the setup link carries the token, and the form posts to GitHub.
fn browser_page(status: StatusCode, html: String) -> Response {
    let mut response = (status, Html(html)).into_response();
    no_cache_headers(response.headers_mut());
    response
}

/// A bare refusal with the same cache rules as [`browser_page`].
fn refuse(status: StatusCode) -> Response {
    let mut response = status.into_response();
    no_cache_headers(response.headers_mut());
    response
}

fn no_cache_headers(headers: &mut HeaderMap) {
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
}

/// The public URL as the engine will use it: an absolute http(s) URL with no
/// credentials, query, or fragment, and no trailing slash.
fn normalize_public_url(raw: &str) -> Result<String, &'static str> {
    let url = reqwest::Url::parse(raw.trim())
        .map_err(|_| "public_url must be an absolute http or https URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err("public_url must be an absolute http or https URL");
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("public_url must not carry credentials, a query, or a fragment");
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

/// A GitHub organization login: up to 39 alphanumerics and hyphens, not
/// starting or ending with a hyphen.
fn valid_org(org: &str) -> bool {
    org.len() <= 39
        && org.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        && !org.starts_with('-')
        && !org.ends_with('-')
}

/// 256 random bits as a URL-safe token.
fn random_token() -> String {
    let bytes: [u8; 32] = rand::random();
    URL_SAFE_NO_PAD.encode(bytes)
}

/// 128 random bits as a session handle.
fn random_id() -> String {
    let bytes: [u8; 16] = rand::random();
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PUBLIC: &str = "https://ci.example.com";

    fn open(sessions: &SetupSessions, now: Instant) -> NewSession {
        sessions.open(now, PUBLIC.to_owned(), None, "preloop-local".to_owned())
    }

    #[test]
    fn the_link_works_once_and_then_the_form_awaits_approval() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        assert_eq!(sessions.status(now, &opened.id), Some(Status::AwaitingLink));

        let grant = sessions
            .redeem_link(now, &opened.token)
            .expect("the first use of the link serves the form");
        assert_eq!(grant.public_url, PUBLIC);
        assert_eq!(grant.app_name, "preloop-local");
        assert!(!grant.state.is_empty());
        assert_eq!(
            sessions.status(now, &opened.id),
            Some(Status::AwaitingApproval)
        );

        assert!(
            sessions.redeem_link(now, &opened.token).is_none(),
            "the token is burned on use"
        );
    }

    #[test]
    fn a_wrong_token_is_refused_and_the_real_one_still_works() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        assert!(sessions.redeem_link(now, "not-the-token").is_none());
        assert!(
            sessions.redeem_link(now, &opened.token).is_some(),
            "one wrong guess does not burn a live link"
        );
    }

    #[test]
    fn five_bad_tokens_burn_the_session() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        for _ in 0..MAX_BAD_ATTEMPTS {
            assert!(sessions.redeem_link(now, "guess").is_none());
        }
        assert!(
            sessions.redeem_link(now, &opened.token).is_none(),
            "the right token is useless once the session is burned"
        );
        assert!(matches!(
            sessions.status(now, &opened.id),
            Some(Status::Failed(_))
        ));
    }

    #[test]
    fn an_unused_link_expires() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        let later = now + SETUP_LINK_TTL;
        assert!(sessions.redeem_link(later, &opened.token).is_none());
        assert_eq!(sessions.status(later, &opened.id), Some(Status::Expired));
    }

    #[test]
    fn approval_expires_after_the_form_is_served() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        let served_at = now + Duration::from_secs(60);
        let grant = sessions.redeem_link(served_at, &opened.token).unwrap();
        let late = served_at + APPROVAL_TTL;
        assert!(
            sessions
                .begin_conversion(late, Some(&grant.state), Some("code"))
                .is_err()
        );
        assert_eq!(sessions.status(late, &opened.id), Some(Status::Expired));
    }

    #[test]
    fn callbacks_are_refused_until_the_form_is_served() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        assert_eq!(
            sessions
                .begin_conversion(now, Some("any"), Some("code"))
                .err(),
            Some(Refusal::Inactive),
            "no session at all"
        );
        let opened = open(&sessions, now);
        assert_eq!(
            sessions
                .begin_conversion(now, Some("any"), Some("code"))
                .err(),
            Some(Refusal::Inactive),
            "a session whose link has not been used yet"
        );
        assert!(sessions.redeem_link(now, &opened.token).is_some());
    }

    #[test]
    fn a_callback_must_echo_the_state_nonce_and_carry_a_code() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        let grant = sessions.redeem_link(now, &opened.token).unwrap();

        assert_eq!(
            sessions
                .begin_conversion(now, Some("forged"), Some("code"))
                .err(),
            Some(Refusal::Rejected)
        );
        assert_eq!(
            sessions.begin_conversion(now, None, Some("code")).err(),
            Some(Refusal::Rejected),
            "a missing state is not a match"
        );
        assert_eq!(
            sessions
                .begin_conversion(now, Some(&grant.state), Some(""))
                .err(),
            Some(Refusal::Rejected),
            "an empty code is refused"
        );

        let conversion = sessions
            .begin_conversion(now, Some(&grant.state), Some("the-code"))
            .expect("the nonce and code match");
        assert_eq!(conversion.id, opened.id);
        assert_eq!(conversion.code, "the-code");
    }

    #[test]
    fn a_consumed_callback_cannot_be_replayed() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        let grant = sessions.redeem_link(now, &opened.token).unwrap();
        sessions
            .begin_conversion(now, Some(&grant.state), Some("code"))
            .unwrap();
        assert_eq!(
            sessions
                .begin_conversion(now, Some(&grant.state), Some("code"))
                .err(),
            Some(Refusal::Inactive),
            "the nonce is consumed by the first redirect"
        );
        assert_eq!(sessions.status(now, &opened.id), Some(Status::Converting));
    }

    #[test]
    fn a_finished_conversion_reports_to_the_poller() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        let grant = sessions.redeem_link(now, &opened.token).unwrap();
        let conversion = sessions
            .begin_conversion(now, Some(&grant.state), Some("code"))
            .unwrap();
        let app = CompletedApp {
            app_id: 987_654,
            slug: Some("preloop-local-app".to_owned()),
            install_url: Some(
                "https://github.com/apps/preloop-local-app/installations/new".to_owned(),
            ),
            webhook_secret_stored: true,
        };
        sessions.finish(&conversion.id, Ok(app.clone()));
        assert_eq!(
            sessions.status(now, &opened.id),
            Some(Status::Completed(app))
        );
    }

    #[test]
    fn finish_ignores_a_session_that_is_not_converting() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        sessions.finish(&opened.id, Err("stale".to_owned()));
        assert_eq!(
            sessions.status(now, &opened.id),
            Some(Status::AwaitingLink),
            "only a converting session takes a result"
        );
    }

    #[test]
    fn a_dropped_settlement_fails_the_session_instead_of_leaving_it_converting() {
        let sessions = Arc::new(SetupSessions::new());
        let now = Instant::now();
        let opened = sessions.open(now, PUBLIC.to_owned(), None, "preloop-local".to_owned());
        let grant = sessions.redeem_link(now, &opened.token).unwrap();
        let conversion = sessions
            .begin_conversion(now, Some(&grant.state), Some("code"))
            .unwrap();
        drop(Settlement {
            sessions: sessions.clone(),
            id: conversion.id.clone(),
            settled: false,
        });
        assert!(matches!(
            sessions.status(now, &opened.id),
            Some(Status::Failed(_))
        ));
    }

    #[test]
    fn opening_a_session_replaces_the_previous_one() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let first = open(&sessions, now);
        let second = open(&sessions, now);
        assert!(sessions.redeem_link(now, &first.token).is_none());
        assert!(sessions.status(now, &first.id).is_none());
        assert!(sessions.redeem_link(now, &second.token).is_some());
    }

    #[test]
    fn requests_are_rate_limited_per_window() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        for _ in 0..RATE_LIMIT {
            assert!(sessions.admit_request(now));
        }
        assert!(!sessions.admit_request(now), "the window is full");
        assert!(
            sessions.admit_request(now + RATE_WINDOW),
            "a new window admits requests again"
        );
    }

    #[test]
    fn public_urls_are_normalized_and_restricted() {
        assert_eq!(
            normalize_public_url(" https://ci.example.com/ ").unwrap(),
            "https://ci.example.com"
        );
        assert_eq!(
            normalize_public_url("http://engine.tailnet.example:9090/prefix/").unwrap(),
            "http://engine.tailnet.example:9090/prefix"
        );
        for refused in [
            "ftp://ci.example.com",
            "ci.example.com",
            "https://ci.example.com/?x=1",
            "https://ci.example.com/#frag",
            "https://user:pw@ci.example.com",
            "https://",
        ] {
            assert!(
                normalize_public_url(refused).is_err(),
                "{refused} must be refused"
            );
        }
    }

    #[test]
    fn org_logins_are_validated() {
        assert!(valid_org("acme"));
        assert!(valid_org("acme-labs-2"));
        assert!(!valid_org("-acme"));
        assert!(!valid_org("acme-"));
        assert!(!valid_org("ac/me"));
        assert!(!valid_org(&"a".repeat(40)));
    }

    #[test]
    fn malformed_queries_read_as_empty() {
        let link: LinkQuery = parse_query(Some("t=abc&extra=1"));
        assert_eq!(link.t.as_deref(), Some("abc"));
        let none: CallbackQuery = parse_query(None);
        assert!(none.code.is_none() && none.state.is_none());
    }

    #[test]
    fn statuses_never_carry_key_material() {
        let app = CompletedApp {
            app_id: 1,
            slug: None,
            install_url: None,
            webhook_secret_stored: false,
        };
        let body = Status::Completed(app).to_json().to_string();
        assert!(!body.contains("pem"), "{body}");
        assert!(
            !body.contains("\"webhook_secret\""),
            "only a flag, never the secret: {body}"
        );
        assert!(body.contains("\"app_id\":1"), "{body}");
    }

    mod http {
        use super::*;
        use crate::state::AppState;
        use axum::body::Body;
        use axum::http::Request;
        use std::net::{Ipv4Addr, SocketAddr};
        use tokio_util::sync::CancellationToken;
        use tower::ServiceExt as _;

        const PEM: &str =
            "-----BEGIN RSA PRIVATE KEY-----\nENGINE-HOSTED\n-----END RSA PRIVATE KEY-----\n";

        /// A stand-in for GitHub's conversion endpoint. Answers with the App's
        /// credentials when the code is `good-code`, and refuses anything else.
        async fn stub_github() -> String {
            let listener = tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .await
                .unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let router = axum::Router::new().route(
                "/app-manifests/{code}/conversions",
                axum::routing::post(|axum::extract::Path(code): axum::extract::Path<String>| async move {
                    if code == "good-code" {
                        (
                            axum::http::StatusCode::CREATED,
                            axum::Json(json!({
                                "id": 555001,
                                "slug": "preloop-local",
                                "pem": PEM,
                                "webhook_secret": "hook-secret",
                                "html_url": "https://github.com/apps/preloop-local",
                            })),
                        )
                            .into_response()
                    } else {
                        (axum::http::StatusCode::UNPROCESSABLE_ENTITY, "code expired").into_response()
                    }
                }),
            );
            tokio::spawn(async move { axum::serve(listener, router).await });
            base
        }

        /// The production router over a fresh engine whose config lives in a
        /// temp file, with GitHub's conversion pointed at `api_base`.
        async fn engine(api_base: &str) -> (axum::Router, AppState, tempfile::TempDir) {
            let temp = tempfile::tempdir().unwrap();
            let config_path = temp.path().join("config.toml");
            std::fs::write(&config_path, "credential_store = \"file\"\n").unwrap();
            let mut state = AppState::new_with_config(temp.path().join("state"), config_path)
                .await
                .unwrap();
            state.github_urls.api_url = api_base.to_owned();
            let router = crate::routes::app(state.clone(), CancellationToken::new());
            (router, state, temp)
        }

        async fn send(app: &axum::Router, request: Request<Body>) -> axum::response::Response {
            app.clone().oneshot(request).await.unwrap()
        }

        async fn text(response: axum::response::Response) -> String {
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        }

        /// The session's status as its CLI poller reads it.
        async fn poll_status(app: &axum::Router, state: &AppState, session: &str) -> Value {
            let response = send(
                app,
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/v1/github/setup-sessions/{session}"))
                    .header("authorization", format!("Bearer {}", state.system_token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
            serde_json::from_str(&text(response).await).unwrap()
        }

        fn get(uri: &str) -> Request<Body> {
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap()
        }

        fn open_request(state: &AppState, body: Value) -> Request<Body> {
            Request::builder()
                .method("POST")
                .uri("/api/v1/github/setup-sessions")
                .header("authorization", format!("Bearer {}", state.system_token))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        }

        /// Open a session through the API and return its id and the path+query
        /// of its setup link, as the browser would request them.
        async fn open_link(app: &axum::Router, state: &AppState) -> (String, String) {
            let response = send(
                app,
                open_request(state, json!({"public_url": PUBLIC, "app_name": "preloop-local"})),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED);
            let body: Value = serde_json::from_str(&text(response).await).unwrap();
            let setup_url = body["setup_url"].as_str().unwrap().to_owned();
            let path = setup_url.strip_prefix(PUBLIC).unwrap().to_owned();
            (body["id"].as_str().unwrap().to_owned(), path)
        }

        fn state_of(form: &str) -> String {
            form.split("apps/new?state=")
                .nth(1)
                .and_then(|rest| rest.split('"').next())
                .expect("the form posts with a state nonce")
                .to_owned()
        }

        #[tokio::test]
        async fn opening_a_session_needs_the_system_token() {
            let (app, state, _temp) = engine("http://127.0.0.1:1").await;
            let anonymous = Request::builder()
                .method("POST")
                .uri("/api/v1/github/setup-sessions")
                .header("content-type", "application/json")
                .body(Body::from(json!({"public_url": PUBLIC}).to_string()))
                .unwrap();
            assert_eq!(send(&app, anonymous).await.status(), StatusCode::UNAUTHORIZED);

            let response = send(&app, open_request(&state, json!({"public_url": "ftp://x"}))).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        #[tokio::test]
        async fn the_browser_routes_are_404_without_a_session() {
            let (app, _state, _temp) = engine("http://127.0.0.1:1").await;
            for uri in [
                "/setup/github?t=anything",
                "/setup/github",
                "/setup/github/callback?code=c&state=s",
                "/setup/github/callback?code=%ZZ",
            ] {
                assert_eq!(
                    send(&app, get(uri)).await.status(),
                    StatusCode::NOT_FOUND,
                    "{uri} must not reveal a route without a session"
                );
            }
        }

        #[tokio::test]
        async fn the_setup_routes_are_rate_limited() {
            let (app, _state, _temp) = engine("http://127.0.0.1:1").await;
            for _ in 0..RATE_LIMIT {
                assert_eq!(
                    send(&app, get("/setup/github?t=x")).await.status(),
                    StatusCode::NOT_FOUND
                );
            }
            let limited = send(&app, get("/setup/github?t=x")).await;
            assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(limited.headers()["cache-control"], "no-store");
        }

        /// The whole engine-hosted flow over HTTP: the link serves the form once,
        /// the callback checks the nonce, GitHub's code is converted, the
        /// credentials land in the config, and the poller sees completion.
        #[tokio::test]
        async fn a_full_setup_stores_the_credentials_and_reports_completion() {
            let api = stub_github().await;
            let (app, state, temp) = engine(&api).await;
            let (session, link) = open_link(&app, &state).await;

            let form = send(&app, get(&link)).await;
            assert_eq!(form.status(), StatusCode::OK);
            assert_eq!(form.headers()["cache-control"], "no-store");
            assert_eq!(form.headers()["referrer-policy"], "no-referrer");
            let form = text(form).await;
            assert!(form.contains("getElementById('manifest-form').submit()"));
            assert!(
                form.contains(&format!(
                    "\"redirect_url\":\"{PUBLIC}/setup/github/callback\""
                )),
                "the manifest redirects to the engine's callback: {form}"
            );
            let state_nonce = state_of(&form);

            assert_eq!(
                send(&app, get(&link)).await.status(),
                StatusCode::NOT_FOUND,
                "the link is burned once the form is served"
            );

            assert_eq!(
                poll_status(&app, &state, &session).await["status"],
                "awaiting_approval"
            );

            let forged = send(
                &app,
                get("/setup/github/callback?code=good-code&state=forged"),
            )
            .await;
            assert_eq!(forged.status(), StatusCode::BAD_REQUEST);

            let callback = send(
                &app,
                get(&format!(
                    "/setup/github/callback?code=good-code&state={state_nonce}"
                )),
            )
            .await;
            assert_eq!(callback.status(), StatusCode::OK);
            assert_eq!(callback.headers()["cache-control"], "no-store");
            let done = text(callback).await;
            assert!(done.contains("GitHub App created"), "{done}");
            assert!(!done.contains("ENGINE-HOSTED"), "the key is never shown: {done}");

            let stored = crate::config::load_config_from(&temp.path().join("config.toml")).unwrap();
            assert_eq!(stored.github.app_id.as_deref(), Some("555001"));
            assert_eq!(stored.github.app_pem(), Some(PEM));
            assert_eq!(stored.github.webhook_secret(), Some("hook-secret"));
            let on_disk = std::fs::read_to_string(temp.path().join("config.toml")).unwrap();
            assert!(!on_disk.contains("ENGINE-HOSTED"), "no inline key: {on_disk}");

            let finished = poll_status(&app, &state, &session).await;
            assert_eq!(finished["status"], "completed");
            assert_eq!(finished["app_id"], 555001);
            assert_eq!(finished["webhook_secret_stored"], true);
            assert!(finished.get("pem").is_none(), "{finished}");

            assert_eq!(
                send(
                    &app,
                    get(&format!(
                        "/setup/github/callback?code=good-code&state={state_nonce}"
                    ))
                )
                .await
                .status(),
                StatusCode::NOT_FOUND,
                "a completed code cannot be converted twice"
            );
        }

        #[tokio::test]
        async fn a_refused_conversion_fails_the_session_and_stores_nothing() {
            let api = stub_github().await;
            let (app, state, temp) = engine(&api).await;
            let (session, link) = open_link(&app, &state).await;
            let form = text(send(&app, get(&link)).await).await;
            let state_nonce = state_of(&form);

            let callback = send(
                &app,
                get(&format!(
                    "/setup/github/callback?code=stale&state={state_nonce}"
                )),
            )
            .await;
            assert_eq!(callback.status(), StatusCode::BAD_GATEWAY);

            let status = poll_status(&app, &state, &session).await;
            assert_eq!(status["status"], "failed");
            assert!(
                status["error"].as_str().unwrap().contains("code expired"),
                "GitHub's reason reaches the CLI: {status}"
            );
            let on_disk = std::fs::read_to_string(temp.path().join("config.toml")).unwrap();
            assert!(
                !on_disk.contains("app_id"),
                "nothing is stored after a refused conversion: {on_disk}"
            );
        }

        #[tokio::test]
        async fn an_unknown_session_polls_as_not_found() {
            let (app, state, _temp) = engine("http://127.0.0.1:1").await;
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/api/v1/github/setup-sessions/nope")
                        .header("authorization", format!("Bearer {}", state.system_token))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
    }
}
