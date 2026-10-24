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
//! live at a time: opening another replaces it and invalidates its link,
//! except while the live one is still converting or is holding a result its
//! poller has not read — replacing that would drop an App GitHub has already
//! created.

use crate::config::load_config_from;
use crate::credential_store::{CredentialStoreKind, CredentialWriter, SecretString, selected_kind};
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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// How long a setup link works after its session opens.
pub const SETUP_LINK_TTL: Duration = Duration::from_secs(10 * 60);

/// How long the operator has to approve the App once the form is served.
pub const APPROVAL_TTL: Duration = Duration::from_secs(10 * 60);

/// Repository checks one setup session may ask the engine to run. Each check
/// mints an installation token and probes GitHub, so the list is bounded like
/// any other operator-supplied input.
const MAX_REPO_CHECKS: usize = 16;

/// How long `--repo` checks wait for the App to be installed on those
/// repositories before checking them anyway.
///
/// The App exists before the operator can install it, and the browser is on
/// GitHub's installation page while the engine finishes converting the code,
/// so a check that ran immediately would report an installation that is simply
/// not there yet. The loopback flow gives the operator's installation the same
/// five minutes before it runs `doctor`.
const INSTALL_WAIT: Duration = Duration::from_secs(300);

/// How often the installation lookup is re-tried while waiting.
const INSTALL_POLL: Duration = Duration::from_secs(3);

/// [`INSTALL_WAIT`], or `PRELOOP_SETUP_INSTALL_WAIT_SECS` when the operator set
/// it — tests use `0` to keep a check from waiting on an installation that
/// will never appear.
fn install_wait() -> Duration {
    std::env::var("PRELOOP_SETUP_INSTALL_WAIT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map_or(INSTALL_WAIT, Duration::from_secs)
}

/// Refused requests one browser-facing setup route answers per
/// [`RATE_WINDOW`], across every client. Only refusals are charged, so the
/// window caps guessing without ever billing the operator's own request, and
/// each route has its own window, so a flood against one cannot spend the
/// other's.
const RATE_LIMIT: u32 = 30;
const RATE_WINDOW: Duration = Duration::from_secs(60);

/// The two browser-facing setup routes. Each has its own refusal budget: the
/// link is what a guesser can try to steal, and the callback is what GitHub
/// answers with once the App exists, so neither may be able to starve the
/// other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetupRoute {
    /// `GET /setup/github`: the App-creation form the setup link opens.
    Form,
    /// `GET /setup/github/callback`: GitHub's redirect after creation.
    Callback,
}

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
    form_window: Window,
    callback_window: Window,
}

impl Table {
    fn window(&mut self, route: SetupRoute) -> &mut Window {
        match route {
            SetupRoute::Form => &mut self.form_window,
            SetupRoute::Callback => &mut self.callback_window,
        }
    }
}

/// One route's refusal budget: how many refused requests it has answered in
/// the window that started at `started`.
#[derive(Default)]
struct Window {
    started: Option<Instant>,
    requests: u32,
}

impl Window {
    /// Charge one refused request. `false` means the window is full, so the
    /// caller answers 429 and the refusal goes unbilled.
    fn charge(&mut self, now: Instant) -> bool {
        let window_open = self
            .started
            .is_some_and(|start| now.duration_since(start) < RATE_WINDOW);
        if !window_open {
            self.started = Some(now);
            self.requests = 0;
        }
        if self.requests >= RATE_LIMIT {
            return false;
        }
        self.requests += 1;
        true
    }
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
    /// Repositories the operator asked to verify once the App exists.
    repos: Vec<String>,
    /// Link expiry while awaiting the link; approval expiry afterwards.
    deadline: Instant,
    phase: Phase,
    /// Whether a poller has read a terminal phase. An unread result is what
    /// stops a second setup from taking this session's place.
    result_read: bool,
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
    /// One entry per repository the operator asked to verify, in the order
    /// asked. Empty when `--repo` was not used.
    pub repo_checks: Vec<RepoCheck>,
}

/// One repository's verification outcome, checked with the App's own freshly
/// stored credentials exactly as `preloop doctor --repo` would.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoCheck {
    pub repo: String,
    pub ok: bool,
    /// Granted permissions on success, or why the check failed.
    pub detail: String,
}

impl RepoCheck {
    /// The line the CLI prints, matching `preloop doctor`'s report.
    pub fn line(&self, app_id: u64) -> String {
        let mark = if self.ok { "✓" } else { "✗" };
        format!("{mark} App {app_id} on {}: {}", self.repo, self.detail)
    }
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
    /// Repositories to verify once the credentials are stored.
    pub repos: Vec<String>,
}

/// Why [`SetupSessions::open`] refused to start a session.
#[derive(Debug, PartialEq, Eq)]
pub enum OpenRefusal {
    /// The live session is converting, or it has finished but no poller has
    /// read its result yet. Its App may already exist on GitHub, so starting
    /// over would strand it: the CLI waiting on that session would report
    /// failure while the engine holds credentials for an App nobody meant to
    /// keep.
    ResultPending,
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

    /// Charge one refused request against `route`'s budget. `false` means the
    /// window is full and the caller should answer 429.
    ///
    /// Charged only for requests the route is refusing, and only after the
    /// token or session check has already failed. The window is what slows
    /// guessing down, and a guess is a refusal; billing the requests
    /// themselves would let a stranger's guesses spend the operator's own
    /// link or callback into a 429.
    pub fn charge_refusal(&self, now: Instant, route: SetupRoute) -> bool {
        self.inner.lock().window(route).charge(now)
    }

    /// Start a session, replacing any earlier one that has nothing pending.
    ///
    /// A session that is converting, or that has reached a terminal phase no
    /// poller has read yet, is not replaced: its App may already exist on
    /// GitHub, and its CLI is still waiting to hear about it.
    pub fn open(
        &self,
        now: Instant,
        public_url: String,
        org: Option<String>,
        app_name: String,
        repos: Vec<String>,
    ) -> Result<NewSession, OpenRefusal> {
        let token = random_token();
        let id = random_id();
        let session = Session {
            id: id.clone(),
            token_digest: Some(digest(&token)),
            state: None,
            public_url,
            org,
            app_name,
            repos,
            deadline: now + SETUP_LINK_TTL,
            phase: Phase::AwaitingLink,
            result_read: false,
        };
        let mut table = self.inner.lock();
        if let Some(current) = table.current.as_ref() {
            let pending = matches!(current.phase, Phase::Converting)
                || (!current.result_read
                    && matches!(current.phase, Phase::Completed(_) | Phase::Failed(_)));
            if pending {
                return Err(OpenRefusal::ResultPending);
            }
        }
        table.current = Some(session);
        Ok(NewSession { id, token })
    }

    /// Redeem the setup link for `token`. A live link with the right token is
    /// burned and returns what the form needs; anything else returns `None`.
    ///
    /// A wrong token does not touch the session. The route that reaches here is
    /// unauthenticated, so anything a guess could change is something an
    /// onlooker could use to end a setup that is in progress — with a 256-bit
    /// token compared in constant time, a guess cannot succeed, and the form's
    /// refusal budget is what caps how fast anyone can try.
    pub fn redeem_link(&self, now: Instant, token: &str) -> Option<FormGrant> {
        let mut table = self.inner.lock();
        let session = table.current.as_mut()?;
        if !matches!(session.phase, Phase::AwaitingLink) || now >= session.deadline {
            return None;
        }
        let stored = session.token_digest?;
        if !constant_time_eq(&stored, &digest(token)) {
            warn!(
                session = %session.id,
                "refused a setup link whose token did not match the live session"
            );
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
            // Like a wrong token, a forged callback is refused without
            // touching the session the operator is still waiting on.
            warn!(
                session = %session.id,
                "refused a setup callback whose state did not match the live session"
            );
            return Err(Refusal::Rejected);
        };
        session.state = None;
        session.phase = Phase::Converting;
        Ok(Conversion {
            id: session.id.clone(),
            code: code.to_owned(),
            repos: session.repos.clone(),
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
    ///
    /// Reading a terminal status releases the session's hold on the single
    /// slot in [`SetupSessions::open`]: the poller that was waiting now has
    /// its answer.
    pub fn status(&self, now: Instant, id: &str) -> Option<Status> {
        let mut table = self.inner.lock();
        let session = table.current.as_mut().filter(|session| session.id == id)?;
        let pending = matches!(session.phase, Phase::AwaitingLink | Phase::AwaitingApproval);
        let status = if pending && now >= session.deadline {
            Status::Expired
        } else {
            match &session.phase {
                Phase::AwaitingLink => Status::AwaitingLink,
                Phase::AwaitingApproval => Status::AwaitingApproval,
                Phase::Converting => Status::Converting,
                Phase::Completed(app) => Status::Completed(app.clone()),
                Phase::Failed(reason) => Status::Failed(reason.clone()),
            }
        };
        if matches!(
            status,
            Status::Completed(_) | Status::Failed(_) | Status::Expired
        ) {
            session.result_read = true;
        }
        Some(status)
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
                "repo_checks": app
                    .repo_checks
                    .iter()
                    .map(|check| json!({
                        "repo": check.repo,
                        "ok": check.ok,
                        "line": check.line(app.app_id),
                    }))
                    .collect::<Vec<_>>(),
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
    /// Create an App even when this engine already has one configured. The
    /// plain command avoids a second App, exactly like the local flow: the
    /// engine holds the config the decision has to be made against.
    #[serde(default)]
    pub add: bool,
    /// Repositories to verify the new App against, once it exists. Each is
    /// an `owner/name` slug, checked with the App's own credentials.
    #[serde(default)]
    pub repos: Vec<String>,
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
    let engine_origin = engine_public_origin().map_err(ApiError::internal)?;
    let public_url = normalize_public_url(&request.public_url, engine_origin.as_ref())
        .map_err(ApiError::bad_request)?;
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
    let repos = validate_repos(&request.repos)?;
    match preflight_config(shared.state.config_path.clone(), request.add)
        .await
        .map_err(ApiError::internal)?
    {
        ConfigPreflight::Ready => {}
        ConfigPreflight::AppConfigured(app_ids) => {
            return Err(ApiError::conflict(format!(
                "this engine already has GitHub App {app_ids} configured; pass --add to create an \
                 additional App, or update the existing App's webhook to {public_url} from a \
                 machine that holds its private key"
            )));
        }
    }
    let opened = shared
        .state
        .github_setup
        .open(Instant::now(), public_url.clone(), org, app_name, repos)
        .map_err(|OpenRefusal::ResultPending| {
            ApiError::conflict(
                "another App setup is still running on this engine; wait for it to report its \
                 result — GitHub may already have created that App — then run the command again",
            )
        })?;
    info!(session = %opened.id, "opened a GitHub App setup session");
    if engine_origin.is_none() {
        // The engine's own idea of where it is published comes from
        // `PRELOOP_PUBLIC_URL`, and nothing a CLI sends can change a running
        // process's environment. Say so once, where it matters: the App's
        // webhook will point at `public_url` while the drift check keeps
        // expecting the engine's own idea of itself, which reads as a broken
        // deployment until the operator sets it.
        warn!(
            public_url = %public_url,
            "this engine has no PRELOOP_PUBLIC_URL; set it to the same address so webhook drift \
             checks and run links agree with the App's webhook"
        );
    }
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id": opened.id,
            "setup_url": format!("{public_url}/setup/github?t={}", opened.token),
            "expires_at": unix_now_secs() + SETUP_LINK_TTL.as_secs(),
        })),
    ))
}

/// What the engine's config says before a setup starts.
enum ConfigPreflight {
    /// Credentials can be stored and this engine has no App yet.
    Ready,
    /// Apps are already configured; their ids go in the operator's refusal.
    AppConfigured(String),
}

/// Answer the two questions a setup must settle before GitHub creates an App:
/// can this engine store the credentials it is about to receive exactly once,
/// and does it already have an App the operator did not mean to duplicate?
///
/// The loopback flow asks the first question before sending the operator to
/// GitHub, because a key that cannot be stored is a key that is lost. The
/// second is the engine's own version of the CLI's local duplicate check,
/// which cannot see a remote engine's config.
///
/// Blocking, and therefore run off the async workers: resolving credentials
/// can wait on the OS keychain.
async fn preflight_config(path: PathBuf, add: bool) -> Result<ConfigPreflight, String> {
    tokio::task::spawn_blocking(move || {
        let config = load_config_from(&path).map_err(|error| format!("{error:#}"))?;
        // The same store selection the save will use, so a store that cannot
        // hold the key is reported before the App exists rather than after.
        //
        // `memory` can hold it, but not past this process: the config would
        // keep a reference no later engine resolves, while the App exists on
        // GitHub with a key nobody has — and a restart is exactly what this
        // flow asks the operator to do next. Refuse before GitHub creates
        // anything, and say why, rather than trade a working setup for an
        // orphaned App.
        if matches!(
            selected_kind(config.credential_store.as_deref())
                .map_err(|error| format!("{error:#}"))?,
            CredentialStoreKind::Memory
        ) {
            return Err(
                "hosted App setup needs a credential store that outlives the engine, but \
                 PRELOOP_CREDENTIAL_STORE=memory keeps the App's key for this process only, so a \
                 restart would leave the App on GitHub with no key this engine could load. Unset \
                 it (or select file/os), then run the command again — nothing has been created on \
                 GitHub yet"
                    .to_owned(),
            );
        }
        CredentialWriter::for_config(&path, config.credential_store.as_deref())
            .map_err(|error| format!("{error:#}"))?;
        if add {
            return Ok(ConfigPreflight::Ready);
        }
        if let Some(app_id) = config
            .github
            .app_id
            .clone()
            .filter(|_| config.github.app_pem().is_some())
        {
            return Ok(ConfigPreflight::AppConfigured(app_id));
        }
        if !config.github.apps.is_empty() {
            let ids = config
                .github
                .apps
                .iter()
                .map(|app| app.app_id.clone())
                .collect::<Vec<_>>()
                .join(", ");
            return Ok(ConfigPreflight::AppConfigured(ids));
        }
        Ok(ConfigPreflight::Ready)
    })
    .await
    .map_err(|error| format!("checking the engine's config was interrupted: {error}"))?
}

/// The repositories to verify: `owner/name` slugs, at most
/// [`MAX_REPO_CHECKS`] of them, each checked once.
fn validate_repos(repos: &[String]) -> Result<Vec<String>, ApiError> {
    if repos.len() > MAX_REPO_CHECKS {
        return Err(ApiError::bad_request(format!(
            "at most {MAX_REPO_CHECKS} repositories can be verified in one setup"
        )));
    }
    let mut checked: Vec<String> = Vec::with_capacity(repos.len());
    for repo in repos {
        let repo = repo.trim().to_owned();
        if !crate::secrets_api::valid_repo(&repo) {
            return Err(ApiError::bad_request(format!(
                "repo `{repo}` must look like owner/name"
            )));
        }
        if !checked.contains(&repo) {
            checked.push(repo);
        }
    }
    Ok(checked)
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
///
/// The token is checked before any budget is spent: a valid link is the
/// operator's own request and is never answered 429 because a stranger spent
/// the form's refusal window guessing.
pub async fn setup_form(
    State(shared): State<Arc<SharedState>>,
    RawQuery(raw): RawQuery,
) -> Response {
    let now = Instant::now();
    let sessions = &shared.state.github_setup;
    let query: LinkQuery = parse_query(raw.as_deref());
    let redeemed = query
        .t
        .filter(|token| !token.is_empty())
        .and_then(|token| sessions.redeem_link(now, &token));
    let Some(grant) = redeemed else {
        return if sessions.charge_refusal(now, SetupRoute::Form) {
            refuse(StatusCode::NOT_FOUND)
        } else {
            refuse(StatusCode::TOO_MANY_REQUESTS)
        };
    };
    let redirect_url = format!("{}/setup/github/callback", grant.public_url);
    let manifest = manifest(
        &grant.app_name,
        &redirect_url,
        Some(&grant.public_url),
        None,
    );
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
///
/// The state is checked before any budget is spent, so GitHub's redirect for
/// the operator's own setup is never answered 429 because a stranger spent
/// the callback's refusal window, and the form's window is a separate one
/// again: guesses against the link cannot spend this route's either.
pub async fn setup_callback(
    State(shared): State<Arc<SharedState>>,
    RawQuery(raw): RawQuery,
) -> Response {
    let now = Instant::now();
    let sessions = &shared.state.github_setup;
    let query: CallbackQuery = parse_query(raw.as_deref());
    let conversion = match sessions.begin_conversion(
        now,
        query.state.as_deref(),
        query.code.as_deref(),
    ) {
        Ok(conversion) => conversion,
        Err(refusal) => {
            if !sessions.charge_refusal(now, SetupRoute::Callback) {
                return refuse(StatusCode::TOO_MANY_REQUESTS);
            }
            return match refusal {
                Refusal::Inactive => refuse(StatusCode::NOT_FOUND),
                Refusal::Rejected => browser_page(
                    StatusCode::BAD_REQUEST,
                    error_page(
                        "Setup not accepted",
                        "This address did not come from the App-creation form this engine served.",
                    ),
                ),
            };
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
async fn complete_conversion(
    shared: Arc<SharedState>,
    conversion: Conversion,
) -> (StatusCode, String) {
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
            // Only the failure's shape reaches the session and the page: the
            // CLI prints this string, and GitHub's rejection text is not the
            // operator's to read. The detail stays in this log.
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

    // A GHES App lives on its own server: the installation page has to come
    // from the same web root the App was created on.
    let install_url = credentials.install_url(&shared.state.github_urls.server_url);
    let webhook_secret_stored = credentials.webhook_secret.is_some();
    let config_path = shared.state.config_path.clone();
    let AppCredentials {
        id: app_id,
        slug,
        pem,
        webhook_secret,
        ..
    } = credentials;
    // `--repo` is verified with the key GitHub just handed over, before it is
    // handed to the credential store.
    let verify_pem = pem.clone();
    // The keychain can block, so the write leaves the async workers.
    let saved = tokio::task::spawn_blocking(move || {
        save_app_credentials_at(&config_path, &app_id.to_string(), &pem, webhook_secret)
    })
    .await;
    let notices = match saved {
        Ok(Ok(notices)) => notices,
        Ok(Err(error)) => {
            warn!("GitHub App credentials could not be saved after engine-hosted conversion");
            settlement.settle(Err(format!(
                "GitHub created App {app_id}, but its credentials could not be saved on the \
                 engine ({error:#}); regenerate a private key under the App's settings to \
                 recover one"
            )));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_page(
                    "Setup could not be completed",
                    "GitHub created the App, but the engine could not store its credentials.",
                ),
            );
        }
        Err(join_error) => {
            warn!("saving GitHub App credentials was interrupted");
            settlement.settle(Err(format!(
                "GitHub created App {app_id}, but saving its credentials was interrupted \
                 ({join_error}); regenerate a private key under the App's settings to recover one"
            )));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_page(
                    "Setup could not be completed",
                    "GitHub created the App, but the engine could not store its credentials.",
                ),
            );
        }
    };
    for notice in notices {
        warn!("{notice}");
    }
    info!(
        app_id,
        "registered a GitHub App through engine-hosted setup"
    );
    let completed = CompletedApp {
        app_id,
        slug,
        install_url,
        webhook_secret_stored,
        repo_checks: Vec::new(),
    };
    if conversion.repos.is_empty() {
        // Nothing to verify: settle now, and report it to the browser.
        let page = done_page(&completed, false);
        settlement.settle(Ok(completed));
        return (StatusCode::OK, page);
    }
    // `--repo` asks for the new credential to be proven against repositories
    // before the command succeeds, and that has to wait for the operator to
    // install the App — but the browser is on GitHub's installation page right
    // now, so the page goes out first. The checks keep the session in
    // `converting` until they settle, which is what the CLI's poller sees.
    let page = done_page(&completed, true);
    let engine = shared.clone();
    let repos = conversion.repos.clone();
    tokio::spawn(async move {
        // This task owns the settlement: if it panics, the session fails
        // rather than waiting in `converting` forever.
        let repo_checks = verify_repos(&engine, app_id, &verify_pem, &repos).await;
        settlement.settle(Ok(CompletedApp {
            repo_checks,
            ..completed
        }));
    });
    (StatusCode::OK, page)
}

/// Check each `owner/name` against the App's own credentials, exactly as
/// `preloop doctor --repo` does on the loopback path.
///
/// Each repository first gets [`INSTALL_WAIT`] — shared across the whole list,
/// so the command's wait does not grow with `--repo` — for the App to be
/// installed; then the credential is probed. A repository the App never
/// reached reports that, rather than a permission error it never had the
/// chance to earn.
///
/// The private key reaches GitHub's JWT and installation-token API here: the
/// protocol boundary that needs the plaintext, and the same one the loopback
/// flow's `doctor` uses.
async fn verify_repos(
    shared: &Arc<SharedState>,
    app_id: u64,
    pem: &SecretString,
    repos: &[String],
) -> Vec<RepoCheck> {
    let api_base = shared.state.github_urls.api_url.clone();
    let app_id_text = app_id.to_string();
    let deadline = Instant::now() + install_wait();
    let mut checks = Vec::with_capacity(repos.len());
    for repo in repos {
        let mut installed = crate::github_app::app_covers_repository_at(
            &api_base,
            &app_id_text,
            pem.expose(),
            repo,
        )
        .await;
        while !installed && Instant::now() < deadline {
            tokio::time::sleep(INSTALL_POLL).await;
            installed = crate::github_app::app_covers_repository_at(
                &api_base,
                &app_id_text,
                pem.expose(),
                repo,
            )
            .await;
        }
        let check = if !installed {
            RepoCheck {
                repo: repo.clone(),
                ok: false,
                detail: format!(
                    "the App is not installed on this repository yet; install it, then run \
                     `preloop doctor --repo {repo}`"
                ),
            }
        } else {
            match crate::github_app::verify_app_config_at(
                &api_base,
                &app_id_text,
                pem.expose(),
                repo,
            )
            .await
            {
                Ok(granted) => RepoCheck {
                    repo: repo.clone(),
                    ok: true,
                    detail: granted
                        .iter()
                        .map(|(permission, access)| format!("{permission}: {access}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                },
                Err(error) => RepoCheck {
                    repo: repo.clone(),
                    ok: false,
                    detail: format!("{error:#}"),
                },
            }
        };
        checks.push(check);
    }
    checks
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
///
/// `checks_pending` says the engine is still waiting for the App to be
/// installed on the repositories `--repo` named: the page has to go out now
/// (the operator is on GitHub's installation page), so it says where the result
/// will land instead of showing checks that have not run.
fn done_page(app: &CompletedApp, checks_pending: bool) -> String {
    let install = match app.install_url.as_deref() {
        Some(url) => format!(
            "<p>Install it on your repositories: <a href=\"{url}\">{url}</a></p>",
            url = escape_html(url)
        ),
        None => "<p>Install it on your repositories from the App's settings page.</p>".to_owned(),
    };
    let checks = if app.repo_checks.is_empty() {
        if checks_pending {
            "<p>The engine is checking the repositories you asked for; the command that started \
             this setup reports each result.</p>"
                .to_owned()
        } else {
            String::new()
        }
    } else {
        let failed = app.repo_checks.iter().any(|check| !check.ok);
        let items = app
            .repo_checks
            .iter()
            .map(|check| format!("<li>{}</li>", escape_html(&check.line(app.app_id))))
            .collect::<Vec<_>>()
            .join("");
        format!(
            "<h2>Repository checks{}</h2><ul>{items}</ul>",
            if failed { " — some failed" } else { "" }
        )
    };
    format!(
        r#"<!DOCTYPE html>
<html><head><title>GitHub App created</title></head>
<body style="font-family: system-ui, sans-serif; padding: 40px; max-width: 640px; margin: auto;">
  <h1 style="color: #2da44e;">GitHub App created</h1>
  <p>Its credentials are stored on this engine. Restart the engine (<code>preloop serve</code>) to use them.</p>
  {install}
  {checks}
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

/// The public URL as the engine will use it: on the engine's own origin when
/// the operator configured one, https unless it is loopback, with no
/// credentials, query, fragment, or trailing slash.
///
/// Two refusals guard the one-time token the setup link carries and the
/// one-time code GitHub sends back with it:
///
/// * An origin other than `PRELOOP_PUBLIC_URL` sends GitHub's redirect — and
///   with it the App's private key — to a host this engine is not reachable
///   at, so anyone holding an API token could direct the credentials to
///   themselves instead of taking a setup over. The operator-declared URL is
///   the only origin the engine can vouch for.
/// * Plain http on a non-loopback host puts the token on the wire, where
///   anyone on the path can redeem it first. Loopback never leaves the
///   machine, so it stays allowed.
fn normalize_public_url(raw: &str, engine_origin: Option<&reqwest::Url>) -> Result<String, String> {
    let url = reqwest::Url::parse(raw.trim())
        .map_err(|_| "public_url must be an absolute http or https URL".to_owned())?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err("public_url must be an absolute http or https URL".to_owned());
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("public_url must not carry credentials, a query, or a fragment".to_owned());
    }
    let loopback = url.host_str().is_some_and(is_loopback_host);
    if url.scheme() == "http" && !loopback {
        return Err(
            "public_url must use https: the one-time setup link would otherwise travel in clear \
             text, and http is only accepted for a loopback address"
                .to_owned(),
        );
    }
    if let Some(engine) = engine_origin
        && (url.scheme() != engine.scheme()
            || url.host_str() != engine.host_str()
            || url.port_or_known_default() != engine.port_or_known_default())
    {
        return Err(format!(
            "public_url must be on this engine's own origin ({origin}); GitHub's redirect carries \
             the App's one-time code and private key, so it is only accepted where the engine is \
             published",
            origin = engine.origin().ascii_serialization()
        ));
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

/// The origin this engine publishes as its own, from `PRELOOP_PUBLIC_URL` —
/// the same value the webhook URL, run links, and drift reports are built
/// from. `None` when the operator has not set one.
///
/// A value that is not an absolute URL is reported rather than ignored:
/// skipping the origin check silently would let a setup redirect anywhere
/// while the operator believes the engine is pinned.
fn engine_public_origin() -> Result<Option<reqwest::Url>, String> {
    let Ok(raw) = std::env::var("PRELOOP_PUBLIC_URL") else {
        return Ok(None);
    };
    let raw = raw.trim().to_owned();
    if raw.is_empty() {
        return Ok(None);
    }
    let url = reqwest::Url::parse(&raw).map_err(|_| {
        "this engine's PRELOOP_PUBLIC_URL is not an absolute URL, so a setup redirect cannot be \
         checked against it; fix or unset it on the engine"
            .to_owned()
    })?;
    Ok(Some(url))
}

/// Whether a URL host is loopback: `localhost`, a `127/8` address, or `::1`
/// (`Url::host_str` keeps an IPv6 host's brackets).
fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_matches(['[', ']']).to_ascii_lowercase();
    host == "localhost"
        || host == "::1"
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|address| address.is_loopback())
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
        sessions
            .open(
                now,
                PUBLIC.to_owned(),
                None,
                "preloop-local".to_owned(),
                Vec::new(),
            )
            .expect("nothing is in flight, so the slot is free")
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
    fn repeated_guesses_do_not_end_the_live_setup() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        // The route that serves the link is unauthenticated: whoever can reach
        // it must not be able to end a setup that is in progress by guessing.
        for _ in 0..50 {
            assert!(sessions.redeem_link(now, "guess").is_none());
        }
        assert_eq!(
            sessions.status(now, &opened.id),
            Some(Status::AwaitingLink),
            "the session is untouched by refused guesses"
        );
        assert!(
            sessions.redeem_link(now, &opened.token).is_some(),
            "the operator's own link still works"
        );
    }

    #[test]
    fn repeated_forged_callbacks_do_not_end_the_live_setup() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        let grant = sessions.redeem_link(now, &opened.token).unwrap();
        for _ in 0..50 {
            assert_eq!(
                sessions
                    .begin_conversion(now, Some("forged"), Some("code"))
                    .err(),
                Some(Refusal::Rejected)
            );
        }
        assert_eq!(
            sessions.status(now, &opened.id),
            Some(Status::AwaitingApproval)
        );
        let conversion = sessions
            .begin_conversion(now, Some(&grant.state), Some("the-code"))
            .expect("the real callback still converts");
        assert_eq!(conversion.id, opened.id);
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
            repo_checks: Vec::new(),
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
    fn a_conversion_in_flight_keeps_its_session() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        let grant = sessions.redeem_link(now, &opened.token).unwrap();
        sessions
            .begin_conversion(now, Some(&grant.state), Some("code"))
            .unwrap();
        assert_eq!(
            sessions
                .open(
                    now,
                    PUBLIC.to_owned(),
                    None,
                    "second".to_owned(),
                    Vec::new()
                )
                .err(),
            Some(OpenRefusal::ResultPending),
            "a second setup must not take the slot an in-flight conversion owns"
        );
        assert_eq!(
            sessions.status(now, &opened.id),
            Some(Status::Converting),
            "the first session is still the live one"
        );
    }

    #[test]
    fn an_unread_result_keeps_its_session_until_a_poller_reads_it() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = open(&sessions, now);
        let grant = sessions.redeem_link(now, &opened.token).unwrap();
        let conversion = sessions
            .begin_conversion(now, Some(&grant.state), Some("code"))
            .unwrap();
        sessions.finish(
            &conversion.id,
            Ok(CompletedApp {
                app_id: 7,
                slug: None,
                install_url: None,
                webhook_secret_stored: false,
                repo_checks: Vec::new(),
            }),
        );
        assert_eq!(
            sessions
                .open(
                    now,
                    PUBLIC.to_owned(),
                    None,
                    "second".to_owned(),
                    Vec::new()
                )
                .err(),
            Some(OpenRefusal::ResultPending),
            "the App may already exist; its result is the waiting CLI's"
        );
        assert!(matches!(
            sessions.status(now, &opened.id),
            Some(Status::Completed(_))
        ));
        assert!(
            sessions
                .open(
                    now,
                    PUBLIC.to_owned(),
                    None,
                    "second".to_owned(),
                    Vec::new()
                )
                .is_ok(),
            "once a poller has read the result, the slot is free"
        );
    }

    #[test]
    fn an_unused_link_is_still_replaced_by_a_new_setup() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let first = open(&sessions, now);
        let second = open(&sessions, now);
        assert!(
            sessions.status(now, &first.id).is_none(),
            "the abandoned link is gone"
        );
        assert_eq!(sessions.status(now, &second.id), Some(Status::AwaitingLink));
    }

    #[test]
    fn requested_repositories_reach_the_conversion() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        let opened = sessions
            .open(
                now,
                PUBLIC.to_owned(),
                None,
                "preloop-local".to_owned(),
                vec!["acme/private".to_owned()],
            )
            .unwrap();
        let grant = sessions.redeem_link(now, &opened.token).unwrap();
        let conversion = sessions
            .begin_conversion(now, Some(&grant.state), Some("code"))
            .unwrap();
        assert_eq!(conversion.repos, vec!["acme/private".to_owned()]);
    }

    #[test]
    fn a_remote_http_public_url_is_refused() {
        let error = normalize_public_url("http://ci.example.com", None).unwrap_err();
        assert!(error.contains("must use https"), "{error}");
        for local in [
            "http://127.0.0.1:9090",
            "http://localhost:9090",
            "http://[::1]:9090",
        ] {
            assert!(
                normalize_public_url(local, None).is_ok(),
                "{local} is loopback, so http never leaves the machine"
            );
        }
        assert!(normalize_public_url("https://ci.example.com", None).is_ok());
    }

    #[test]
    fn a_public_url_off_the_engines_own_origin_is_refused() {
        let engine = reqwest::Url::parse("https://ci.example.com").unwrap();
        assert!(normalize_public_url("https://ci.example.com", Some(&engine)).is_ok());
        assert!(
            normalize_public_url("https://ci.example.com/preloop", Some(&engine)).is_ok(),
            "a path behind the same origin is the operator's reverse proxy"
        );
        assert!(
            normalize_public_url("https://ci.example.com:8443", Some(&engine)).is_err(),
            "a different port is a different origin"
        );
        let error = normalize_public_url("https://evil.example", Some(&engine)).unwrap_err();
        assert!(error.contains("https://ci.example.com"), "{error}");
    }

    #[test]
    fn repository_lists_are_validated_and_deduplicated() {
        assert_eq!(
            validate_repos(&["acme/private".to_owned(), "acme/private".to_owned()]).unwrap(),
            vec!["acme/private".to_owned()]
        );
        assert!(
            validate_repos(&["acme".to_owned()]).is_err(),
            "not owner/name"
        );
        assert!(
            validate_repos(&vec!["acme/private".to_owned(); MAX_REPO_CHECKS + 1]).is_err(),
            "the list is bounded"
        );
    }

    #[test]
    fn a_dropped_settlement_fails_the_session_instead_of_leaving_it_converting() {
        let sessions = Arc::new(SetupSessions::new());
        let now = Instant::now();
        let opened = sessions
            .open(
                now,
                PUBLIC.to_owned(),
                None,
                "preloop-local".to_owned(),
                Vec::new(),
            )
            .expect("nothing is in flight, so the slot is free");
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
    fn refusals_are_rate_limited_per_route_and_window() {
        let sessions = SetupSessions::new();
        let now = Instant::now();
        for _ in 0..RATE_LIMIT {
            assert!(sessions.charge_refusal(now, SetupRoute::Form));
        }
        assert!(
            !sessions.charge_refusal(now, SetupRoute::Form),
            "the form's window is full"
        );
        assert!(
            sessions.charge_refusal(now, SetupRoute::Callback),
            "the callback's window is its own: form refusals do not spend it"
        );
        for _ in 1..RATE_LIMIT {
            assert!(sessions.charge_refusal(now, SetupRoute::Callback));
        }
        assert!(!sessions.charge_refusal(now, SetupRoute::Callback));
        assert!(
            sessions.charge_refusal(now + RATE_WINDOW, SetupRoute::Form),
            "a new window takes refusals again"
        );
    }

    #[test]
    fn public_urls_are_normalized_and_restricted() {
        assert_eq!(
            normalize_public_url(" https://ci.example.com/ ", None).unwrap(),
            "https://ci.example.com"
        );
        assert_eq!(
            normalize_public_url("http://127.0.0.1:9090/prefix/", None).unwrap(),
            "http://127.0.0.1:9090/prefix",
            "loopback http keeps its path and loses only the trailing slash"
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
                normalize_public_url(refused, None).is_err(),
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
            repo_checks: vec![RepoCheck {
                repo: "acme/private".to_owned(),
                ok: false,
                detail: "installation not found".to_owned(),
            }],
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
            let listener =
                tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                    .await
                    .unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let router = axum::Router::new().route(
                "/app-manifests/{code}/conversions",
                axum::routing::post(
                    |axum::extract::Path(code): axum::extract::Path<String>| async move {
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
                            (axum::http::StatusCode::UNPROCESSABLE_ENTITY, "code expired")
                                .into_response()
                        }
                    },
                ),
            );
            tokio::spawn(async move { axum::serve(listener, router).await });
            base
        }

        /// The production router over a fresh engine whose config lives in a
        /// temp file, with GitHub's conversion pointed at `api_base`.
        async fn engine(api_base: &str) -> (axum::Router, AppState, tempfile::TempDir) {
            engine_with_config(api_base, "credential_store = \"file\"\n").await
        }

        /// [`engine`] with the config file's text chosen by the caller, for the
        /// cases that depend on what the engine already has configured.
        async fn engine_with_config(
            api_base: &str,
            config_text: &str,
        ) -> (axum::Router, AppState, tempfile::TempDir) {
            let temp = tempfile::tempdir().unwrap();
            let config_path = temp.path().join("config.toml");
            std::fs::write(&config_path, config_text).unwrap();
            let mut state = AppState::new_with_config(temp.path().join("state"), config_path)
                .await
                .unwrap();
            state.github_urls.api_url = api_base.to_owned();
            let router = crate::routes::app(state.clone(), CancellationToken::new());
            (router, state, temp)
        }

        /// Pin the two process-globals this route reads — the engine's own
        /// `PRELOOP_PUBLIC_URL` and the `--repo` install wait — the way every
        /// other test that touches the environment does: under
        /// `GITHUB_ENV_LOCK`, for the whole test. The wait is zero so a check
        /// against a stub that will never grow an installation does not sit
        /// out its five minutes.
        async fn pin_setup_env(
            public_url: Option<&'static str>,
        ) -> (
            (crate::state::TestEnvVar, crate::state::TestEnvVar),
            tokio::sync::MutexGuard<'static, ()>,
        ) {
            let guard = crate::state::GITHUB_ENV_LOCK.lock().await;
            let public = match public_url {
                Some(value) => crate::state::TestEnvVar::set("PRELOOP_PUBLIC_URL", value),
                None => crate::state::TestEnvVar::unset("PRELOOP_PUBLIC_URL"),
            };
            let wait = crate::state::TestEnvVar::set("PRELOOP_SETUP_INSTALL_WAIT_SECS", "0");
            ((public, wait), guard)
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
                open_request(
                    state,
                    json!({
                        "public_url": PUBLIC,
                        "app_name": "preloop-local",
                        // The stub only answers the conversion endpoint, so
                        // this check has to fail: what is pinned is that the
                        // engine runs it and reports the outcome.
                        "repos": ["acme/private"],
                    }),
                ),
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
            let _env = pin_setup_env(None).await;
            let (app, state, _temp) = engine("http://127.0.0.1:1").await;
            let anonymous = Request::builder()
                .method("POST")
                .uri("/api/v1/github/setup-sessions")
                .header("content-type", "application/json")
                .body(Body::from(json!({"public_url": PUBLIC}).to_string()))
                .unwrap();
            assert_eq!(
                send(&app, anonymous).await.status(),
                StatusCode::UNAUTHORIZED
            );

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

            // The callback bursts the same way on its own window, and an
            // inactive session reads as 404 until that window fills.
            for _ in 0..RATE_LIMIT {
                assert_eq!(
                    send(&app, get("/setup/github/callback?code=c&state=s"))
                        .await
                        .status(),
                    StatusCode::NOT_FOUND,
                    "no session awaits the callback"
                );
            }
            let limited = send(&app, get("/setup/github/callback?code=c&state=s")).await;
            assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(limited.headers()["cache-control"], "no-store");
        }

        /// The windows bill refusals, not requests: a stranger can spend the
        /// form's whole window guessing and the operator's own link still
        /// opens the form.
        #[tokio::test]
        async fn a_spent_refusal_window_never_gates_the_operators_link() {
            let _env = pin_setup_env(None).await;
            let api = stub_github().await;
            let (app, state, _temp) = engine(&api).await;
            let (_session, link) = open_link(&app, &state).await;

            for _ in 0..RATE_LIMIT {
                assert_eq!(
                    send(&app, get("/setup/github?t=x")).await.status(),
                    StatusCode::NOT_FOUND
                );
            }
            assert_eq!(
                send(&app, get("/setup/github?t=x")).await.status(),
                StatusCode::TOO_MANY_REQUESTS,
                "the form's window is full of refusals"
            );

            let form = send(&app, get(&link)).await;
            assert_eq!(
                form.status(),
                StatusCode::OK,
                "the operator's own link is served, not gated by the refusals"
            );
            let form = text(form).await;
            assert!(form.contains("apps/new?state="), "{form}");
        }

        /// Each route refuses on its own window: guesses against the link
        /// cannot spend the callback's, and a spent callback window never
        /// refuses GitHub's redirect for the live setup.
        #[tokio::test]
        async fn each_route_refuses_on_its_own_window() {
            let _env = pin_setup_env(None).await;
            let api = stub_github().await;
            let (app, state, _temp) = engine(&api).await;
            let (session, link) = open_link(&app, &state).await;
            let form = text(send(&app, get(&link)).await).await;
            let state_nonce = state_of(&form);

            // Spend the form's window on guesses.
            for _ in 0..RATE_LIMIT {
                assert_eq!(
                    send(&app, get("/setup/github?t=x")).await.status(),
                    StatusCode::NOT_FOUND
                );
            }
            assert_eq!(
                send(&app, get("/setup/github?t=x")).await.status(),
                StatusCode::TOO_MANY_REQUESTS
            );

            // The callback has a window of its own, full only once it has
            // answered a burst itself.
            let forged = "/setup/github/callback?code=good-code&state=forged";
            for _ in 0..RATE_LIMIT {
                assert_eq!(
                    send(&app, get(forged)).await.status(),
                    StatusCode::BAD_REQUEST,
                    "a forged callback is refused on the callback's own window"
                );
            }
            assert_eq!(
                send(&app, get(forged)).await.status(),
                StatusCode::TOO_MANY_REQUESTS,
                "the callback's window fills like the form's"
            );
            assert_eq!(
                send(&app, get("/setup/github?t=x")).await.status(),
                StatusCode::TOO_MANY_REQUESTS,
                "neither route resets the other's window"
            );

            let callback = send(
                &app,
                get(&format!(
                    "/setup/github/callback?code=good-code&state={state_nonce}"
                )),
            )
            .await;
            assert_eq!(
                callback.status(),
                StatusCode::OK,
                "a spent refusal window never refuses the real callback"
            );
            let done = text(callback).await;
            assert!(done.contains("GitHub App created"), "{done}");
            assert_eq!(
                poll_status(&app, &state, &session).await["status"],
                "completed"
            );
        }

        /// The whole engine-hosted flow over HTTP: the link serves the form once,
        /// the callback checks the nonce, GitHub's code is converted, the
        /// credentials land in the config, and the poller sees completion.
        #[tokio::test]
        async fn a_full_setup_stores_the_credentials_and_reports_completion() {
            let _env = pin_setup_env(None).await;
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
            assert!(
                !done.contains("ENGINE-HOSTED"),
                "the key is never shown: {done}"
            );

            let stored = crate::config::load_config_from(&temp.path().join("config.toml")).unwrap();
            assert_eq!(stored.github.app_id.as_deref(), Some("555001"));
            assert_eq!(stored.github.app_pem(), Some(PEM));
            assert_eq!(stored.github.webhook_secret(), Some("hook-secret"));
            let on_disk = std::fs::read_to_string(temp.path().join("config.toml")).unwrap();
            assert!(
                !on_disk.contains("ENGINE-HOSTED"),
                "no inline key: {on_disk}"
            );

            let finished = poll_status(&app, &state, &session).await;
            assert_eq!(finished["status"], "completed");
            assert_eq!(finished["app_id"], 555001);
            assert_eq!(finished["webhook_secret_stored"], true);
            assert!(finished.get("pem").is_none(), "{finished}");
            assert_eq!(finished["repo_checks"][0]["repo"], "acme/private");
            assert_eq!(finished["repo_checks"][0]["ok"], false);
            assert!(
                finished["repo_checks"][0]["line"]
                    .as_str()
                    .unwrap()
                    .starts_with("✗ App 555001 on acme/private:"),
                "{finished}"
            );
            assert!(
                done.contains("checking the repositories you asked for"),
                "the page goes out before the install wait, so it says the result is coming: {done}"
            );

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
            let _env = pin_setup_env(None).await;
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
            let error = status["error"].as_str().unwrap();
            assert!(
                error.contains("422 Unprocessable Entity"),
                "the poller learns GitHub refused, with the status: {status}"
            );
            assert!(
                !error.contains("code expired"),
                "GitHub's rejection text stays in the engine log: {status}"
            );
            let on_disk = std::fs::read_to_string(temp.path().join("config.toml")).unwrap();
            assert!(
                !on_disk.contains("app_id"),
                "nothing is stored after a refused conversion: {on_disk}"
            );
        }

        #[tokio::test]
        async fn a_second_app_is_refused_unless_add_is_asked_for() {
            let _env = pin_setup_env(None).await;
            let (app, state, _temp) = engine_with_config(
                "http://127.0.0.1:1",
                "credential_store = \"file\"\n\n[[github.apps]]\napp_id = \"4242\"\n",
            )
            .await;
            let refused = send(&app, open_request(&state, json!({"public_url": PUBLIC}))).await;
            assert_eq!(
                refused.status(),
                StatusCode::CONFLICT,
                "the engine's own config decides, not the CLI's"
            );
            let body = text(refused).await;
            assert!(body.contains("4242"), "{body}");
            assert!(body.contains("--add"), "{body}");

            let allowed = send(
                &app,
                open_request(&state, json!({"public_url": PUBLIC, "add": true})),
            )
            .await;
            assert_eq!(
                allowed.status(),
                StatusCode::CREATED,
                "--add still creates an additional App"
            );
        }

        #[tokio::test]
        async fn a_process_lifetime_credential_store_is_refused_by_the_hosted_flow() {
            let _env = pin_setup_env(None).await;
            // The env lock `pin_setup_env` holds is what makes this safe: the
            // store selection is process-wide, so no other test can resolve a
            // store while this one has `memory` selected.
            let _store = crate::state::TestEnvVar::set("PRELOOP_CREDENTIAL_STORE", "memory");
            let (app, state, _temp) = engine("http://127.0.0.1:1").await;

            let refused = send(&app, open_request(&state, json!({"public_url": PUBLIC}))).await;
            assert_eq!(
                refused.status(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "a store that forgets the key at the restart the flow asks for is refused"
            );
            let body = text(refused).await;
            assert!(body.contains("PRELOOP_CREDENTIAL_STORE=memory"), "{body}");
            assert!(
                body.contains("nothing has been created on GitHub yet"),
                "the refusal has to say GitHub has not been told anything: {body}"
            );

            // The same request succeeds on a store that persists.
            {
                let _unset = crate::state::TestEnvVar::unset("PRELOOP_CREDENTIAL_STORE");
                let allowed = send(&app, open_request(&state, json!({"public_url": PUBLIC}))).await;
                assert_eq!(allowed.status(), StatusCode::CREATED);
            }
        }

        #[tokio::test]
        async fn a_setup_for_another_origin_is_refused() {
            let _env = pin_setup_env(Some(PUBLIC)).await;
            let (app, state, _temp) = engine("http://127.0.0.1:1").await;
            let elsewhere = send(
                &app,
                open_request(&state, json!({"public_url": "https://attacker.example"})),
            )
            .await;
            assert_eq!(elsewhere.status(), StatusCode::BAD_REQUEST);
            let body = text(elsewhere).await;
            assert!(body.contains(PUBLIC), "{body}");

            let own = send(&app, open_request(&state, json!({"public_url": PUBLIC}))).await;
            assert_eq!(own.status(), StatusCode::CREATED);
        }

        #[tokio::test]
        async fn a_non_loopback_http_setup_is_refused() {
            let _env = pin_setup_env(None).await;
            let (app, state, _temp) = engine("http://127.0.0.1:1").await;
            let refused = send(
                &app,
                open_request(&state, json!({"public_url": "http://ci.example.com"})),
            )
            .await;
            assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
            assert!(text(refused).await.contains("https"));
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
