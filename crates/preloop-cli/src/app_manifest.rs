//! Browser-driven GitHub App creation (`preloop setup github --via app`).
//!
//! GitHub's App-manifest flow turns App creation into a redirect dance: a
//! form POST hands GitHub a manifest, the operator clicks *Create*, and
//! GitHub redirects back with a one-time code that converts into the App id,
//! private key, and webhook secret. Doing that by hand means creating the App
//! in the web UI, downloading a PEM, and re-running the CLI with two flags —
//! four steps in which the only interesting decision is "yes, create it".
//!
//! This module collapses that into one command by binding a single-use HTTP
//! listener on loopback and using it as the manifest's `redirect_url`. The
//! listener is the security boundary: the private key never leaves the
//! machine (unlike the server-side `/api/v1/github/register` page, which can
//! only display it), and the callback is reachable only from this host.
//!
//! Three routes, one shot each:
//!
//! | Route        | Purpose                                                   |
//! |--------------|-----------------------------------------------------------|
//! | `/`          | auto-submitting form POST to GitHub's App-creation page    |
//! | `/callback`  | manifest code → credentials, then redirect to the install  |
//! | `/installed` | GitHub's post-install redirect; reports the installation   |

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse, Redirect};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

/// Default permissions of the generated App.
///
/// The floor a workflow needs to check out code and report status: GitHub
/// always grants `metadata: read`, `contents: read` backs `actions/checkout`,
/// `pull_requests: read` backs PR-triggered runs, and `checks: write` lets
/// the engine publish check runs. Anything beyond that is a decision the
/// operator should make in the App's settings, not one this flow makes for
/// them — an installation token is capped by the App's grant, so a narrow
/// default cannot silently widen a job's authority.
const DEFAULT_PERMISSIONS: &[(&str, &str)] = &[
    ("checks", "write"),
    ("contents", "read"),
    ("metadata", "read"),
    ("pull_requests", "read"),
];

/// Events the App subscribes to when webhooks are configured.
const DEFAULT_EVENTS: &[&str] = &["push", "pull_request"];

/// How long to wait for the operator to finish in the browser.
const BROWSER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// How long to wait for the App installation after the App itself exists.
///
/// Shorter than [`BROWSER_TIMEOUT`]: by this point the credentials are
/// already written, so a timeout costs the operator a printed URL, not the
/// setup.
const INSTALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Credentials GitHub returns from a manifest conversion.
#[derive(Deserialize)]
pub(crate) struct AppCredentials {
    /// Numeric App id (the JWT `iss`).
    pub id: u64,
    /// URL slug, for the installation link.
    #[serde(default)]
    pub slug: Option<String>,
    /// PKCS#1 private key.
    pub pem: String,
    /// `X-Hub-Signature-256` secret; absent when the manifest disabled hooks.
    #[serde(default)]
    pub webhook_secret: Option<String>,
    /// Human-facing App page.
    #[serde(default)]
    pub html_url: Option<String>,
}

/// Manual `Debug`: this carries the App's private key and webhook secret,
/// either of which a single `debug!(?credentials)` would otherwise disclose.
impl std::fmt::Debug for AppCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppCredentials")
            .field("id", &self.id)
            .field("slug", &self.slug)
            .field("pem", &"<redacted>")
            .field(
                "webhook_secret",
                &self.webhook_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("html_url", &self.html_url)
            .finish()
    }
}

impl AppCredentials {
    /// Where the operator installs the App on their repositories.
    ///
    /// Prefers the slug-based path, which lands directly on the repository
    /// picker; falls back to the App page when GitHub omitted the slug.
    pub(crate) fn install_url(&self) -> Option<String> {
        self.slug
            .as_ref()
            .map(|slug| format!("https://github.com/apps/{slug}/installations/new"))
            .or_else(|| self.html_url.clone())
    }
}

/// Everything the flow learned, in order of arrival.
#[derive(Debug)]
pub(crate) struct Registration {
    pub credentials: AppCredentials,
    /// Installation id, when the operator completed the install before
    /// [`INSTALL_TIMEOUT`]. The engine discovers it per repository anyway,
    /// so its absence is not a failure.
    pub installation_id: Option<u64>,
}

/// Where the App is created: a personal account or an organization.
pub(crate) fn creation_url(org: Option<&str>, state: &str) -> String {
    match org {
        Some(org) => format!(
            "https://github.com/organizations/{org}/settings/apps/new?state={state}",
            org = urlencode(org),
            state = urlencode(state),
        ),
        None => format!(
            "https://github.com/settings/apps/new?state={state}",
            state = urlencode(state)
        ),
    }
}

/// The manifest GitHub renders as a pre-filled App-creation form.
///
/// `hook_attributes` is only meaningful when GitHub can reach the engine, so
/// a loopback-only setup ships the App with no webhook configured rather
/// than pointing one at an address that will never resolve from GitHub's
/// side. GitHub rejects any `hook_attributes.url` it cannot reach over the
/// public Internet — including an `active: false` one — so the field is
/// omitted entirely.
pub(crate) fn manifest(
    name: &str,
    redirect_url: &str,
    public_url: Option<&str>,
) -> serde_json::Value {
    let permissions: serde_json::Map<String, serde_json::Value> = DEFAULT_PERMISSIONS
        .iter()
        .map(|(scope, access)| ((*scope).to_owned(), serde_json::Value::from(*access)))
        .collect();
    let mut manifest = serde_json::json!({
        "name": name,
        "url": public_url.unwrap_or("https://github.com/preloop-dev/preloop"),
        "redirect_url": redirect_url,
        "setup_url": format!("{}/installed", redirect_url.trim_end_matches("/callback")),
        "setup_on_update": false,
        "public": false,
        "default_permissions": permissions,
    });
    match public_url {
        Some(public) => {
            manifest["hook_attributes"] = serde_json::json!({
                "url": format!("{}/api/v1/github/webhooks", public.trim_end_matches('/')),
                "active": true,
            });
            manifest["default_events"] = serde_json::json!(DEFAULT_EVENTS);
        }
        // No webhook at all: GitHub validates hook URL reachability even for
        // inactive hooks and rejects loopback addresses. Omitting the object
        // means no `webhook_secret` is minted, so a later `--public-url`
        // has to add the secret when enabling the hook in App settings.
        None => {}
    }
    manifest
}

/// Shared state of the one-shot listener.
struct Flow {
    /// CSRF nonce echoed by GitHub; a callback carrying anything else did not
    /// originate from the form this process served.
    state: String,
    name: String,
    public_url: Option<String>,
    redirect_url: String,
    api_base: String,
    /// Install URL, published by the callback for the `/installed` page.
    install_url: Mutex<Option<String>>,
    /// Fires once, with the converted credentials.
    credentials_tx: Mutex<Option<oneshot::Sender<Result<AppCredentials>>>>,
    /// Fires once, with the installation id GitHub redirects back with.
    installed_tx: Mutex<Option<oneshot::Sender<u64>>>,
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    code: String,
    #[serde(default)]
    state: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InstalledQuery {
    #[serde(default)]
    installation_id: Option<u64>,
}

/// Run the browser flow to completion.
///
/// Returns once GitHub has converted the manifest; the installation step is
/// best-effort and bounded by [`INSTALL_TIMEOUT`].
pub(crate) async fn register(
    name: &str,
    org: Option<&str>,
    port: u16,
    public_url: Option<&str>,
    open_browser: bool,
) -> Result<Registration> {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
        .await
        .with_context(|| format!("binding 127.0.0.1:{port} for the GitHub redirect"))?;
    let local = listener
        .local_addr()
        .context("reading the local listener address")?;
    let base = format!("http://127.0.0.1:{}", local.port());
    println!("Open this URL to create the GitHub App:\n  {base}");
    if open_browser && open_in_browser(&base).is_err() {
        println!("(could not open a browser automatically — open the URL above)");
    }
    println!("Waiting for GitHub to redirect back…");
    let api_base = std::env::var("PRELOOP_GITHUB_API_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_owned());
    register_on(listener, name, org, public_url, &api_base).await
}

/// [`register`] against an already-bound listener, so a test can learn the
/// address before the flow starts.
async fn register_on(
    listener: tokio::net::TcpListener,
    name: &str,
    org: Option<&str>,
    public_url: Option<&str>,
    api_base: &str,
) -> Result<Registration> {
    let local = listener
        .local_addr()
        .context("reading the local listener address")?;
    let base = format!("http://127.0.0.1:{}", local.port());
    let (credentials_tx, credentials_rx) = oneshot::channel();
    let (installed_tx, installed_rx) = oneshot::channel();
    let flow = Arc::new(Flow {
        state: nonce(),
        name: name.to_owned(),
        public_url: public_url.map(str::to_owned),
        redirect_url: format!("{base}/callback"),
        api_base: api_base.to_owned(),
        install_url: Mutex::new(None),
        credentials_tx: Mutex::new(Some(credentials_tx)),
        installed_tx: Mutex::new(Some(installed_tx)),
    });
    let org = org.map(str::to_owned);
    let router = Router::new()
        .route("/", get(page_form))
        .route("/callback", get(page_callback))
        .route("/installed", get(page_installed))
        .with_state((flow.clone(), org));

    let shutdown = tokio_util::sync::CancellationToken::new();
    let server = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await
        }
    });

    let credentials = match tokio::time::timeout(BROWSER_TIMEOUT, credentials_rx).await {
        Ok(Ok(result)) => result?,
        Ok(Err(_)) => anyhow::bail!("the registration listener stopped before GitHub replied"),
        Err(_) => anyhow::bail!(
            "timed out after {}s waiting for GitHub — re-run `preloop setup github --via app`",
            BROWSER_TIMEOUT.as_secs()
        ),
    };

    let installation_id = match tokio::time::timeout(INSTALL_TIMEOUT, installed_rx).await {
        Ok(Ok(id)) => Some(id),
        _ => None,
    };
    shutdown.cancel();
    let _ = server.await;
    Ok(Registration {
        credentials,
        installation_id,
    })
}

/// The form page: GitHub only accepts a manifest as a form POST, so this is
/// a self-submitting form rather than a link.
async fn page_form(State((flow, org)): State<(Arc<Flow>, Option<String>)>) -> impl IntoResponse {
    let manifest = manifest(&flow.name, &flow.redirect_url, flow.public_url.as_deref());
    let action = creation_url(org.as_deref(), &flow.state);
    let scope = match &org {
        Some(org) => format!("the <strong>{}</strong> organization", escape(org)),
        None => "your personal account".to_owned(),
    };
    let webhooks = match &flow.public_url {
        Some(public) => format!("Webhooks deliver to {}.", escape(public)),
        None => "Webhooks are off — this engine is not reachable from GitHub.".to_owned(),
    };
    Html(format!(
        r#"<!DOCTYPE html>
<html>
<head><title>Create the preloop GitHub App</title></head>
<body style="font-family: system-ui, sans-serif; padding: 40px; max-width: 640px; margin: auto;">
  <h1>Create the preloop GitHub App</h1>
  <p>This creates a private App named <strong>{name}</strong> on {scope}, with
     permissions {permissions}. {webhooks}</p>
  <p>GitHub returns the credentials to this page, which writes them to your
     local config — the private key never leaves this machine.</p>
  <form id="manifest-form" action="{action}" method="post">
    <input type="hidden" name="manifest" value='{manifest}'>
    <button type="submit" style="font-size: 16px; padding: 10px 20px; cursor: pointer; background: #2da44e; color: white; border: none; border-radius: 6px; font-weight: bold;">Create on GitHub</button>
  </form>
</body>
</html>"#,
        name = escape(&flow.name),
        scope = scope,
        permissions = escape(
            &DEFAULT_PERMISSIONS
                .iter()
                .map(|(scope, access)| format!("{scope}: {access}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        webhooks = webhooks,
        action = escape(&action),
        manifest = escape_attr(&manifest.to_string()),
    ))
}

/// GitHub's redirect target: converts the one-time code, then sends the
/// browser on to the installation page so the operator never copies a URL.
async fn page_callback(
    State((flow, _)): State<(Arc<Flow>, Option<String>)>,
    Query(query): Query<CallbackQuery>,
) -> axum::response::Response {
    if query.state.as_deref() != Some(flow.state.as_str()) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Html(error_page(
                "State mismatch",
                "This callback did not come from the form this command served.",
            )),
        )
            .into_response();
    }
    let converted = convert(&flow.api_base, &query.code).await;
    let install_url = converted
        .as_ref()
        .ok()
        .and_then(AppCredentials::install_url);
    let response = match &converted {
        Ok(_) => match &install_url {
            Some(url) => {
                *flow.install_url.lock().await = Some(url.clone());
                Redirect::to(url).into_response()
            }
            None => Html(success_page(
                "App created",
                "Install it on your repositories from the App's settings page.",
            ))
            .into_response(),
        },
        Err(error) => (
            axum::http::StatusCode::BAD_GATEWAY,
            Html(error_page("Conversion failed", &format!("{error:#}"))),
        )
            .into_response(),
    };
    if let Some(tx) = flow.credentials_tx.lock().await.take() {
        let _ = tx.send(converted);
    }
    response
}

/// GitHub's post-install redirect (`setup_url`).
async fn page_installed(
    State((flow, _)): State<(Arc<Flow>, Option<String>)>,
    Query(query): Query<InstalledQuery>,
) -> impl IntoResponse {
    if let Some(id) = query.installation_id {
        if let Some(tx) = flow.installed_tx.lock().await.take() {
            let _ = tx.send(id);
        }
    }
    Html(success_page(
        "preloop is connected",
        "Credentials are written to your local config. You can close this tab and return to the terminal.",
    ))
}

/// Exchange the one-time manifest code for the App's credentials.
async fn convert(api_base: &str, code: &str) -> Result<AppCredentials> {
    let url = format!("{}/app-manifests/{}/conversions", api_base, urlencode(code));
    // Not `crate::build_client()`: that one is pinned to the engine's unix
    // socket, and this call goes to api.github.com.
    let response = reqwest::Client::new()
        .post(&url)
        .header("User-Agent", "preloop")
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .context("calling GitHub's manifest conversion endpoint")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    anyhow::ensure!(
        status.is_success(),
        "GitHub rejected the manifest conversion ({status}): {}",
        body.trim()
    );
    serde_json::from_str(&body).context("parsing GitHub's manifest conversion response")
}

/// Open `url` in the platform browser. Failure is not fatal — the URL is
/// always printed first.
pub(crate) fn open_in_browser(url: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    let (program, args): (&str, Vec<&str>) = ("open", vec![url]);
    #[cfg(target_os = "windows")]
    let (program, args): (&str, Vec<&str>) = ("cmd", vec!["/C", "start", "", url]);
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let (program, args): (&str, Vec<&str>) = ("xdg-open", vec![url]);

    let status = std::process::Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .with_context(|| format!("spawning {program}"))?;
    anyhow::ensure!(status.success(), "{program} exited with {status}");
    Ok(())
}

/// 128 bits of CSRF nonce, hex-encoded.
fn nonce() -> String {
    let bytes: [u8; 16] = rand::random();
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Percent-encode everything outside the unreserved set, so a slug or nonce
/// cannot break out of the query string it is spliced into.
fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Escape HTML text content.
fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Escape a value destined for a single-quoted HTML attribute. The manifest
/// is JSON, so it is full of double quotes: the attribute is single-quoted
/// and only the single quote (and `&`/`<`) needs escaping.
fn escape_attr(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('\'', "&#39;")
}

fn success_page(title: &str, detail: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html><head><title>{title}</title></head>
<body style="font-family: system-ui, sans-serif; padding: 40px; max-width: 640px; margin: auto;">
  <h1 style="color: #2da44e;">{title}</h1>
  <p>{detail}</p>
</body></html>"#,
        title = escape(title),
        detail = escape(detail)
    )
}

fn error_page(title: &str, detail: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html><head><title>{title}</title></head>
<body style="font-family: system-ui, sans-serif; padding: 40px; max-width: 640px; margin: auto;">
  <h1 style="color: #cf222e;">{title}</h1>
  <p>{detail}</p>
  <p>Re-run <code>preloop setup github --via app</code> to try again.</p>
</body></html>"#,
        title = escape(title),
        detail = escape(detail)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in for `api.github.com`'s manifest-conversion endpoint.
    ///
    /// Returns the address it listens on; the caller passes it to
    /// [`register_on`] as the API base.
    async fn mock_github() -> String {
        let listener = tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .unwrap();
        let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let router = Router::new().route(
            "/app-manifests/:code/conversions",
            axum::routing::post(|axum::extract::Path(code): axum::extract::Path<String>| async move {
                assert_eq!(code, "conversion-code", "the code from GitHub is forwarded verbatim");
                axum::Json(serde_json::json!({
                    "id": 424242,
                    "slug": "preloop-local",
                    "pem": "-----BEGIN RSA PRIVATE KEY-----\nKEY\n-----END RSA PRIVATE KEY-----",
                    "webhook_secret": "hook-secret",
                    "html_url": "https://github.com/apps/preloop-local",
                }))
            }),
        );
        tokio::spawn(async move { axum::serve(listener, router).await });
        base
    }

    /// The whole redirect dance: the form page hands GitHub a manifest and a
    /// nonce, the callback converts the code and forwards the browser to the
    /// install page, and `setup_url` reports the installation back.
    #[tokio::test]
    async fn browser_flow_converts_and_reports_the_installation() {
        let api = mock_github().await;
        let listener = tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .unwrap();
        let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let flow =
            tokio::spawn(
                async move { register_on(listener, "preloop-local", None, None, &api).await },
            );

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let form = client
            .get(&base)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            form.contains(r#"action="https://github.com/settings/apps/new?state="#),
            "the form must post the manifest to GitHub: {form}"
        );
        let state = form
            .split("apps/new?state=")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .expect("state nonce in the form action")
            .to_owned();
        assert!(
            form.contains("127.0.0.1") && form.contains("/callback"),
            "the manifest must redirect back to this listener"
        );

        // A callback that did not come from this form is refused, and the
        // flow stays open for the real one.
        let forged = client
            .get(format!("{base}/callback?code=conversion-code&state=forged"))
            .send()
            .await
            .unwrap();
        assert_eq!(forged.status(), reqwest::StatusCode::BAD_REQUEST);

        let callback = client
            .get(format!(
                "{base}/callback?code=conversion-code&state={state}"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(callback.status(), reqwest::StatusCode::SEE_OTHER);
        assert_eq!(
            callback
                .headers()
                .get("location")
                .and_then(|value| value.to_str().ok()),
            Some("https://github.com/apps/preloop-local/installations/new"),
            "the browser is sent straight to the install page"
        );

        client
            .get(format!(
                "{base}/installed?installation_id=99&setup_action=install"
            ))
            .send()
            .await
            .unwrap();

        let registration = flow.await.unwrap().unwrap();
        assert_eq!(registration.credentials.id, 424242);
        assert_eq!(
            registration.credentials.webhook_secret.as_deref(),
            Some("hook-secret")
        );
        assert!(registration
            .credentials
            .pem
            .contains("BEGIN RSA PRIVATE KEY"));
        assert_eq!(registration.installation_id, Some(99));
    }

    /// GitHub rejecting the conversion must surface as an error, not a
    /// half-written config.
    #[tokio::test]
    async fn conversion_failure_is_reported() {
        let listener = tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .unwrap();
        let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let api_listener =
            tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .await
                .unwrap();
        let api = format!(
            "http://127.0.0.1:{}",
            api_listener.local_addr().unwrap().port()
        );
        tokio::spawn(async move {
            let router = Router::new().route(
                "/app-manifests/:code/conversions",
                axum::routing::post(|| async {
                    (axum::http::StatusCode::UNPROCESSABLE_ENTITY, "code expired")
                }),
            );
            axum::serve(api_listener, router).await
        });
        let flow =
            tokio::spawn(
                async move { register_on(listener, "preloop-local", None, None, &api).await },
            );

        let client = reqwest::Client::new();
        let form = client
            .get(&base)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let state = form
            .split("apps/new?state=")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .expect("state nonce in the form action")
            .to_owned();
        let response = client
            .get(format!("{base}/callback?code=stale&state={state}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);

        let error = flow.await.unwrap().unwrap_err();
        assert!(
            format!("{error:#}").contains("code expired"),
            "GitHub's reason must reach the operator: {error:#}"
        );
    }

    #[test]
    fn manifest_disables_webhooks_without_a_public_url() {
        let manifest = manifest("preloop", "http://127.0.0.1:4000/callback", None);
        assert!(
            manifest.get("hook_attributes").is_none(),
            "GitHub rejects hook URLs it cannot reach over the public \
             Internet (127.0.0.1), even when the hook is inactive; no \
             public URL must mean no webhook configured"
        );
        assert!(manifest.get("default_events").is_none());
        assert_eq!(
            manifest["setup_url"], "http://127.0.0.1:4000/installed",
            "the post-install redirect must return to this listener"
        );
        assert_eq!(manifest["public"], false);
        assert_eq!(manifest["default_permissions"]["contents"], "read");
    }

    #[test]
    fn manifest_points_webhooks_at_a_public_url() {
        let manifest = manifest(
            "preloop",
            "http://127.0.0.1:4000/callback",
            Some("https://ci.example.com/"),
        );
        assert_eq!(
            manifest["hook_attributes"]["url"],
            "https://ci.example.com/api/v1/github/webhooks"
        );
        assert_eq!(manifest["hook_attributes"]["active"], true);
        assert_eq!(manifest["default_events"][0], "push");
    }

    #[test]
    fn creation_url_targets_the_org_when_given() {
        assert_eq!(
            creation_url(Some("acme"), "abc123"),
            "https://github.com/organizations/acme/settings/apps/new?state=abc123"
        );
        assert_eq!(
            creation_url(None, "abc123"),
            "https://github.com/settings/apps/new?state=abc123"
        );
    }

    #[test]
    fn install_url_prefers_the_slug() {
        let credentials = AppCredentials {
            id: 7,
            slug: Some("my-app".to_owned()),
            pem: String::new(),
            webhook_secret: None,
            html_url: Some("https://github.com/apps/my-app".to_owned()),
        };
        assert_eq!(
            credentials.install_url().as_deref(),
            Some("https://github.com/apps/my-app/installations/new")
        );
        let without_slug = AppCredentials {
            slug: None,
            ..credentials
        };
        assert_eq!(
            without_slug.install_url().as_deref(),
            Some("https://github.com/apps/my-app")
        );
    }

    #[test]
    fn manifest_attribute_escaping_cannot_close_the_attribute() {
        let escaped = escape_attr(r#"{"name":"it's <b>"}"#);
        assert!(!escaped.contains('\''), "single quotes must be escaped");
        assert!(!escaped.contains('<'), "tags must be escaped");
        assert!(
            escaped.contains(r#""name""#),
            "double quotes stay literal inside a single-quoted attribute"
        );
    }
}
