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
//! machine (unlike the engine-hosted setup, which stores it on the engine),
//! and the callback is reachable only from this host.
//!
//! Over SSH the browser is on the laptop, which cannot reach this host's
//! loopback. The listener then binds a fixed port that the operator forwards
//! with `ssh -L`; the forward carries both trips, the form page and GitHub's
//! redirect.
//!
//! Three routes, one shot each:
//!
//! | Route        | Purpose                                                   |
//! |--------------|-----------------------------------------------------------|
//! | `/`          | auto-submitting form POST to GitHub's App-creation page    |
//! | `/callback`  | manifest code → credentials, then redirect to the install  |
//! | `/installed` | GitHub's post-install redirect; reports the installation   |

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse, Redirect};
use axum::routing::get;
use preloop_runner_server::github_app_setup::{
    AppCredentials, FormPage, convert_manifest_code, creation_url, error_page, form_page, manifest,
    random_nonce, success_page,
};
use serde::Deserialize;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};

/// Where the loopback flow creates Apps. The engine-hosted setup uses the
/// engine's configured GitHub host instead.
/// GitHub's web root, for a manifest-driven App by default. GHES engines
/// configure their own host in `github.server_url`.
pub(crate) const GITHUB_WEB: &str = "https://github.com";

/// How long to wait for the operator to finish in the browser.
const BROWSER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// How long to wait for the App installation after the App itself exists.
///
/// Shorter than [`BROWSER_TIMEOUT`]: by this point the credentials are
/// already written, so a timeout costs the operator a printed URL, not the
/// setup.
const INSTALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// The listener port over SSH when `--port` is not given. A forward needs a
/// port the operator can name before the flow starts, so the default is fixed
/// rather than random.
pub(crate) const SSH_FORWARD_PORT: u16 = 8765;

/// Says where GitHub's result goes. The loopback page writes to the local config.
const DELIVERY_NOTE: &str = "GitHub returns the credentials to this page, which writes them to your local config — the private key never leaves this machine.";

/// Everything the flow learned, in order of arrival.
#[derive(Debug)]
pub(crate) struct Registration {
    pub credentials: AppCredentials,
    /// Installation id, when the operator completed the install before
    /// [`INSTALL_TIMEOUT`]. The engine discovers it per repository anyway,
    /// so its absence is not a failure.
    pub installation_id: Option<u64>,
}

/// Shared state of the one-shot listener.
struct Flow {
    /// CSRF nonce echoed by GitHub; a callback carrying anything else did not
    /// originate from the form this process served.
    state: String,
    name: String,
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

/// Run the browser flow and persist the converted credentials immediately.
///
/// The callback runs as soon as GitHub's conversion response arrives, before
/// waiting for the optional installation redirect. GitHub returns the private
/// key only once, so waiting for installation before saving it would let a
/// timeout or Ctrl-C lose the key.
///
/// `guidance`, when given, replaces the plain "open this URL" lines. It is
/// printed once the listener is bound, so it can name the port it reached.
pub(crate) async fn register_with_persist<F, T>(
    name: &str,
    org: Option<&str>,
    port: u16,
    guidance: Option<&str>,
    open_browser: bool,
    persist: F,
) -> Result<(Registration, T)>
where
    F: FnOnce(&AppCredentials) -> Result<T>,
{
    let listener = tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
        .await
        .with_context(|| format!("binding 127.0.0.1:{port} for the GitHub redirect"))?;
    let local = listener
        .local_addr()
        .context("reading the local listener address")?;
    let base = format!("http://127.0.0.1:{}", local.port());
    match guidance {
        Some(text) => println!("{text}"),
        None => println!("Open this URL to create the GitHub App:\n  {base}"),
    }
    let api_base = std::env::var("PRELOOP_GITHUB_API_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_owned());
    // The listener is bound, but nothing answers on it until the server task
    // starts inside `register_on_with_persist` — so the browser is opened from
    // there. Otherwise a browser that connects immediately could wait on a
    // socket no one is serving yet.
    register_on_with_persist(listener, name, org, &api_base, persist, |base| {
        if open_browser && open_in_browser(base).is_err() {
            println!("(could not open a browser automatically — open the URL above)");
        }
        println!("Waiting for GitHub to redirect back…");
    })
    .await
}

/// [`register_with_persist`] against an already-bound listener, so a test can
/// learn the address before the flow starts.
#[cfg(test)]
async fn register_on(
    listener: tokio::net::TcpListener,
    name: &str,
    org: Option<&str>,
    api_base: &str,
) -> Result<Registration> {
    let (registration, ()) =
        register_on_with_persist(listener, name, org, api_base, |_| Ok(()), |_| {}).await?;
    Ok(registration)
}

/// [`register_on`] with immediate persistence of the converted credentials.
///
/// `announce` runs once the listener is actually being served, so a caller
/// that opens a browser can do it after the page it asks for can be answered.
async fn register_on_with_persist<F, T, A>(
    listener: tokio::net::TcpListener,
    name: &str,
    org: Option<&str>,
    api_base: &str,
    persist: F,
    announce: A,
) -> Result<(Registration, T)>
where
    F: FnOnce(&AppCredentials) -> Result<T>,
    A: FnOnce(&str),
{
    let local = listener
        .local_addr()
        .context("reading the local listener address")?;
    let base = format!("http://127.0.0.1:{}", local.port());
    let (credentials_tx, credentials_rx) = oneshot::channel();
    let (installed_tx, installed_rx) = oneshot::channel();
    let flow = Arc::new(Flow {
        state: random_nonce(),
        name: name.to_owned(),
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

    announce(&base);

    let credentials = match tokio::time::timeout(BROWSER_TIMEOUT, credentials_rx).await {
        Ok(Ok(result)) => result?,
        Ok(Err(_)) => anyhow::bail!("the registration listener stopped before GitHub replied"),
        Err(_) => anyhow::bail!(
            "timed out after {}s waiting for GitHub — re-run `preloop setup github --via app`",
            BROWSER_TIMEOUT.as_secs()
        ),
    };
    let persisted = match persist(&credentials) {
        Ok(value) => value,
        Err(error) => {
            shutdown.cancel();
            let _ = server.await;
            return Err(error)
                .context("persisting GitHub App credentials immediately after conversion");
        }
    };

    let installation_id = match tokio::time::timeout(INSTALL_TIMEOUT, installed_rx).await {
        Ok(Ok(id)) => Some(id),
        _ => None,
    };
    shutdown.cancel();
    let _ = server.await;
    Ok((
        Registration {
            credentials,
            installation_id,
        },
        persisted,
    ))
}

/// The form page: GitHub only accepts a manifest as a form POST, so this is
/// a self-submitting form rather than a link.
async fn page_form(State((flow, org)): State<(Arc<Flow>, Option<String>)>) -> impl IntoResponse {
    let setup_url = format!(
        "{}/installed",
        flow.redirect_url.trim_end_matches("/callback")
    );
    let manifest = manifest(&flow.name, &flow.redirect_url, None, Some(&setup_url));
    let action = creation_url(GITHUB_WEB, org.as_deref(), &flow.state);
    Html(form_page(&FormPage {
        name: &flow.name,
        org: org.as_deref(),
        action: &action,
        manifest: &manifest,
        public_url: None,
        delivery: DELIVERY_NOTE,
        auto_submit: false,
    }))
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
    let converted = convert_manifest_code(&flow.api_base, &query.code).await;
    let install_url = converted
        .as_ref()
        .ok()
        .and_then(|credentials| credentials.install_url(GITHUB_WEB));
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
    if let Some(id) = query.installation_id
        && let Some(tx) = flow.installed_tx.lock().await.take()
    {
        let _ = tx.send(id);
    }
    Html(success_page(
        "preloop is connected",
        "Credentials are written to your local config. You can close this tab and return to the terminal.",
    ))
}

/// Everything an SSH operator needs to reach the listener from a laptop: the
/// login name and address the session arrived on, and the port `sshd` listens
/// on when it is not 22.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SshTarget {
    pub user: String,
    pub host: String,
    pub port: Option<u16>,
}

impl SshTarget {
    /// Build the target from `SSH_CONNECTION` (`client_ip client_port
    /// server_ip server_port`), the login name, and the machine's hostname.
    ///
    /// `server_ip` is the address the laptop connected to, so the laptop can
    /// reach it by definition. The hostname only stands in when the variable
    /// is missing.
    pub(crate) fn from_env(connection: Option<&str>, user: Option<&str>, hostname: &str) -> Self {
        let fields: Vec<&str> = connection
            .map(|value| value.split_whitespace().collect())
            .unwrap_or_default();
        let (host, port) = match fields.as_slice() {
            [_, _, server_ip, server_port] => (
                ipv4_mapped_to_v4(server_ip).to_owned(),
                server_port.parse::<u16>().ok(),
            ),
            _ => (hostname.to_owned(), None),
        };
        let user = user
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("<user>")
            .to_owned();
        Self { user, host, port }
    }

    /// The command to run in a second terminal on the laptop. The port on both
    /// sides is `port`, so the browser's `127.0.0.1:port` reaches this
    /// listener.
    pub(crate) fn forward_command(&self, port: u16) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        let ssh_port = match self.port {
            Some(ssh_port) if ssh_port != 22 => format!(" -p {ssh_port}"),
            _ => String::new(),
        };
        format!(
            "ssh -L {port}:127.0.0.1:{port}{ssh_port} {user}@{host}",
            user = self.user
        )
    }
}

/// `::ffff:203.0.113.5` is the IPv4 address `sshd` reports for an IPv4 client
/// on a dual-stack socket; the laptop should be given the plain address.
fn ipv4_mapped_to_v4(address: &str) -> &str {
    address
        .strip_prefix("::ffff:")
        .filter(|rest| rest.parse::<Ipv4Addr>().is_ok())
        .unwrap_or(address)
}

/// Whether this process runs inside an SSH session. `sshd` sets
/// `SSH_CONNECTION` for every session and `SSH_TTY` once a terminal is
/// allocated; either means the browser is not on this machine.
pub(crate) fn ssh_session_from_env(get: impl Fn(&str) -> Option<String>) -> bool {
    ["SSH_CONNECTION", "SSH_TTY"]
        .iter()
        .any(|key| get(key).is_some_and(|value| !value.trim().is_empty()))
/// Exchange the one-time manifest code for the App's credentials.
async fn convert(api_base: &str, code: &str) -> Result<AppCredentials> {
    let url = format!("{}/app-manifests/{}/conversions", api_base, urlencode(code));
    // Not `crate::build_client()`: that one targets the engine's native API
    // surface, and this call goes to api.github.com.
    let response = preloop_runner_server::shared_http::with_tls_roots(reqwest::Client::builder())
        .build()
        .context("building the GitHub manifest client")?
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

/// This machine's hostname, for the forward command when `SSH_CONNECTION` is
/// absent. `<host>` stands in when the hostname cannot be read.
pub(crate) fn local_hostname() -> String {
    std::process::Command::new("hostname")
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "<host>".to_owned())
}

/// The text printed over SSH before the listener waits: the forward to run on
/// the laptop, then the address to open once it is up.
pub(crate) fn forwarded_guidance(target: &SshTarget, port: u16) -> String {
    format!(
        "This host is reached over SSH, so GitHub's redirect can only come back through a port forward.\n\
         In a second terminal on your laptop, run:\n\n  {command}\n\n\
         Use the name you normally connect with if it differs, for example an ~/.ssh/config alias.\n\
         Keep that terminal open, then open this address in your laptop's browser:\n\n  http://127.0.0.1:{port}\n",
        command = target.forward_command(port),
    )
}

/// Open `url` in a browser. Failure is not fatal — the URL is always printed first.
///
/// `$BROWSER` is tried first, as `xdg-open` and most tools do: a
/// colon-separated list of commands, each run with the URL as its last
/// argument or in place of `%s`. VS Code and Cursor set it to a helper that
/// opens the URL on the user's machine and forwards the port, which is what
/// makes the remote-terminal case work. The platform opener is the fallback.
pub(crate) fn open_in_browser(url: &str) -> Result<()> {
    if let Some(spec) = std::env::var("BROWSER")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        for argv in browser_commands(&spec, url) {
            if run_browser_command(&argv).is_ok() {
                return Ok(());
            }
        }
    }

    #[cfg(target_os = "macos")]
    let (program, args): (&str, Vec<&str>) = ("open", vec![url]);
    #[cfg(target_os = "windows")]
    let (program, args): (&str, Vec<&str>) = ("cmd", vec!["/C", "start", "", url]);
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let (program, args): (&str, Vec<&str>) = ("xdg-open", vec![url]);

    // Started, not waited on, for the same reason `$BROWSER` is: the opener
    // may hold the terminal until the browser exits.
    std::process::Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .with_context(|| format!("spawning {program}"))?;
    Ok(())
}

/// Expand a `$BROWSER` value into argument vectors, one per colon-separated
/// entry. Each entry is split into words the way a shell would, so a quoted
/// path or argument stays one word (`"/opt/Remote Browser/bin/open" %s` names
/// `/opt/Remote Browser/bin/open`, not `"/opt/Remote`); `%s` in any word is
/// replaced by the URL, and when no word has one the URL is appended. Blank
/// entries and entries with an unterminated quote are skipped. The URL is
/// always a single argument, and no shell ever sees any of it.
fn browser_commands(spec: &str, url: &str) -> Vec<Vec<String>> {
    let mut commands = Vec::new();
    for entry in spec.split(':') {
        let Some(words) = split_browser_words(entry) else {
            continue;
        };
        if words.is_empty() {
            continue;
        }
        let mut substituted = false;
        let mut argv: Vec<String> = words
            .into_iter()
            .map(|word| {
                if word.contains("%s") {
                    substituted = true;
                    word.replace("%s", url)
                } else {
                    word
                }
            })
            .collect();
        if !substituted {
            argv.push(url.to_owned());
        }
        commands.push(argv);
    }
    commands
}

/// Split one `$BROWSER` entry into words, honouring single and double quotes
/// and backslash escapes the way a shell does. `None` means the entry cannot
/// be read (a trailing backslash or an unterminated quote): a helper whose
/// command line cannot be reconstructed is not one to guess at, so it is
/// skipped and the next entry — or the platform opener — is tried.
fn split_browser_words(entry: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut started = false;
    let mut quote: Option<char> = None;
    let mut chars = entry.chars();
    while let Some(ch) = chars.next() {
        match quote {
            Some(open) if ch == open => quote = None,
            Some(_) => {
                word.push(ch);
                started = true;
            }
            None => match ch {
                '\'' | '"' => {
                    quote = Some(ch);
                    started = true;
                }
                '\\' => {
                    let escaped = chars.next()?;
                    word.push(escaped);
                    started = true;
                }
                _ if ch.is_whitespace() => {
                    if started {
                        words.push(std::mem::take(&mut word));
                        started = false;
                    }
                }
                _ => {
                    word.push(ch);
                    started = true;
                }
            },
        }
    }
    if quote.is_some() {
        return None;
    }
    if started {
        words.push(word);
    }
    Some(words)
}

/// Start a browser command without waiting for it to exit.
///
/// A browser outlives the setup command by design, and `$BROWSER` in a remote
/// terminal is a helper that keeps running. Waiting for its lifetime would
/// stall the flow around it: in the loopback flow the setup page cannot be
/// served while the CLI waits for the browser, and the browser waits for the
/// page. What matters here is only whether the program could be started at
/// all, which is what `spawn` reports.
fn run_browser_command(argv: &[String]) -> Result<()> {
    let (program, args) = argv.split_first().context("empty BROWSER entry")?;
    std::process::Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .with_context(|| format!("spawning {program}"))?;
    Ok(())
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
            "/app-manifests/{code}/conversions",
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
            tokio::spawn(async move { register_on(listener, "preloop-local", None, &api).await });

        let client = preloop_runner_server::shared_http::with_tls_roots(
            reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()),
        )
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
            form.contains(r#"name="manifest" value='"#),
            "the manifest must be a hidden form field: {form}"
        );
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
            form.contains(&format!(
                r#"action="https://github.com/settings/apps/new?state={state}""#
            )),
            "the action carries the extracted state nonce: {form}"
        );
        assert!(
            form.contains("127.0.0.1") && form.contains("/callback"),
            "the manifest must redirect back to this listener"
        );
        assert!(
            !form.contains("<script>"),
            "the loopback form waits for the operator's click, as it always has"
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
        let callback_body = callback.text().await.unwrap();
        assert!(
            !callback_body.contains("conversion-code")
                && !callback_body.contains("BEGIN RSA PRIVATE KEY")
                && !callback_body.contains("hook-secret"),
            "the callback page must not echo code, PEM, or webhook secret: {callback_body}"
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
            registration
                .credentials
                .webhook_secret
                .as_ref()
                .map(|secret| secret.expose()),
            Some("hook-secret")
        );
        assert!(
            registration
                .credentials
                .pem
                .expose()
                .contains("BEGIN RSA PRIVATE KEY")
        );
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
                "/app-manifests/{code}/conversions",
                axum::routing::post(|| async {
                    (axum::http::StatusCode::UNPROCESSABLE_ENTITY, "code expired")
                }),
            );
            axum::serve(api_listener, router).await
        });
        let flow =
            tokio::spawn(async move { register_on(listener, "preloop-local", None, &api).await });

        let client = preloop_runner_server::shared_http::with_tls_roots(reqwest::Client::builder())
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
        let error = format!("{error:#}");
        assert!(
            error.contains("422 Unprocessable Entity"),
            "the operator learns GitHub refused, with its status: {error}"
        );
        assert!(
            !error.contains("code expired"),
            "GitHub's response body stays in the log, not in the operator's error: {error}"
        );
    }

    #[test]
    fn browser_spec_splits_on_colons_and_places_the_url() {
        let url = "http://127.0.0.1:8765";
        assert_eq!(
            browser_commands("helper.sh", url),
            vec![vec!["helper.sh".to_owned(), url.to_owned()]],
            "without %s the URL is the last argument"
        );
        assert_eq!(
            browser_commands("open -a Safari %s:firefox", url),
            vec![
                vec![
                    "open".to_owned(),
                    "-a".to_owned(),
                    "Safari".to_owned(),
                    url.to_owned()
                ],
                vec!["firefox".to_owned(), url.to_owned()],
            ],
            "%s is replaced in place, and each colon-separated entry is its own command"
        );
        assert!(
            browser_commands(" : ", url).is_empty(),
            "blank entries are skipped"
        );
        assert_eq!(
            browser_commands("browser.sh --url=%s", "http://h/?a=1&b=2"),
            vec![vec![
                "browser.sh".to_owned(),
                "--url=http://h/?a=1&b=2".to_owned()
            ]],
            "the URL stays one argument, never shell-parsed"
        );
    }

    #[test]
    fn browser_spec_honours_quotes_and_escapes() {
        let url = "http://127.0.0.1:8765";
        assert_eq!(
            browser_commands("\"/opt/Remote Browser/bin/open\" %s", url),
            vec![vec![
                "/opt/Remote Browser/bin/open".to_owned(),
                url.to_owned()
            ]],
            "a quoted path with a space is one program, not a token with quotes in it"
        );
        assert_eq!(
            browser_commands("helper.sh '--url %s':firefox", url),
            vec![
                vec!["helper.sh".to_owned(), format!("--url {url}")],
                vec!["firefox".to_owned(), url.to_owned()],
            ],
            "single quotes group a flag and its value, so the URL stays in place"
        );
        assert_eq!(
            browser_commands(r"my\ helper %s", url),
            vec![vec!["my helper".to_owned(), url.to_owned()]],
            "a backslash escape joins words"
        );
        assert_eq!(
            browser_commands("open -a \"Safari\"", url),
            vec![vec![
                "open".to_owned(),
                "-a".to_owned(),
                "Safari".to_owned(),
                url.to_owned()
            ]],
            "quotes are removed from the argument they group"
        );
        assert!(
            browser_commands("unterminated \"quote %s:firefox", url).len() == 1,
            "an entry that cannot be parsed is skipped, not guessed at"
        );
        assert_eq!(
            browser_commands("unterminated \"quote %s:firefox", url),
            vec![vec!["firefox".to_owned(), url.to_owned()]],
            "the next entry still gets its chance"
        );
    }

    #[test]
    fn ssh_target_is_the_address_the_laptop_connected_to() {
        let target = SshTarget::from_env(
            Some("203.0.113.7 40000 203.0.113.5 22"),
            Some("alice"),
            "build-box",
        );
        assert_eq!(target.host, "203.0.113.5");
        assert_eq!(target.user, "alice");
        assert_eq!(
            target.forward_command(8765),
            "ssh -L 8765:127.0.0.1:8765 alice@203.0.113.5",
            "port 22 needs no -p"
        );
    }

    #[test]
    fn ssh_target_names_a_non_default_port_and_falls_back_without_the_variable() {
        let target = SshTarget::from_env(
            Some("203.0.113.7 40000 203.0.113.5 2222"),
            Some("alice"),
            "build-box",
        );
        assert_eq!(
            target.forward_command(8765),
            "ssh -L 8765:127.0.0.1:8765 -p 2222 alice@203.0.113.5"
        );

        let fallback = SshTarget::from_env(None, Some("bob"), "build-box");
        assert_eq!(fallback.host, "build-box");
        assert_eq!(fallback.port, None);
        assert_eq!(
            fallback.forward_command(8765),
            "ssh -L 8765:127.0.0.1:8765 bob@build-box"
        );

        let anonymous = SshTarget::from_env(None, None, "build-box");
        assert_eq!(anonymous.user, "<user>", "a placeholder, never a guess");
    }

    #[test]
    fn ssh_target_brackets_ipv6_and_unmaps_ipv4() {
        let v6 = SshTarget::from_env(Some("2001:db8::9 5000 2001:db8::5 22"), Some("a"), "h");
        assert_eq!(
            v6.forward_command(8765),
            "ssh -L 8765:127.0.0.1:8765 a@[2001:db8::5]"
        );
        let mapped = SshTarget::from_env(
            Some("::ffff:203.0.113.7 5000 ::ffff:203.0.113.5 22"),
            Some("a"),
            "h",
        );
        assert_eq!(mapped.host, "203.0.113.5");
    }

    #[test]
    fn ssh_is_detected_from_either_variable() {
        assert!(ssh_session_from_env(|name| {
            (name == "SSH_CONNECTION").then(|| "x".to_owned())
        }));
        assert!(ssh_session_from_env(|name| {
            (name == "SSH_TTY").then(|| "x".to_owned())
        }));
        assert!(!ssh_session_from_env(|_| None));
        assert!(
            !ssh_session_from_env(|name| (name == "SSH_TTY").then(|| "  ".to_owned())),
            "a blank value counts as unset"
        );
    }

    #[test]
    fn guidance_names_the_forward_and_the_address_to_open() {
        let target = SshTarget::from_env(
            Some("203.0.113.7 40000 203.0.113.5 22"),
            Some("alice"),
            "build-box",
        );
        let text = forwarded_guidance(&target, SSH_FORWARD_PORT);
        assert!(
            text.contains("ssh -L 8765:127.0.0.1:8765 alice@203.0.113.5"),
            "{text}"
        );
        assert!(text.contains("http://127.0.0.1:8765"), "{text}");
        assert!(
            text.contains("~/.ssh/config"),
            "the host may need adjusting: {text}"
        );
    }
}
