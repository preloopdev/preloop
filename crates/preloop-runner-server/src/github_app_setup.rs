//! GitHub App creation through GitHub's manifest flow.
//!
//! Two front ends create Apps: the CLI's loopback listener
//! (`preloop setup github --via app`) and the engine-hosted setup
//! (`/setup/github`, see [`session`]). They share the parts that must not
//! differ between them: the manifest GitHub receives, the page that posts it,
//! the conversion of GitHub's one-time code, and the write of the converted
//! credentials into the config file and credential store.

pub mod session;

use crate::config::{AppConfig, ConfigFile, load_config_from, write_config_to};
use crate::credential_store::{
    CredentialRef, CredentialWriter, SecretString, Written, github_reference,
    github_reference_with_host,
};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;
use tracing::warn;

/// Default permissions of the generated App.
///
/// The floor a workflow needs to check out code and report status: GitHub
/// always grants `metadata: read`, `contents: read` backs `actions/checkout`,
/// `pull_requests: read` backs PR-triggered runs, and `checks: write` lets
/// the engine publish check runs. Anything beyond that is a decision the
/// operator should make in the App's settings, not one this flow makes for
/// them — an installation token is capped by the App's grant, so a narrow
/// default cannot silently widen a job's authority.
pub const DEFAULT_PERMISSIONS: &[(&str, &str)] = &[
    ("checks", "write"),
    ("contents", "read"),
    ("metadata", "read"),
    ("pull_requests", "read"),
];

/// Credentials GitHub returns from a manifest conversion.
///
/// The private key and webhook secret arrive once and are never shown again,
/// so they are `SecretString` from the moment they are deserialized: every
/// `Debug` and any accidental forwarding redacts them, and the plaintext
/// exists only where a caller writes it to the credential store.
#[derive(Debug, Deserialize)]
pub struct AppCredentials {
    /// Numeric App id (the JWT `iss`).
    pub id: u64,
    /// URL slug, for the installation link.
    #[serde(default)]
    pub slug: Option<String>,
    /// PKCS#1 private key.
    pub pem: SecretString,
    /// `X-Hub-Signature-256` secret; absent when the manifest disabled hooks.
    #[serde(default)]
    pub webhook_secret: Option<SecretString>,
    /// Human-facing App page.
    #[serde(default)]
    pub html_url: Option<String>,
}

impl AppCredentials {
    /// Where the operator installs the App on their repositories.
    ///
    /// Prefers the slug-based path, which lands directly on the repository
    /// picker; falls back to the App page when GitHub omitted the slug.
    /// `github` is the same web root the App was created on — a GHES App's
    /// installation page is on its own server, never on `github.com`.
    pub fn install_url(&self, github: &str) -> Option<String> {
        self.slug
            .as_ref()
            .map(|slug| {
                format!(
                    "{}/apps/{}/installations/new",
                    github.trim_end_matches('/'),
                    urlencode(slug)
                )
            })
            .or_else(|| self.html_url.clone())
    }
}

/// Where the App is created: a personal account or an organization.
///
/// `github` is GitHub's web root (`https://github.com`, or a GHES host).
pub fn creation_url(github: &str, org: Option<&str>, state: &str) -> String {
    let github = github.trim_end_matches('/');
    match org {
        Some(org) => format!(
            "{github}/organizations/{org}/settings/apps/new?state={state}",
            org = urlencode(org),
            state = urlencode(state),
        ),
        None => format!(
            "{github}/settings/apps/new?state={state}",
            state = urlencode(state)
        ),
    }
}

/// The manifest GitHub renders as a pre-filled App-creation form.
///
/// `hook_attributes` is only meaningful when GitHub can reach the engine, so
/// an engine without a public URL ships the App with no webhook configured
/// rather than pointing one at an address that will never resolve from
/// GitHub's side. GitHub rejects any `hook_attributes.url` it cannot reach
/// over the public Internet — including an `active: false` one — so the field
/// is omitted entirely. `setup_url` is where GitHub sends the browser after
/// installation; callers that have no such page leave it out.
pub fn manifest(
    name: &str,
    redirect_url: &str,
    public_url: Option<&str>,
    setup_url: Option<&str>,
) -> serde_json::Value {
    let permissions: serde_json::Map<String, serde_json::Value> = DEFAULT_PERMISSIONS
        .iter()
        .map(|(scope, access)| ((*scope).to_owned(), serde_json::Value::from(*access)))
        .collect();
    let mut manifest = serde_json::json!({
        "name": name,
        "url": public_url.unwrap_or("https://github.com/preloop-dev/preloop"),
        "redirect_url": redirect_url,
        "setup_on_update": false,
        "public": false,
        "default_permissions": permissions,
    });
    if let Some(setup_url) = setup_url {
        manifest["setup_url"] = serde_json::Value::from(setup_url);
    }
    if let Some(public) = public_url {
        manifest["hook_attributes"] = serde_json::json!({
            "url": format!("{}/api/v1/github/webhooks", public.trim_end_matches('/')),
            "active": true,
        });
        manifest["default_events"] = serde_json::json!(crate::github::manifest_default_events());
    }
    manifest
}

/// The App-creation page's content. The caller writes the explanation of
/// where GitHub's result goes, because it differs by front end.
pub struct FormPage<'a> {
    /// App name, shown to the operator.
    pub name: &'a str,
    /// Organization the App is created under, when it is not the personal account.
    pub org: Option<&'a str>,
    /// The `settings/apps/new` URL the form posts to, carrying the state nonce.
    pub action: &'a str,
    /// The manifest GitHub receives in the form's `manifest` field.
    pub manifest: &'a serde_json::Value,
    /// Public URL that receives webhooks, or `None` when webhooks are off.
    pub public_url: Option<&'a str>,
    /// One paragraph of raw HTML saying where GitHub's result goes.
    pub delivery: &'a str,
    /// Submit the form as soon as the page loads. Otherwise the operator
    /// presses the button.
    pub auto_submit: bool,
}

/// The App-creation page. GitHub only accepts a manifest as a form POST, so
/// the page is a form rather than a link.
pub fn form_page(page: &FormPage<'_>) -> String {
    let scope = match page.org {
        Some(org) => format!("the <strong>{}</strong> organization", escape_html(org)),
        None => "your personal account".to_owned(),
    };
    let webhooks = match page.public_url {
        Some(public) => format!("Webhooks deliver to {}.", escape_html(public)),
        None => "Webhooks are off — this engine is not reachable from GitHub.".to_owned(),
    };
    let permissions = DEFAULT_PERMISSIONS
        .iter()
        .map(|(scope, access)| format!("{scope}: {access}"))
        .collect::<Vec<_>>()
        .join(", ");
    let submit = if page.auto_submit {
        "<script>document.getElementById('manifest-form').submit();</script>"
    } else {
        ""
    };
    format!(
        r#"<!DOCTYPE html>
<html>
<head><title>Create the preloop GitHub App</title></head>
<body style="font-family: system-ui, sans-serif; padding: 40px; max-width: 640px; margin: auto;">
  <h1>Create the preloop GitHub App</h1>
  <p>This creates a private App named <strong>{name}</strong> on {scope}, with
     permissions {permissions}. {webhooks}</p>
  <p>{delivery}</p>
  <form id="manifest-form" action="{action}" method="post">
    <input type="hidden" name="manifest" value='{manifest}'>
    <button type="submit" style="font-size: 16px; padding: 10px 20px; cursor: pointer; background: #2da44e; color: white; border: none; border-radius: 6px; font-weight: bold;">Create on GitHub</button>
  </form>
  {submit}
</body>
</html>"#,
        name = escape_html(page.name),
        scope = scope,
        permissions = escape_html(&permissions),
        webhooks = webhooks,
        delivery = page.delivery,
        action = escape_html(page.action),
        manifest = escape_attr(&page.manifest.to_string()),
        submit = submit,
    )
}

/// Exchange the one-time manifest code for the App's credentials.
///
/// `api_base` is GitHub's REST root: `https://api.github.com`, or a GHES
/// `/api/v3` base.
pub async fn convert_manifest_code(api_base: &str, code: &str) -> Result<AppCredentials> {
    let url = format!(
        "{}/app-manifests/{}/conversions",
        api_base.trim_end_matches('/'),
        urlencode(code)
    );
    let response = crate::shared_http::CLIENT
        .post(&url)
        .header("User-Agent", "preloop")
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .context("calling GitHub's manifest conversion endpoint")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        // The body stays in the engine's log: callers surface this error to
        // an operator (the CLI prints it, the loopback flow renders it in a
        // page), and a rejection's full text is not theirs to read. The
        // status is what tells them whether to retry or fix the App.
        warn!(
            status = %status,
            response = %body.trim(),
            "GitHub rejected the manifest conversion"
        );
        anyhow::bail!("GitHub rejected the manifest conversion ({status})");
    }
    serde_json::from_str(&body).context("parsing GitHub's manifest conversion response")
}

/// 128 bits of CSRF nonce, hex-encoded.
pub fn random_nonce() -> String {
    let bytes: [u8; 16] = rand::random();
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Equality that inspects every byte before answering, so the time taken does
/// not reveal how much of a secret matched.
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let diff = left
        .iter()
        .zip(right)
        .fold(0_u8, |acc, (a, b)| acc | (a ^ b));
    left.len() == right.len() && diff == 0
}

/// Persist App credentials in the config file at `path` and the credential
/// store beside it, exactly as `preloop setup github` always has.
///
/// Supports both the primary legacy App and additional Apps in `github.apps`:
/// - If `app_id` matches `config.github.app_id`, updates the legacy fields in place.
/// - If `app_id` matches an existing entry in `config.github.apps`, updates that entry in place.
/// - If `config.github.app_id` is empty and `config.github.apps` is empty, writes to the legacy fields.
/// - If `config.github.app_id` is set to a different App, appends a new `AppConfig` to `config.github.apps`.
///
/// The webhook secret only exists on the manifest path — GitHub generates it
/// during creation — so it is written when present and left untouched
/// otherwise, which keeps a manual re-run from clearing a working secret.
///
/// Returns the operator notices the write produced (a credential store that
/// fell back to the private file store). The caller decides how to show them.
pub fn save_app_credentials_at(
    path: &Path,
    app_id: &str,
    pem: &SecretString,
    webhook_secret: Option<SecretString>,
) -> Result<Vec<String>> {
    // Whole-file mutation: read, change, write back under the lock every
    // other front that rewrites this file also takes, so a secrets or policy
    // update landing mid-flight is not dropped by this older snapshot.
    let _mutation = crate::config::config_mutation_lock().lock();
    let mut config = load_config_from(path)?;
    let mut writer = CredentialWriter::for_config(path, config.credential_store.as_deref())?;
    let notices = record_app_credentials(&mut config, &mut writer, app_id, pem, webhook_secret)?;
    write_config_to(path, &config)?;
    Ok(notices)
}

/// Record one App's credentials in `config`. The secrets go through `writer`,
/// and `config` keeps only their references. Kept apart from
/// [`save_app_credentials_at`] so the fallback path can be tested without the
/// real home directory or keychain.
pub fn record_app_credentials(
    config: &mut ConfigFile,
    writer: &mut CredentialWriter,
    app_id: &str,
    pem: &SecretString,
    webhook_secret: Option<SecretString>,
) -> Result<Vec<String>> {
    let mut notices = Vec::new();
    let host = config.github.server_url.clone();
    let host = host.as_deref();
    if config.github.mint_failure.is_none() {
        config.github.mint_failure = Some("local".into());
    }

    let pem_ref = github_reference_with_host("app-pem", host, Some(app_id))?;
    // `expose()` here: this is the credential-store protocol boundary, the
    // only place the plaintext leaves the typing that redacts it.
    notices.extend(persist_secret(writer, config, &pem_ref, pem.expose())?);
    let webhook_ref = match &webhook_secret {
        Some(secret) => {
            let reference = github_reference_with_host("webhook", host, Some(app_id))?;
            notices.extend(persist_secret(writer, config, &reference, secret.expose())?);
            Some(reference.as_str().to_owned())
        }
        None => None,
    };
    let pem_ref = pem_ref.as_str().to_owned();

    if let Some(existing_primary) = &config.github.app_id
        && existing_primary == app_id
    {
        config.github.legacy_app_pem = None;
        config.github.app_pem_ref = Some(pem_ref);
        if webhook_ref.is_some() {
            config.github.legacy_webhook_secret = None;
            config.github.webhook_secret_ref = webhook_ref;
        }
        return Ok(notices);
    }
    if let Some(existing) = config.github.apps.iter_mut().find(|a| a.app_id == app_id) {
        existing.legacy_pem.clear();
        existing.pem_ref = Some(pem_ref);
        if webhook_ref.is_some() {
            existing.legacy_webhook_secret = None;
            existing.webhook_secret_ref = webhook_ref;
        }
        return Ok(notices);
    }
    if config.github.app_id.is_none() && config.github.apps.is_empty() {
        config.github.app_id = Some(app_id.to_owned());
        config.github.legacy_app_pem = None;
        config.github.app_pem_ref = Some(pem_ref);
        // An App id now exists, so a secret parked inline by `--webhook-secret`
        // can finally be namespaced and moved.
        if webhook_ref.is_some() {
            config.github.legacy_webhook_secret = None;
            config.github.webhook_secret_ref = webhook_ref;
        } else if let Some(secret) = config.github.legacy_webhook_secret.take() {
            let reference = github_reference("webhook", Some(app_id))?;
            notices.extend(persist_secret(writer, config, &reference, &secret)?);
            config.github.webhook_secret_ref = Some(reference.as_str().to_owned());
        }
        return Ok(notices);
    }
    config.github.apps.push(AppConfig {
        app_id: app_id.to_owned(),
        pem_ref: Some(pem_ref),
        webhook_secret_ref: webhook_ref,
        ..Default::default()
    });
    Ok(notices)
}

/// Write one credential through `writer`. When the OS store could not hold it,
/// the private file store did: the returned notice says where it went, and
/// `config` records `credential_store = "file"` so the next start reads the
/// same place.
pub fn persist_secret(
    writer: &mut CredentialWriter,
    config: &mut ConfigFile,
    reference: &CredentialRef,
    value: &str,
) -> Result<Option<String>> {
    if let Written::FileFallback { reason } = writer.write(reference, &SecretString::new(value))? {
        config.credential_store = Some("file".to_owned());
        return Ok(Some(format!(
            "warning: the operating-system credential store could not hold this credential ({reason:#}).\n  \
             Stored it in the private file store at {} instead; config.toml now sets credential_store = \"file\".",
            writer.file_dir().display()
        )));
    }
    Ok(None)
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

/// Escape HTML text content and double-quoted attribute values.
pub fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Escape a value destined for a single-quoted HTML attribute. The manifest
/// is JSON, so it is full of double quotes: the attribute is single-quoted
/// and only the single quote (and `&`/`<`) needs escaping.
pub fn escape_attr(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('\'', "&#39;")
}

/// A page confirming success.
pub fn success_page(title: &str, detail: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html><head><title>{title}</title></head>
<body style="font-family: system-ui, sans-serif; padding: 40px; max-width: 640px; margin: auto;">
  <h1 style="color: #2da44e;">{title}</h1>
  <p>{detail}</p>
</body></html>"#,
        title = escape_html(title),
        detail = escape_html(detail)
    )
}

/// A page reporting a failed setup.
pub fn error_page(title: &str, detail: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html><head><title>{title}</title></head>
<body style="font-family: system-ui, sans-serif; padding: 40px; max-width: 640px; margin: auto;">
  <h1 style="color: #cf222e;">{title}</h1>
  <p>{detail}</p>
  <p>Re-run <code>preloop setup github --via app</code> to try again.</p>
</body></html>"#,
        title = escape_html(title),
        detail = escape_html(detail)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const PEM: &str =
        "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBAKj\n-----END RSA PRIVATE KEY-----\n";

    #[test]
    fn manifest_disables_webhooks_without_a_public_url() {
        let manifest = manifest(
            "preloop",
            "http://127.0.0.1:4000/callback",
            None,
            Some("http://127.0.0.1:4000/installed"),
        );
        assert!(
            manifest.get("hook_attributes").is_none(),
            "GitHub rejects hook URLs it cannot reach over the public \
             Internet (127.0.0.1), even when the hook is inactive; no \
             public URL must mean no webhook configured"
        );
        assert!(manifest.get("default_events").is_none());
        assert_eq!(
            manifest["setup_url"], "http://127.0.0.1:4000/installed",
            "the post-install redirect must return to the listener"
        );
        assert_eq!(manifest["public"], false);
        assert_eq!(manifest["default_permissions"]["contents"], "read");
    }

    #[test]
    fn manifest_points_webhooks_at_a_public_url() {
        let manifest = manifest(
            "preloop",
            "https://ci.example.com/setup/github/callback",
            Some("https://ci.example.com/"),
            None,
        );
        assert_eq!(
            manifest["hook_attributes"]["url"],
            "https://ci.example.com/api/v1/github/webhooks"
        );
        assert_eq!(manifest["hook_attributes"]["active"], true);
        let events = manifest["default_events"].as_array().unwrap();
        assert!(events.iter().any(|e| e == "push"));
        assert!(events.iter().any(|e| e == "pull_request"));
        assert!(
            manifest.get("setup_url").is_none(),
            "no setup page means no setup_url"
        );
    }

    #[test]
    fn creation_url_targets_the_org_when_given() {
        assert_eq!(
            creation_url("https://github.com", Some("acme"), "abc123"),
            "https://github.com/organizations/acme/settings/apps/new?state=abc123"
        );
        assert_eq!(
            creation_url("https://github.com", None, "abc123"),
            "https://github.com/settings/apps/new?state=abc123"
        );
        assert_eq!(
            creation_url("https://ghe.example.com/", None, "abc123"),
            "https://ghe.example.com/settings/apps/new?state=abc123",
            "a GHES host keeps its own web root"
        );
    }

    #[test]
    fn install_url_prefers_the_slug_on_the_configured_host() {
        let credentials = AppCredentials {
            id: 7,
            slug: Some("my-app".to_owned()),
            pem: SecretString::new(""),
            webhook_secret: None,
            html_url: Some("https://github.com/apps/my-app".to_owned()),
        };
        assert_eq!(
            credentials.install_url("https://github.com").as_deref(),
            Some("https://github.com/apps/my-app/installations/new")
        );
        assert_eq!(
            credentials
                .install_url("https://ghe.example.com/")
                .as_deref(),
            Some("https://ghe.example.com/apps/my-app/installations/new"),
            "a GHES App is installed on the server it was created on"
        );
        let without_slug = AppCredentials {
            slug: None,
            ..credentials
        };
        assert_eq!(
            without_slug
                .install_url("https://ghe.example.com")
                .as_deref(),
            Some("https://github.com/apps/my-app"),
            "without a slug the response's own App page is used verbatim"
        );
    }

    #[test]
    fn converted_credentials_redact_their_secrets() {
        let credentials = AppCredentials {
            id: 7,
            slug: None,
            pem: SecretString::new("-----BEGIN RSA PRIVATE KEY-----"),
            webhook_secret: Some(SecretString::new("hook-secret")),
            html_url: None,
        };
        let rendered = format!("{credentials:?}");
        assert!(!rendered.contains("BEGIN RSA"), "{rendered}");
        assert!(!rendered.contains("hook-secret"), "{rendered}");
        assert_eq!(
            credentials.pem.expose(),
            "-----BEGIN RSA PRIVATE KEY-----",
            "the plaintext is still reachable where a store needs it"
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

    #[test]
    fn form_page_submits_on_load_only_when_asked() {
        let manifest = manifest("preloop", "http://127.0.0.1:4000/callback", None, None);
        let page = |auto_submit| {
            form_page(&FormPage {
                name: "preloop",
                org: None,
                action: "https://github.com/settings/apps/new?state=abc",
                manifest: &manifest,
                public_url: None,
                delivery: "delivery",
                auto_submit,
            })
        };
        assert!(page(true).contains("getElementById('manifest-form').submit()"));
        assert!(
            !page(false).contains("<script>"),
            "the loopback page waits for the button, as it always has"
        );
        assert!(page(false).contains("Create on GitHub"));
    }

    #[test]
    fn form_page_escapes_the_name_and_carries_the_manifest_intact() {
        let manifest = manifest("a\"<b>", "http://127.0.0.1:4000/callback", None, None);
        let html = form_page(&FormPage {
            name: "a\"<b>",
            org: Some("acme"),
            action: "https://github.com/organizations/acme/settings/apps/new?state=abc",
            manifest: &manifest,
            public_url: None,
            delivery: "delivery",
            auto_submit: false,
        });
        assert!(html.contains("a&quot;&lt;b&gt;"), "{html}");
        assert!(html.contains("the <strong>acme</strong> organization"));
        let attribute = html
            .split("value='")
            .nth(1)
            .and_then(|rest| rest.split("'>").next())
            .expect("the manifest sits in a single-quoted attribute");
        assert!(
            !attribute.contains('\''),
            "a single quote would end the attribute: {attribute}"
        );
        assert!(
            attribute.starts_with('{') && attribute.contains("\"redirect_url\""),
            "the manifest JSON survives escaping: {attribute}"
        );
        assert!(
            !attribute.contains('<'),
            "tags in the name are escaped: {attribute}"
        );
    }

    #[test]
    fn constant_time_eq_compares_length_and_every_byte() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }

    /// The credentials reach the config file as references, the key reaches
    /// the credential store, and a normal load returns the key unchanged.
    #[test]
    fn saved_credentials_keep_secrets_out_of_the_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "credential_store = \"file\"\n").unwrap();

        let notices = save_app_credentials_at(
            &path,
            "4242",
            &SecretString::new(PEM),
            Some(SecretString::new("hook-secret")),
        )
        .unwrap();
        assert!(
            notices.is_empty(),
            "the file store needs no fallback: {notices:?}"
        );

        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            !text.contains("BEGIN RSA"),
            "private key written inline: {text}"
        );
        assert!(
            !text.contains("hook-secret"),
            "webhook secret written inline: {text}"
        );
        assert!(text.contains("app_pem_ref"), "{text}");

        let loaded = load_config_from(&path).unwrap();
        assert_eq!(loaded.github.app_id.as_deref(), Some("4242"));
        assert_eq!(loaded.github.app_pem(), Some(PEM));
        assert_eq!(loaded.github.webhook_secret(), Some("hook-secret"));
    }
}
