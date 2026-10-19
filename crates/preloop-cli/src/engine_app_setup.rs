//! `preloop setup github --via app` when a public URL is set: the running
//! engine hosts the App-creation form and converts GitHub's code itself.
//!
//! This is the CLI side. It opens a setup session on the engine, prints the
//! one-time link, and waits for the engine to report how the setup ended. The
//! App's private key and webhook secret are stored by the engine and never
//! pass through this process.

use crate::github_setup::GithubSetupArgs;
use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

/// How often the CLI asks the engine for the session's status.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// A ceiling on waiting, in case the engine stops answering. The engine
/// expires a session itself (ten minutes for the link, ten more for approval),
/// so a healthy engine always ends the wait first.
const WAIT_LIMIT: Duration = Duration::from_secs(30 * 60);

/// Create the App on the engine at `public_url`. Returns once the engine
/// reports the setup completed, or an error saying why it did not.
pub(crate) async fn setup_app_via_engine(args: &GithubSetupArgs, public_url: &str) -> Result<()> {
    if let Some(error) = insecure_public_url_error(public_url) {
        bail!("{error}");
    }
    let engine = crate::server_url();
    let engine = engine.trim_end_matches('/');
    let client = crate::build_client();

    let mut request = client
        .post(format!("{engine}/api/v1/github/setup-sessions"))
        .json(&json!({
            "public_url": public_url,
            "org": args.org,
            "app_name": args.app_name,
            // The engine decides on its own config whether an App already
            // exists, which is what the local flow checks in its own file.
            "add": args.add,
            // Verified by the engine, which ends up holding the private key.
            "repos": args.repos,
        }));
    if let Some(token) = crate::api_token() {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|error| unreachable_engine(engine, &error))?;
    let opened = read_reply(response, engine).await?;
    let setup_url = opened["setup_url"]
        .as_str()
        .context("the engine's reply has no setup_url")?;
    let session = opened["id"]
        .as_str()
        .context("the engine's reply has no session id")?;

    println!(
        "Open this link in a browser that can reach the engine. It works once and expires in about ten minutes:\n\n  {setup_url}\n"
    );
    println!(
        "Approve the App on GitHub. The engine creates it and stores its credentials; this terminal waits for that."
    );
    if !args.repos.is_empty() {
        println!(
            "Install the App on {} as soon as GitHub offers the install page: the engine waits \
             up to five minutes for it, then verifies each one.",
            args.repos.join(", ")
        );
    }
    if !args.no_browser && crate::app_manifest::open_in_browser(setup_url).is_err() {
        println!("(could not open a browser automatically — open the link above)");
    }
    wait_for_completion(engine, session).await
}

/// Poll the session until the engine reports a terminal status.
async fn wait_for_completion(engine: &str, session: &str) -> Result<()> {
    let client = crate::build_client();
    let started = Instant::now();
    let mut announced = String::new();
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        if started.elapsed() > WAIT_LIMIT {
            bail!(
                "gave up waiting for the engine after {} minutes; run `preloop setup github --via app` again",
                WAIT_LIMIT.as_secs() / 60
            );
        }
        let mut request = client.get(format!("{engine}/api/v1/github/setup-sessions/{session}"));
        if let Some(token) = crate::api_token() {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|error| unreachable_engine(engine, &error))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            bail!(
                "the setup session ended without a result: the engine restarted, or a newer setup replaced it. Run `preloop setup github --via app` again"
            );
        }
        let status = read_reply(response, engine).await?;
        let state = status["status"].as_str().unwrap_or_default();
        match state {
            "completed" => {
                let (lines, failed) = completion_report(&status);
                for line in lines {
                    println!("{line}");
                }
                anyhow::ensure!(!failed, "one or more App verification checks failed");
                return Ok(());
            }
            "failed" => bail!(
                "{}",
                status["error"]
                    .as_str()
                    .unwrap_or("the engine could not finish the App setup")
            ),
            // The engine reports one `expired` for both ten-minute windows:
            // the link was never opened, or the form was served but GitHub's
            // approval never came back. Say both, and what an App that exists
            // on GitHub anyway means — its key was never stored.
            "expired" => bail!(
                "the setup expired: the link was not opened, or the App was not approved on GitHub, \
                 within ten minutes. If GitHub created the App, its private key was never stored — \
                 delete it in its settings, then run `preloop setup github --via app` again"
            ),
            _ if state != announced => {
                announced = state.to_owned();
                match state {
                    "awaiting_link" => println!("Waiting for the link to be opened…"),
                    "awaiting_approval" => {
                        println!(
                            "The form was opened. Waiting for the App to be approved on GitHub…"
                        )
                    }
                    "converting" => {
                        println!("GitHub approved the App. Storing its credentials on the engine…")
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
}

/// What the operator is told once the engine has stored the App. The status
/// carries only non-secret facts: the App id, its install page, whether a
/// webhook secret was stored, and the `--repo` checks the engine ran.
///
/// Returns the lines to print, and whether every repository check passed —
/// a failed check is reported after the App exists, exactly as the loopback
/// flow's `doctor` does, so the command has to fail without pretending the
/// App was not created.
fn completion_report(status: &Value) -> (Vec<String>, bool) {
    let mut lines = vec![match status["app_id"].as_u64() {
        Some(app_id) => format!("Created App {app_id} and stored its credentials on the engine."),
        None => "Created the App and stored its credentials on the engine.".to_owned(),
    }];
    if status["webhook_secret_stored"].as_bool() == Some(true) {
        lines.push("Webhook secret stored — signed deliveries will verify.".to_owned());
    }
    if let Some(url) = status["install_url"].as_str() {
        lines.push(format!(
            "Install it on your repositories to finish:\n  {url}"
        ));
    }
    let checks = status["repo_checks"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let failed = checks
        .iter()
        .any(|check| check["ok"].as_bool() != Some(true));
    for check in &checks {
        if let Some(line) = check["line"].as_str() {
            lines.push(line.to_owned());
        }
    }
    lines.push("Restart the engine (`preloop serve`) to pick up the config.".to_owned());
    if checks.is_empty() {
        lines.push(
            "Tip: verify with `preloop doctor --repo owner/name` on the engine's host.".to_owned(),
        );
    }
    (lines, failed)
}

/// The engine's JSON reply, or the engine's reason for refusing.
async fn read_reply(response: reqwest::Response, engine: &str) -> Result<Value> {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if status.is_success() {
        return serde_json::from_str(&body).context("the engine's reply was not JSON");
    }
    let message = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| value["error"].as_str().map(str::to_owned))
        .unwrap_or_else(|| body.trim().to_owned());
    match status {
        reqwest::StatusCode::UNAUTHORIZED => bail!(
            "the engine at {engine} rejected the API token ({message}); set PRELOOP_TOKEN to its system token"
        ),
        reqwest::StatusCode::NOT_FOUND => bail!(
            "the engine at {engine} does not serve engine-hosted App setup; upgrade it, or run without a public URL to create the App on this machine"
        ),
        _ => bail!("the engine at {engine} refused the App setup ({status}): {message}"),
    }
}

fn unreachable_engine(engine: &str, error: &reqwest::Error) -> anyhow::Error {
    anyhow!(
        "cannot reach the engine at {engine} ({error}); start it with `preloop serve`, or point PRELOOP_URL at it"
    )
}

/// The refusal for a public URL that is plain http on a non-loopback host.
///
/// The setup link carries a one-time token and GitHub's redirect carries the
/// App's one-time code, so over http anyone on the path could redeem them
/// first. The engine refuses the same URLs; failing here says why without a
/// round trip. Loopback http never leaves the machine, so it stays allowed.
pub(crate) fn insecure_public_url_error(public_url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(public_url).ok()?;
    let loopback = parsed.host_str().is_some_and(|host| {
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    (parsed.scheme() == "http" && !loopback).then(|| {
        format!(
            "{public_url} is plain http: the one-time setup link would travel in clear text. \
             Reach this engine over https (a reverse proxy or tunnel with a certificate), or use \
             a loopback address."
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plain_http_on_a_remote_host_is_refused() {
        assert!(insecure_public_url_error("http://ci.example.com").is_some());
        assert!(insecure_public_url_error("https://ci.example.com").is_none());
        assert!(insecure_public_url_error("http://127.0.0.1:9090").is_none());
        assert!(insecure_public_url_error("http://localhost:9090").is_none());
        assert!(insecure_public_url_error("http://[::1]:9090").is_none());
        assert!(
            insecure_public_url_error("http://[::1]:8765/preloop").is_none(),
            "IPv6 loopback with a path is still local"
        );
        assert!(
            insecure_public_url_error("not a url").is_none(),
            "the engine rejects it with a clear error instead"
        );
    }

    #[test]
    fn completion_names_the_app_and_its_install_page() {
        let status = json!({
            "status": "completed",
            "app_id": 987654,
            "install_url": "https://github.com/apps/preloop-local/installations/new",
            "webhook_secret_stored": true,
        });
        let (lines, failed) = completion_report(&status);
        let text = lines.join("\n");
        assert!(!failed, "no checks were requested, so none failed");
        assert!(text.contains("Created App 987654"), "{text}");
        assert!(
            text.contains("https://github.com/apps/preloop-local/installations/new"),
            "{text}"
        );
        assert!(text.contains("Webhook secret stored"), "{text}");
        assert!(text.contains("Restart the engine"), "{text}");
        assert!(
            !text.to_lowercase().contains("pem") && !text.to_lowercase().contains("private key"),
            "the CLI never receives or prints key material: {text}"
        );
    }

    #[test]
    fn completion_without_an_app_id_still_reports_success() {
        let (lines, failed) = completion_report(&json!({"status": "completed"}));
        assert!(!failed);
        assert!(lines[0].starts_with("Created the App"), "{lines:?}");
        assert!(
            lines.iter().all(|line| !line.contains("Webhook")),
            "{lines:?}"
        );
    }

    #[test]
    fn engine_side_repo_checks_are_printed_and_decide_the_exit() {
        let ok = json!({
            "status": "completed",
            "app_id": 4242,
            "repo_checks": [
                {"repo": "acme/private", "ok": true, "line": "✓ App 4242 on acme/private: contents: read"},
            ],
        });
        let (lines, failed) = completion_report(&ok);
        let text = lines.join("\n");
        assert!(!failed, "{text}");
        assert!(text.contains("✓ App 4242 on acme/private"), "{text}");
        assert!(
            !text.contains("preloop doctor --repo"),
            "the tip is redundant once the engine ran the checks: {text}"
        );

        let bad = json!({
            "status": "completed",
            "app_id": 4242,
            "repo_checks": [
                {"repo": "acme/missing", "ok": false, "line": "✗ App 4242 on acme/missing: installation not found"},
            ],
        });
        let (lines, failed) = completion_report(&bad);
        assert!(failed, "a failed check has to fail the command");
        assert!(
            lines.join("\n").contains("✗ App 4242 on acme/missing"),
            "{lines:?}"
        );
    }
}
