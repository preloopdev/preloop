//! Periodic App webhook configuration health.
//!
//! preloop already reads the App's event subscription back at startup. That
//! catches a misconfigured App on day one and nothing afterwards: someone
//! narrows the event list on day 40, or repoints the delivery URL at a host
//! that no longer exists, and the only symptom is silence — which looks
//! exactly like "nobody pushed anything".
//!
//! So the check runs on a schedule and publishes its verdict into
//! [`crate::webhook_status::WebhookResilienceStatus`], where the operational
//! snapshot turns it into a condition. GitHub deliberately offers no API to
//! change an App's event subscription, so this monitor pages a human; it
//! never pretends it can self-heal.

use std::sync::Arc;
use std::time::Duration;

use crate::state::SharedState;
use crate::webhook_status::{now_us, AppWebhookConfigStatus};

const DEFAULT_INTERVAL_SECS: u64 = 900;
const MIN_INTERVAL_SECS: u64 = 60;

/// Path the webhook handler is mounted on. The expected delivery URL is
/// this appended to the operator's public base URL.
const WEBHOOK_PATH: &str = "/api/v1/github/webhooks";

fn health_interval() -> Duration {
    Duration::from_secs(
        std::env::var("PRELOOP_WEBHOOK_HEALTH_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_INTERVAL_SECS)
            .max(MIN_INTERVAL_SECS),
    )
}

/// The delivery URL GitHub should be calling, when the operator told us what
/// this server is publicly reachable at. Unknown expectation is `None`:
/// reporting drift against a guess would be worse than reporting nothing.
fn expected_webhook_url() -> Option<String> {
    std::env::var("PRELOOP_PUBLIC_URL")
        .ok()
        .map(|base| base.trim().trim_end_matches('/').to_owned())
        .filter(|base| !base.is_empty())
        .map(|base| format!("{base}{WEBHOOK_PATH}"))
}

/// Background loop; one check per interval until shutdown.
pub(crate) async fn run_webhook_health_monitor(shared: Arc<SharedState>) {
    let interval = health_interval();
    loop {
        if shared.shutdown.is_cancelled() {
            break;
        }
        let statuses = check_webhook_config_once(&shared).await;
        for status in &statuses {
            if status.url_drifted() {
                tracing::warn!(
                    app_id = %status.app_id,
                    configured = status.hook_url.as_deref().unwrap_or("<none>"),
                    expected = status.expected_url.as_deref().unwrap_or("<unknown>"),
                    "GitHub App webhook URL does not point at this server; \
                     deliveries are going somewhere else"
                );
            }
        }
        tokio::select! {
            _ = shared.shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

/// Read every configured App's webhook subscription and delivery config, and
/// publish the comparison. Returns what was published.
pub(crate) async fn check_webhook_config_once(
    shared: &Arc<SharedState>,
) -> Vec<AppWebhookConfigStatus> {
    let apps = crate::github_app::registered_apps(&shared.state);
    if apps.is_empty() {
        shared
            .state
            .webhook_status
            .set_app_config(Vec::new(), now_us());
        return Vec::new();
    }
    if shared.state.github_breaker.is_open() {
        // A probe during a known outage would overwrite a good verdict with
        // a transport error and add load to a failing dependency.
        tracing::debug!("GitHub breaker open; skipping App webhook health check");
        return shared.state.webhook_status.app_config();
    }

    let api_base = crate::github::github_api_base();
    let expected_url = expected_webhook_url();
    let mut statuses = Vec::with_capacity(apps.len());
    for app in apps {
        let mut status = AppWebhookConfigStatus {
            app_id: app.app_id.clone(),
            expected_url: expected_url.clone(),
            ..Default::default()
        };
        let mut errors: Vec<String> = Vec::new();

        match crate::github_app::read_app_subscription_at(&api_base, &app.app_id, &app.private_key)
            .await
        {
            Ok(subscription) => {
                // One canonical warning message, shared with the startup
                // read-back: two wordings for the same defect is two things
                // to keep true.
                crate::github_app::warn_missing_trigger_events(&app.app_id, &subscription);
                for missing in crate::github_app::missing_trigger_events(&subscription) {
                    status.missing_events.push(missing.event.to_owned());
                    if !missing.permission_granted
                        && !status
                            .missing_permissions
                            .iter()
                            .any(|permission| permission == missing.permission)
                    {
                        status
                            .missing_permissions
                            .push(missing.permission.to_owned());
                    }
                }
            }
            Err(error) => errors.push(format!("subscription: {error}")),
        }

        match crate::github_app::read_app_hook_config_at(&api_base, &app.app_id, &app.private_key)
            .await
        {
            Ok(config) => status.hook_url = config.url,
            Err(error) => errors.push(format!("hook config: {error}")),
        }

        if !errors.is_empty() {
            status.error = Some(errors.join("; "));
        }
        statuses.push(status);
    }

    shared
        .state
        .webhook_status
        .set_app_config(statuses.clone(), now_us());
    statuses
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde_json::json;

    static TEST_KEY: std::sync::LazyLock<rsa::RsaPrivateKey> = std::sync::LazyLock::new(|| {
        rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).expect("test key")
    });

    async fn stub_github(hook_url: &str, hook_status: axum::http::StatusCode) -> String {
        let hook_url = hook_url.to_owned();
        let router = Router::new()
            .route(
                "/app",
                get(|| async {
                    // A fully-subscribed App, derived from the requirement
                    // list itself: a hard-coded subset would make every
                    // health assertion here fail for the wrong reason the
                    // day preloop starts consuming another event.
                    let events: Vec<&str> = crate::github_app::required_trigger_events()
                        .iter()
                        .map(|required| required.event)
                        .collect();
                    let permissions: serde_json::Map<String, serde_json::Value> =
                        crate::github_app::required_trigger_events()
                            .iter()
                            .map(|required| {
                                (
                                    required.permission.to_owned(),
                                    serde_json::Value::String("write".to_owned()),
                                )
                            })
                            .collect();
                    Json(json!({ "events": events, "permissions": permissions }))
                }),
            )
            .route(
                "/app/hook/config",
                get(move || {
                    let hook_url = hook_url.clone();
                    async move {
                        if !hook_status.is_success() {
                            return (hook_status, Json(json!({"message": "nope"})));
                        }
                        (
                            axum::http::StatusCode::OK,
                            Json(json!({
                                "url": hook_url,
                                "content_type": "json",
                                "insecure_ssl": "0",
                            })),
                        )
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        format!("http://{addr}")
    }

    async fn shared_with_app() -> Arc<SharedState> {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.keep();
        let mut state = crate::AppState::new(path).await.unwrap();
        state.github_app = Some(crate::github_app::GitHubAppCredentials::for_tests(
            "424",
            TEST_KEY.clone(),
            crate::github_app::MintFailurePolicy::LocalJwt,
        ));
        Arc::new(SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        })
    }

    #[tokio::test]
    async fn reports_url_drift_against_the_public_url() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let api_base = stub_github(
            "https://elsewhere.example/api/v1/github/webhooks",
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let _public = crate::state::TestEnvVar::set("PRELOOP_PUBLIC_URL", "https://here.example");
        let shared = shared_with_app().await;

        let statuses = check_webhook_config_once(&shared).await;

        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].url_drifted(), "{statuses:?}");
        assert!(!statuses[0].healthy());
        assert_eq!(
            shared.state.webhook_status.app_config().len(),
            1,
            "the verdict must be published, not just returned"
        );
    }

    #[tokio::test]
    async fn matching_url_modulo_trailing_slash_is_not_drift() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let api_base = stub_github(
            "https://here.example/api/v1/github/webhooks/",
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let _public = crate::state::TestEnvVar::set("PRELOOP_PUBLIC_URL", "https://here.example/");
        let shared = shared_with_app().await;

        let statuses = check_webhook_config_once(&shared).await;

        assert!(!statuses[0].url_drifted(), "{statuses:?}");
        assert!(statuses[0].healthy(), "{statuses:?}");
    }

    #[tokio::test]
    async fn unreadable_hook_config_is_an_error_not_a_clean_bill_of_health() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let api_base = stub_github(
            "https://here.example/api/v1/github/webhooks",
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let _public = crate::state::TestEnvVar::set("PRELOOP_PUBLIC_URL", "https://here.example");
        let shared = shared_with_app().await;

        let statuses = check_webhook_config_once(&shared).await;

        assert!(statuses[0].error.is_some(), "{statuses:?}");
        assert!(!statuses[0].healthy());
        assert!(statuses[0].hook_url.is_none());
    }
}
