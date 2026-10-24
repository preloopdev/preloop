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
use crate::webhook_status::{AppWebhookConfigStatus, now_us};

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
pub async fn run_webhook_health_monitor(shared: Arc<SharedState>) {
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
pub async fn check_webhook_config_once(shared: &Arc<SharedState>) -> Vec<AppWebhookConfigStatus> {
    let apps = crate::github_app::registered_apps(&shared.state);
    if apps.is_empty() {
        shared
            .state
            .webhook_status
            .set_app_config(Vec::new(), Some(now_us()));
        return Vec::new();
    }
    // The verdicts published last pass. An App held back by its breaker is not
    // re-checked and keeps its previous verdict, so the published list still
    // covers every App.
    let (previous, previous_checked_at_us) = shared.state.webhook_status.app_config_snapshot();
    let app_count = apps.len();
    let api_base = crate::github::github_api_base();
    let expected_url = expected_webhook_url();
    let mut statuses = Vec::with_capacity(apps.len());
    let mut checked = 0usize;
    for app in apps {
        let bucket = crate::github_breaker::GithubCredentialBucket::for_app(&app.app_id);
        if let Some(retry_after) = shared.state.github_breakers.retry_after_for(bucket) {
            tracing::debug!(
                app_id = %app.app_id,
                retry_in_secs = retry_after.as_secs(),
                "GitHub breaker open or probe in progress; skipping App webhook health check"
            );
            let kept: Vec<AppWebhookConfigStatus> = previous
                .iter()
                .filter(|status| status.app_id == app.app_id)
                .cloned()
                .collect();
            if kept.is_empty() {
                // Nothing was ever published for this App, so there is no
                // verdict to keep — but omitting it would make a blocked App
                // invisible exactly when its wiring matters. Publish an
                // explicitly unchecked verdict instead, with no check time.
                statuses.push(AppWebhookConfigStatus {
                    app_id: app.app_id.clone(),
                    expected_url: expected_url.clone(),
                    error: Some("not checked: GitHub breaker open or probe in progress".to_owned()),
                    ..Default::default()
                });
            } else {
                statuses.extend(kept);
            }
            continue;
        }
        checked += 1;
        let mut status = AppWebhookConfigStatus {
            app_id: app.app_id.clone(),
            expected_url: expected_url.clone(),
            checked_at_us: Some(now_us()),
            ..Default::default()
        };
        let mut errors: Vec<String> = Vec::new();

        match crate::github_app::read_app_subscription_at_with_breaker(
            &api_base,
            &app.app_id,
            &app.private_key,
            Some((
                &shared.state.github_breakers,
                &shared.state.github_consumption,
            )),
        )
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

        match crate::github_app::read_app_hook_config_at_with_breaker(
            &api_base,
            &app.app_id,
            &app.private_key,
            Some((
                &shared.state.github_breakers,
                &shared.state.github_consumption,
            )),
        )
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

    if checked == 0 {
        // Every App is held back by its breaker: publish nothing new.
        return previous;
    }
    // The list is verified as a whole only when every configured App was
    // actually read this pass. A partial pass keeps the previous publication
    // time: reusing a verdict must not make it look freshly checked.
    let checked_at_us = if checked == app_count {
        Some(now_us())
    } else {
        previous_checked_at_us
    };
    shared
        .state
        .webhook_status
        .set_app_config(statuses.clone(), checked_at_us);
    statuses
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde_json::json;

    static TEST_KEY: std::sync::LazyLock<rsa::RsaPrivateKey> = std::sync::LazyLock::new(|| {
        rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).expect("test key")
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

    async fn shared_with_app() -> (tempfile::TempDir, Arc<SharedState>) {
        let temp = tempfile::tempdir().unwrap();
        let mut state = crate::AppState::new(temp.path().to_path_buf())
            .await
            .unwrap();
        state.github_app = Some(crate::github_app::GitHubAppCredentials::for_tests(
            "424",
            TEST_KEY.clone(),
            crate::github_app::MintFailurePolicy::LocalJwt,
        ));
        (
            temp,
            Arc::new(SharedState {
                state,
                shutdown: tokio_util::sync::CancellationToken::new(),
            }),
        )
    }

    async fn shared_with_two_apps() -> (tempfile::TempDir, Arc<SharedState>) {
        let temp = tempfile::tempdir().unwrap();
        let mut state = crate::AppState::new(temp.path().to_path_buf())
            .await
            .unwrap();
        state.github_apps = Some(crate::github_app::GitHubApps {
            apps: vec![
                crate::github_app::GitHubAppCredentials::for_tests(
                    "424",
                    TEST_KEY.clone(),
                    crate::github_app::MintFailurePolicy::LocalJwt,
                ),
                crate::github_app::GitHubAppCredentials::for_tests(
                    "525",
                    TEST_KEY.clone(),
                    crate::github_app::MintFailurePolicy::LocalJwt,
                ),
            ],
            default_index: 0,
        });
        (
            temp,
            Arc::new(SharedState {
                state,
                shutdown: tokio_util::sync::CancellationToken::new(),
            }),
        )
    }

    /// Trip one App's own breaker bucket the way a 429 would.
    fn park_app(shared: &Arc<SharedState>, app_id: &str) {
        shared
            .state
            .github_breakers
            .breaker_for(crate::github_breaker::GithubCredentialBucket::for_app(
                app_id,
            ))
            .record_failure(
                &crate::github_breaker::GithubFailureKind::RateLimited {
                    retry_after: Some(Duration::from_secs(60)),
                },
                "429",
            );
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
        let (_temp, shared) = shared_with_app().await;

        let statuses = check_webhook_config_once(&shared).await;

        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].url_drifted(), "{statuses:?}");
        assert!(!statuses[0].healthy());
        assert_eq!(
            shared.state.webhook_status.app_config_snapshot().0.len(),
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
        let (_temp, shared) = shared_with_app().await;

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
        let (_temp, shared) = shared_with_app().await;

        let statuses = check_webhook_config_once(&shared).await;

        assert!(statuses[0].error.is_some(), "{statuses:?}");
        assert!(!statuses[0].healthy());
        assert!(statuses[0].hook_url.is_none());
    }

    /// Two Apps, the first behind an open breaker: only the second is checked,
    /// and the published list still covers both — the held-back App appears
    /// as explicitly unchecked rather than vanishing from the snapshot.
    #[tokio::test]
    async fn an_app_behind_an_open_breaker_is_not_checked_while_the_others_are() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let api_base = stub_github(
            "https://here.example/api/v1/github/webhooks",
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let _public = crate::state::TestEnvVar::set("PRELOOP_PUBLIC_URL", "https://here.example");
        let (_temp, shared) = shared_with_two_apps().await;
        park_app(&shared, "424");

        let statuses = check_webhook_config_once(&shared).await;

        let ids: Vec<&str> = statuses
            .iter()
            .map(|status| status.app_id.as_str())
            .collect();
        assert_eq!(ids, ["424", "525"], "{statuses:?}");
        assert!(
            statuses[0].error.is_some(),
            "the held-back App must say it was not checked: {statuses:?}"
        );
        assert!(!statuses[0].healthy(), "{statuses:?}");
        assert!(statuses[1].healthy(), "{statuses:?}");
        assert!(
            statuses[0].checked_at_us.is_none(),
            "an App that was never checked has no check time: {statuses:?}"
        );
        assert!(statuses[1].checked_at_us.is_some(), "{statuses:?}");
    }

    /// An App held back by its breaker is not re-checked, so the verdict it
    /// published last pass stays in the list.
    #[tokio::test]
    async fn an_app_held_back_keeps_its_last_published_verdict() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let api_base = stub_github(
            "https://here.example/api/v1/github/webhooks",
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let _public = crate::state::TestEnvVar::set("PRELOOP_PUBLIC_URL", "https://here.example");
        let (_temp, shared) = shared_with_two_apps().await;
        let published = check_webhook_config_once(&shared).await;
        let first_checked_at = shared.state.webhook_status.app_config_snapshot().1;
        park_app(&shared, "424");
        tokio::time::sleep(Duration::from_millis(5)).await;

        let statuses = check_webhook_config_once(&shared).await;

        let ids: Vec<&str> = statuses
            .iter()
            .map(|status| status.app_id.as_str())
            .collect();
        assert_eq!(ids, ["424", "525"], "{statuses:?}");
        assert_eq!(statuses[0], published[0]);
        assert!(
            statuses[1].checked_at_us > published[1].checked_at_us,
            "the re-checked App's verdict must advance: {statuses:?}"
        );
        assert_eq!(
            shared.state.webhook_status.app_config_snapshot().1,
            first_checked_at,
            "a partial pass must not make the whole list look freshly checked"
        );
    }

    /// When every App is held back the pass checks nothing, so it publishes
    /// nothing and the publication time does not move.
    #[tokio::test]
    async fn every_app_held_back_leaves_the_published_snapshot_untouched() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let api_base = stub_github(
            "https://here.example/api/v1/github/webhooks",
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let _public = crate::state::TestEnvVar::set("PRELOOP_PUBLIC_URL", "https://here.example");
        let (_temp, shared) = shared_with_two_apps().await;
        let published = check_webhook_config_once(&shared).await;
        let checked_at = shared.state.webhook_status.app_config_snapshot().1;
        park_app(&shared, "424");
        park_app(&shared, "525");

        let statuses = check_webhook_config_once(&shared).await;

        assert_eq!(statuses, published);
        assert_eq!(
            shared.state.webhook_status.app_config_snapshot().1,
            checked_at,
            "a pass that checked nothing must not move the publication time"
        );
    }
}
