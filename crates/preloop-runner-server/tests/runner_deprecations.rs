//! Integration tests for the runner deprecation lookup API:
//! `GET /api/v3/actions/runners/deprecations/:version` plus the org and repo
//! scoped variants. Mirrors GitHub's September 2026
//! `GET /actions/runners/deprecations/{version}` endpoint.

mod common;

use common::*;

async fn get_deprecation(app: &Router, uri: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(Method::GET).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

async fn test_app() -> (Router, String) {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let token = state.system_token.clone();
    let shutdown = CancellationToken::new();
    (app(state, shutdown), token)
}

/// The endpoint returns GitHub's exact response shape for a known runner
/// version. preloop tracks no end-of-life dates for runner versions (see
/// `runner_deprecations` module docs), so both date fields are null rather
/// than invented.
#[tokio::test]
async fn deprecation_lookup_returns_expected_shape_with_null_dates() {
    let (app, token) = test_app().await;
    for uri in [
        "/api/v3/actions/runners/deprecations/2.336.0".to_owned(),
        "/api/v3/orgs/acme/actions/runners/deprecations/2.336.0".to_owned(),
        "/api/v3/repos/acme/app/actions/runners/deprecations/2.336.0".to_owned(),
    ] {
        let (status, json) = get_deprecation(&app, &uri, Some(&token)).await;
        assert_eq!(status, StatusCode::OK, "uri: {uri}");
        assert_eq!(json["runner_version"], "2.336.0", "uri: {uri}");
        assert!(json["runtime_deprecates_at"].is_null(), "uri: {uri}");
        assert!(json["registration_deprecates_at"].is_null(), "uri: {uri}");
    }
}

/// Older but well-formed runner versions also answer 200 with null dates:
/// preloop has no end-of-life schedule to consult, so it reports "no
/// deprecation scheduled" instead of failing.
#[tokio::test]
async fn deprecation_lookup_old_version_returns_nulls_not_404() {
    let (app, token) = test_app().await;
    let (status, json) = get_deprecation(
        &app,
        "/api/v3/orgs/acme/actions/runners/deprecations/2.329.0",
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["runner_version"], "2.329.0");
    assert!(json["runtime_deprecates_at"].is_null());
    assert!(json["registration_deprecates_at"].is_null());
}

/// A version segment that cannot be a runner version at all is a 404, the
/// way GitHub answers for a version it never released.
#[tokio::test]
async fn deprecation_lookup_malformed_version_is_404() {
    let (app, token) = test_app().await;
    let (status, _) = get_deprecation(
        &app,
        "/api/v3/actions/runners/deprecations/not-a-version",
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// The lookup requires the system token, like the neighboring
/// runner-registration routes: no credential, or the wrong one, is a 401.
#[tokio::test]
async fn deprecation_lookup_requires_system_token() {
    let (app, token) = test_app().await;
    let uri = "/api/v3/actions/runners/deprecations/2.336.0";

    let (status, _) = get_deprecation(&app, uri, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _) = get_deprecation(&app, uri, Some("wrong-token")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _) = get_deprecation(&app, uri, Some(&token)).await;
    assert_eq!(status, StatusCode::OK);
}
