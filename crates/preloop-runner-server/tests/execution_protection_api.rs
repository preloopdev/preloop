//! preloop-runner-server integration tests — execution protection rule
//! management API (`/api/v1/execution-protection`).

mod common;

use common::*;

struct Fixture {
    _temp: tempfile::TempDir,
    config_path: std::path::PathBuf,
    app: Router,
}

async fn fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    let state = AppState::new_with_config(temp.path().to_path_buf(), config_path.clone())
        .await
        .unwrap();
    let app = app(state, CancellationToken::new());
    Fixture {
        _temp: temp,
        config_path,
        app,
    }
}

const POLICY: &str = "/api/v1/execution-protection";
const RULES: &str = "/api/v1/execution-protection/rules";
const SYSTEM: &str = "preloop-system-token";

#[tokio::test]
async fn policy_endpoint_shows_default_prt_rule_in_evaluate_mode() {
    let fixture = fixture().await;
    let policy =
        request_json_with_bearer(&fixture.app, Method::GET, POLICY, Value::Null, SYSTEM).await;
    assert_eq!(policy["mode"], "evaluate");
    let rules = policy["rules"].as_array().expect("rules array");
    assert_eq!(rules.len(), 1, "fresh config must carry the default rule");
    assert_eq!(rules[0]["id"], "event-0");
    assert_eq!(rules[0]["kind"], "event");
    assert_eq!(rules[0]["event"], "pull_request_target");
    assert_eq!(rules[0]["action"], "deny");
}

#[tokio::test]
async fn management_routes_reject_anything_but_the_system_token() {
    let fixture = fixture().await;
    // No bearer at all.
    assert_eq!(
        request_status_without_bearer(&fixture.app, Method::GET, POLICY, Value::Null).await,
        StatusCode::UNAUTHORIZED
    );
    // Wrong bearer.
    assert_eq!(
        request_status_with_bearer(&fixture.app, Method::GET, POLICY, Value::Null, "nope").await,
        StatusCode::UNAUTHORIZED
    );
    // A job's runtime token is a valid local JWT but not the system token.
    let job_token = mint_runtime_token("plan", &uuid::Uuid::new_v4());
    assert_ne!(job_token, SYSTEM);
    assert_eq!(
        request_status_with_bearer(&fixture.app, Method::GET, POLICY, Value::Null, &job_token)
            .await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request_status_with_bearer(
            &fixture.app,
            Method::POST,
            RULES,
            json!({ "kind": "event", "event": "push" }),
            &job_token,
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    // The system token is admitted.
    assert_eq!(
        request_status_with_bearer(&fixture.app, Method::GET, POLICY, Value::Null, SYSTEM).await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn rule_crud_round_trip() {
    let fixture = fixture().await;

    // Create an actor rule: positional id actor-0.
    let created = request_json_with_bearer(
        &fixture.app,
        Method::POST,
        RULES,
        json!({ "kind": "actor", "actor": "mallory", "workflows": ["deploy.yml"] }),
        SYSTEM,
    )
    .await;
    assert_eq!(created["id"], "actor-0");
    assert_eq!(created["kind"], "actor");
    assert_eq!(created["actor"], "mallory");
    assert_eq!(created["workflows"], json!(["deploy.yml"]));
    assert_eq!(created["action"], "deny");

    // The list now holds the default rule plus the new one.
    let listed =
        request_json_with_bearer(&fixture.app, Method::GET, RULES, Value::Null, SYSTEM).await;
    let rules = listed["rules"].as_array().expect("rules array");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0]["id"], "event-0");
    assert_eq!(rules[1]["id"], "actor-0");

    // Read one.
    let one = request_json_with_bearer(
        &fixture.app,
        Method::GET,
        &format!("{RULES}/actor-0"),
        Value::Null,
        SYSTEM,
    )
    .await;
    assert_eq!(one, created);

    // Update it.
    let updated = request_json_with_bearer(
        &fixture.app,
        Method::PUT,
        &format!("{RULES}/actor-0"),
        json!({ "kind": "actor", "actor": "mallory", "workflows": ["release/*.yml"] }),
        SYSTEM,
    )
    .await;
    assert_eq!(updated["id"], "actor-0");
    assert_eq!(updated["workflows"], json!(["release/*.yml"]));

    // A body of the wrong kind is rejected, not silently converted.
    assert_eq!(
        request_status_with_bearer(
            &fixture.app,
            Method::PUT,
            &format!("{RULES}/actor-0"),
            json!({ "kind": "event", "event": "push" }),
            SYSTEM,
        )
        .await,
        StatusCode::BAD_REQUEST
    );

    // Delete it: the list is back to the default rule alone.
    let deleted = request_json_with_bearer(
        &fixture.app,
        Method::DELETE,
        &format!("{RULES}/actor-0"),
        Value::Null,
        SYSTEM,
    )
    .await;
    assert_eq!(deleted["ok"], true);
    let listed =
        request_json_with_bearer(&fixture.app, Method::GET, RULES, Value::Null, SYSTEM).await;
    assert_eq!(listed["rules"].as_array().unwrap().len(), 1);

    // Unknown ids 404.
    assert_eq!(
        request_status_with_bearer(
            &fixture.app,
            Method::GET,
            &format!("{RULES}/actor-9"),
            Value::Null,
            SYSTEM,
        )
        .await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request_status_with_bearer(
            &fixture.app,
            Method::DELETE,
            &format!("{RULES}/bogus"),
            Value::Null,
            SYSTEM,
        )
        .await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn created_rules_persist_to_the_config_file() {
    let fixture = fixture().await;
    request_json_with_bearer(
        &fixture.app,
        Method::POST,
        RULES,
        json!({ "kind": "event", "event": "workflow_dispatch", "workflows": ["deploy.yml"] }),
        SYSTEM,
    )
    .await;

    // The file on disk holds the rule: a reloaded config sees it.
    let reloaded = load_config_from(&fixture.config_path).unwrap();
    assert_eq!(reloaded.execution_protection.event_rules.len(), 2);
    assert_eq!(
        reloaded.execution_protection.event_rules[1].event,
        "workflow_dispatch"
    );

    // A fresh engine pointed at the same file (the restart equivalent)
    // picks the rule up.
    let rebooted = AppState::new_with_config(
        fixture._temp.path().to_path_buf(),
        fixture.config_path.clone(),
    )
    .await
    .unwrap();
    assert_eq!(rebooted.execution_protection.event_rules.len(), 2);
    assert_eq!(
        rebooted.execution_protection.event_rules[1].event,
        "workflow_dispatch"
    );
}

#[tokio::test]
async fn deleting_the_default_rule_sticks_across_reload() {
    let fixture = fixture().await;
    request_json_with_bearer(
        &fixture.app,
        Method::DELETE,
        &format!("{RULES}/event-0"),
        Value::Null,
        SYSTEM,
    )
    .await;

    // The delete writes an explicit empty list, so the default does not
    // reappear on the next load.
    let reloaded = load_config_from(&fixture.config_path).unwrap();
    assert!(reloaded.execution_protection.event_rules.is_empty());
    let policy =
        request_json_with_bearer(&fixture.app, Method::GET, POLICY, Value::Null, SYSTEM).await;
    assert_eq!(policy["rules"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn mode_flip_persists() {
    let fixture = fixture().await;
    let mode = request_json_with_bearer(
        &fixture.app,
        Method::PUT,
        "/api/v1/execution-protection/mode",
        json!({ "mode": "enforce" }),
        SYSTEM,
    )
    .await;
    assert_eq!(mode["mode"], "enforce");
    let policy =
        request_json_with_bearer(&fixture.app, Method::GET, POLICY, Value::Null, SYSTEM).await;
    assert_eq!(policy["mode"], "enforce");
    assert_eq!(
        load_config_from(&fixture.config_path)
            .unwrap()
            .execution_protection
            .mode,
        ProtectionMode::Enforce
    );

    // Unknown modes fail closed with a 400, not a silent no-op.
    assert_eq!(
        request_status_with_bearer(
            &fixture.app,
            Method::PUT,
            "/api/v1/execution-protection/mode",
            json!({ "mode": "audit" }),
            SYSTEM,
        )
        .await,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn create_rejects_input_config_load_would_reject() {
    let fixture = fixture().await;
    for body in [
        // Character-class globs would silently miss at match time.
        json!({ "kind": "event", "event": "push", "workflows": ["deploy-[0-9].yml"] }),
        // Only "deny" is a supported action.
        json!({ "kind": "event", "event": "push", "action": "allow" }),
        // An event rule needs its event.
        json!({ "kind": "event" }),
        // An actor rule needs its actor.
        json!({ "kind": "actor", "event": "push" }),
        // Unknown rule kind.
        json!({ "kind": "repository", "event": "push" }),
    ] {
        assert_eq!(
            request_status_with_bearer(&fixture.app, Method::POST, RULES, body, SYSTEM).await,
            StatusCode::BAD_REQUEST,
            "bad rule body must be rejected"
        );
    }
    // Nothing was persisted by the rejected writes.
    let listed =
        request_json_with_bearer(&fixture.app, Method::GET, RULES, Value::Null, SYSTEM).await;
    assert_eq!(listed["rules"].as_array().unwrap().len(), 1);
}
