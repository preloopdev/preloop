//! Operator REST API for workflow execution protections
//! (`/api/v1/execution-protection`).
//!
//! Mirrors the rule-management half of GitHub's execution protections: list,
//! create, read, update, and delete the event/actor deny rules, and read or
//! replace the table mode (`evaluate` / `enforce`).
//!
//! Design constraints, carried over from the enforcement core
//! ([`crate::execution_protection`]):
//!
//! - **Rules are server-config-owned.** The API is a management surface over
//!   the operator's config file (`[execution_protection]`), not a database:
//!   every mutation loads the file, applies the change, re-runs the same
//!   validation as config load, and writes the file back atomically, so the
//!   rules survive restarts. Workflow authors can never reach these routes —
//!   they sit behind the system-token-only guard, and nothing here reads or
//!   writes workflow repos.
//! - **No hot reload.** Like every other policy table in the config file,
//!   changes take effect on engine restart (the config is read at startup).
//!   Editing the file by hand behaves identically.
//! - **Rule IDs are positional** (`event-0`, `actor-1`, ...): the index of
//!   the rule inside its kind list. They shift when an earlier rule of the
//!   same kind is deleted — there is no stable database behind them, so a
//!   client that caches IDs should re-list after a delete.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::{
    ActorRule, EventRule, ExecutionProtectionConfig, PolicyRuleAction, ProtectionMode,
};
use crate::errors::ApiError;
use crate::state::SharedState;

/// Positional rule id: `<kind>-<index>`, e.g. `event-0`, `actor-2`.
fn rule_id(kind: &str, index: usize) -> String {
    format!("{kind}-{index}")
}

/// Parse a positional rule id back into its kind list and index.
fn parse_rule_id(id: &str) -> Option<(&str, usize)> {
    let (kind, index) = id.rsplit_once('-')?;
    if kind != "event" && kind != "actor" {
        return None;
    }
    Some((kind, index.parse::<usize>().ok()?))
}

#[derive(Serialize)]
struct RuleView<'a> {
    id: String,
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    event: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actor: Option<&'a str>,
    workflows: Option<&'a Vec<String>>,
    action: &'static str,
}

fn event_rule_view(index: usize, rule: &EventRule) -> RuleView<'_> {
    RuleView {
        id: rule_id("event", index),
        kind: "event",
        event: Some(rule.event.as_str()),
        actor: None,
        workflows: rule.workflows.as_ref(),
        action: match rule.action {
            PolicyRuleAction::Deny => "deny",
        },
    }
}

fn actor_rule_view(index: usize, rule: &ActorRule) -> RuleView<'_> {
    RuleView {
        id: rule_id("actor", index),
        kind: "actor",
        event: None,
        actor: Some(rule.actor.as_str()),
        workflows: rule.workflows.as_ref(),
        action: match rule.action {
            PolicyRuleAction::Deny => "deny",
        },
    }
}

fn policy_view(policy: &ExecutionProtectionConfig) -> serde_json::Value {
    let rules: Vec<RuleView> = policy
        .event_rules
        .iter()
        .enumerate()
        .map(|(index, rule)| event_rule_view(index, rule))
        .chain(
            policy
                .actor_rules
                .iter()
                .enumerate()
                .map(|(index, rule)| actor_rule_view(index, rule)),
        )
        .collect();
    serde_json::json!({
        "mode": policy.mode,
        "rules": rules,
    })
}

/// Load the operator's current `[execution_protection]` table from the
/// config file — the same file the engine read at startup.
fn load_policy(shared: &Arc<SharedState>) -> Result<ExecutionProtectionConfig, ApiError> {
    let config = crate::config::load_config_from(&shared.state.config_path)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
    Ok(config.execution_protection)
}

/// Load the operator config, apply `mutate` to its `[execution_protection]`
/// table, re-validate, and persist the file atomically.
///
/// Serialized on `policy_mutation` so two concurrent writes cannot each load
/// the same base config and drop the other's rules from the file. A rule
/// the API accepts must also survive a restart, so the same validation as
/// config load runs before the write — anything that would fail startup is
/// rejected here instead.
///
/// The running engine keeps the copy it loaded at startup: the change takes
/// effect on restart, exactly like hand-editing the file.
async fn mutate_policy(
    shared: &Arc<SharedState>,
    mutate: impl FnOnce(&mut ExecutionProtectionConfig) -> Result<serde_json::Value, ApiError>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let _guard = shared.state.policy_mutation.lock().await;
    let mut config = crate::config::load_config_from(&shared.state.config_path)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
    // `secrets_store = "memory"` only governs where secret values are kept;
    // execution-protection rules are operator policy, not secrets, so they
    // always persist to the config file, exactly as the task requires.
    let response = mutate(&mut config.execution_protection)?;
    crate::config::validate_execution_protection_rules(&config.execution_protection)
        .map_err(|error| ApiError::bad_request(format!("{error:#}")))?;
    crate::config::write_config_to(&shared.state.config_path, &config)
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
    Ok(Json(response))
}

/// Read the whole execution-protection policy: mode plus every rule.
pub async fn get_execution_protection_policy(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    Ok(Json(policy_view(&load_policy(&shared)?)))
}

#[derive(Deserialize)]
pub struct ModeBody {
    #[serde(default)]
    pub mode: String,
}

/// Replace the table mode (`evaluate` / `enforce`).
pub async fn set_execution_protection_mode(
    State(shared): State<Arc<SharedState>>,
    Json(body): Json<ModeBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mode = match body.mode.as_str() {
        "evaluate" => ProtectionMode::Evaluate,
        "enforce" => ProtectionMode::Enforce,
        other => {
            return Err(ApiError::bad_request(format!(
                "unknown mode {other:?}: expected \"evaluate\" or \"enforce\""
            )));
        }
    };
    mutate_policy(&shared, |policy| {
        policy.mode = mode;
        Ok(serde_json::json!({ "mode": policy.mode }))
    })
    .await
}

/// List every execution-protection rule, each with its positional id.
pub async fn list_execution_protection_rules(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let policy = load_policy(&shared)?;
    let rules: Vec<RuleView> = policy
        .event_rules
        .iter()
        .enumerate()
        .map(|(index, rule)| event_rule_view(index, rule))
        .chain(
            policy
                .actor_rules
                .iter()
                .enumerate()
                .map(|(index, rule)| actor_rule_view(index, rule)),
        )
        .collect();
    Ok(Json(serde_json::json!({ "rules": rules })))
}

#[derive(Deserialize)]
pub struct RuleBody {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub event: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub workflows: Option<Vec<String>>,
    #[serde(default)]
    pub action: Option<String>,
}

enum NewRule {
    Event(EventRule),
    Actor(ActorRule),
}

/// Validate a create/update body into a config rule. The only supported
/// action is `deny` (anything else fails closed); workflow patterns get the
/// same character-class rejection as config load so the API can never write
/// a rule the matcher silently misses.
fn validate_rule_body(body: &RuleBody) -> Result<NewRule, ApiError> {
    let action = match body.action.as_deref() {
        None | Some("deny") => PolicyRuleAction::Deny,
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "unknown rule action {other:?}: the only supported action is \"deny\""
            )));
        }
    };
    if let Some(patterns) = &body.workflows {
        for pattern in patterns {
            if pattern.contains(['[', ']']) {
                return Err(ApiError::bad_request(format!(
                    "workflow pattern {pattern:?} uses character classes, which the \
                     matcher does not support (only `*`, `**`, `?`)"
                )));
            }
        }
    }
    match body.kind.as_str() {
        "event" => {
            let event = body
                .event
                .as_deref()
                .filter(|event| !event.trim().is_empty())
                .ok_or_else(|| ApiError::bad_request("event rules need a non-empty \"event\""))?;
            if body.actor.is_some() {
                return Err(ApiError::bad_request("event rules must not set \"actor\""));
            }
            Ok(NewRule::Event(EventRule {
                event: event.to_owned(),
                workflows: body.workflows.clone(),
                action,
            }))
        }
        "actor" => {
            let actor = body
                .actor
                .as_deref()
                .filter(|actor| !actor.trim().is_empty())
                .ok_or_else(|| ApiError::bad_request("actor rules need a non-empty \"actor\""))?;
            if body.event.is_some() {
                return Err(ApiError::bad_request("actor rules must not set \"event\""));
            }
            Ok(NewRule::Actor(ActorRule {
                actor: actor.to_owned(),
                workflows: body.workflows.clone(),
                action,
            }))
        }
        other => Err(ApiError::bad_request(format!(
            "unknown rule kind {other:?}: expected \"event\" or \"actor\""
        ))),
    }
}

/// Create a rule. The response is the created rule with its positional id.
pub async fn create_execution_protection_rule(
    State(shared): State<Arc<SharedState>>,
    Json(body): Json<RuleBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let new_rule = validate_rule_body(&body)?;
    mutate_policy(&shared, |policy| {
        let view = match new_rule {
            NewRule::Event(rule) => {
                let index = policy.event_rules.len();
                policy.event_rules.push(rule);
                serde_json::to_value(event_rule_view(index, &policy.event_rules[index]))
            }
            NewRule::Actor(rule) => {
                let index = policy.actor_rules.len();
                policy.actor_rules.push(rule);
                serde_json::to_value(actor_rule_view(index, &policy.actor_rules[index]))
            }
        }
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
        Ok(view)
    })
    .await
    .map(|Json(value)| (StatusCode::CREATED, Json(value)))
}

/// Read one rule by its positional id.
pub async fn get_execution_protection_rule(
    State(shared): State<Arc<SharedState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let policy = load_policy(&shared)?;
    let (kind, index) = parse_rule_id(&id)
        .ok_or_else(|| ApiError::not_found(format!("no execution protection rule {id:?}")))?;
    let view = match kind {
        "event" => policy
            .event_rules
            .get(index)
            .map(|rule| event_rule_view(index, rule)),
        "actor" => policy
            .actor_rules
            .get(index)
            .map(|rule| actor_rule_view(index, rule)),
        _ => None,
    }
    .ok_or_else(|| ApiError::not_found(format!("no execution protection rule {id:?}")))?;
    Ok(Json(serde_json::to_value(view).map_err(|error| {
        ApiError::internal(format!("{error:#}"))
    })?))
}

/// Replace one rule by its positional id. The body's `kind` must match the
/// id's kind — a rule does not change kind on update.
pub async fn update_execution_protection_rule(
    State(shared): State<Arc<SharedState>>,
    Path(id): Path<String>,
    Json(body): Json<RuleBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let (kind, index) = parse_rule_id(&id)
        .ok_or_else(|| ApiError::not_found(format!("no execution protection rule {id:?}")))?;
    let new_rule = validate_rule_body(&body)?;
    mutate_policy(&shared, |policy| {
        let list_len = match kind {
            "event" => policy.event_rules.len(),
            _ => policy.actor_rules.len(),
        };
        if index >= list_len {
            return Err(ApiError::not_found(format!(
                "no execution protection rule {id:?}"
            )));
        }
        let view = match (kind, new_rule) {
            ("event", NewRule::Event(rule)) => {
                policy.event_rules[index] = rule;
                serde_json::to_value(event_rule_view(index, &policy.event_rules[index]))
            }
            ("actor", NewRule::Actor(rule)) => {
                policy.actor_rules[index] = rule;
                serde_json::to_value(actor_rule_view(index, &policy.actor_rules[index]))
            }
            _ => {
                return Err(ApiError::bad_request(format!(
                    "rule {id:?} is a {kind} rule; the body describes a different kind"
                )));
            }
        }
        .map_err(|error| ApiError::internal(format!("{error:#}")))?;
        Ok(view)
    })
    .await
}

/// Delete one rule by its positional id. Deleting the default
/// `pull_request_target` rule writes an explicit empty `event_rules` list
/// back, so the default does not reappear on the next load.
pub async fn delete_execution_protection_rule(
    State(shared): State<Arc<SharedState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let (kind, index) = parse_rule_id(&id)
        .ok_or_else(|| ApiError::not_found(format!("no execution protection rule {id:?}")))?;
    mutate_policy(&shared, |policy| {
        let removed = match kind {
            "event" => {
                if index < policy.event_rules.len() {
                    policy.event_rules.remove(index);
                    true
                } else {
                    false
                }
            }
            _ => {
                if index < policy.actor_rules.len() {
                    policy.actor_rules.remove(index);
                    true
                } else {
                    false
                }
            }
        };
        if !removed {
            return Err(ApiError::not_found(format!(
                "no execution protection rule {id:?}"
            )));
        }
        Ok(serde_json::json!({ "ok": true }))
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event_body(event: &str) -> RuleBody {
        RuleBody {
            kind: "event".to_owned(),
            event: Some(event.to_owned()),
            actor: None,
            workflows: None,
            action: None,
        }
    }

    #[test]
    fn rule_ids_round_trip() {
        assert_eq!(rule_id("event", 0), "event-0");
        assert_eq!(rule_id("actor", 12), "actor-12");
        assert_eq!(parse_rule_id("event-0"), Some(("event", 0)));
        assert_eq!(parse_rule_id("actor-12"), Some(("actor", 12)));
        assert_eq!(parse_rule_id("bogus"), None);
        assert_eq!(parse_rule_id("event-x"), None);
        assert_eq!(parse_rule_id("workflow-1"), None);
        assert_eq!(parse_rule_id(""), None);
    }

    #[test]
    fn validate_accepts_event_and_actor_rules() {
        let body = event_body("push");
        let rule = validate_rule_body(&body).expect("event rule must validate");
        assert!(matches!(rule, NewRule::Event(_)));

        let body = RuleBody {
            kind: "actor".to_owned(),
            event: None,
            actor: Some("mallory".to_owned()),
            workflows: Some(vec!["deploy.yml".to_owned()]),
            action: Some("deny".to_owned()),
        };
        let rule = validate_rule_body(&body).expect("actor rule must validate");
        match rule {
            NewRule::Actor(rule) => {
                assert_eq!(rule.actor, "mallory");
                assert_eq!(rule.workflows, Some(vec!["deploy.yml".to_owned()]));
                assert_eq!(rule.action, PolicyRuleAction::Deny);
            }
            NewRule::Event(_) => panic!("expected an actor rule"),
        }
    }

    #[test]
    fn validate_rejects_unknown_kind() {
        let body = RuleBody {
            kind: "repository".to_owned(),
            ..event_body("push")
        };
        assert!(validate_rule_body(&body).is_err());
    }

    #[test]
    fn validate_rejects_missing_target() {
        assert!(validate_rule_body(&event_body("")).is_err());
        assert!(validate_rule_body(&event_body("   ")).is_err());
        let body = RuleBody {
            kind: "actor".to_owned(),
            event: None,
            actor: None,
            workflows: None,
            action: None,
        };
        assert!(validate_rule_body(&body).is_err());
    }

    #[test]
    fn validate_rejects_crossed_fields() {
        let mut body = event_body("push");
        body.actor = Some("mallory".to_owned());
        assert!(validate_rule_body(&body).is_err());

        let body = RuleBody {
            kind: "actor".to_owned(),
            event: Some("push".to_owned()),
            actor: Some("mallory".to_owned()),
            workflows: None,
            action: None,
        };
        assert!(validate_rule_body(&body).is_err());
    }

    #[test]
    fn validate_rejects_unknown_action() {
        let mut body = event_body("push");
        body.action = Some("allow".to_owned());
        assert!(validate_rule_body(&body).is_err());
    }

    #[test]
    fn validate_rejects_character_class_patterns() {
        let mut body = event_body("push");
        body.workflows = Some(vec!["deploy-[0-9].yml".to_owned()]);
        assert!(validate_rule_body(&body).is_err());

        let mut body = event_body("push");
        body.workflows = Some(vec!["release/*.yml".to_owned(), "**/test-?.yml".to_owned()]);
        assert!(validate_rule_body(&body).is_ok());
    }

    #[test]
    fn rule_views_carry_positional_ids() {
        let policy = ExecutionProtectionConfig {
            mode: ProtectionMode::Evaluate,
            event_rules: vec![EventRule {
                event: "pull_request_target".to_owned(),
                workflows: None,
                action: PolicyRuleAction::Deny,
            }],
            actor_rules: vec![ActorRule {
                actor: "mallory".to_owned(),
                workflows: Some(vec!["deploy.yml".to_owned()]),
                action: PolicyRuleAction::Deny,
            }],
        };
        let view = policy_view(&policy);
        assert_eq!(view["mode"], "evaluate");
        let rules = view["rules"].as_array().expect("rules array");
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0]["id"], "event-0");
        assert_eq!(rules[0]["kind"], "event");
        assert_eq!(rules[0]["event"], "pull_request_target");
        assert!(rules[0].get("actor").is_none());
        assert_eq!(rules[0]["action"], "deny");
        assert_eq!(rules[1]["id"], "actor-0");
        assert_eq!(rules[1]["kind"], "actor");
        assert_eq!(rules[1]["actor"], "mallory");
        assert_eq!(rules[1]["workflows"], serde_json::json!(["deploy.yml"]));
    }
}
