//! Workflow execution protections: admin-level deny policy on which events
//! and which actors may trigger workflows.
//!
//! Mirrors GitHub's execution protections (event rules, actor rules,
//! per-file targeting, evaluate/enforce modes). Rules live in the operator's
//! server config ([`crate::config::ExecutionProtectionConfig`]) — never in
//! workflow repos — so workflow authors cannot weaken the policy that
//! constrains them.
//!
//! Evaluation is split in two, matching the webhook loop in `github.rs`:
//! - [`denies_event`] considers only *unscoped* rules (no `workflows` list).
//!   A hit denies the whole event before any workflow is matched.
//! - [`denies_workflow`] considers only *scoped* rules, evaluated per
//!   candidate workflow file where the loop matches workflows to the event.
//!
//! The [`ProtectionMode`](crate::config::ProtectionMode) is applied by the
//! caller: `enforce` skips the event/workflow, `evaluate` logs and continues.

use crate::config::ExecutionProtectionConfig;

/// A rule that matched a trigger, for logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleHit {
    /// "event" or "actor".
    pub kind: &'static str,
    /// What matched, e.g. `event="pull_request_target"` or `actor="mallory"`.
    pub detail: String,
    /// Rule scope: "all workflows" or the workflow glob list.
    pub scope: String,
}

impl RuleHit {
    /// Single-line description for log lines, e.g.
    /// `event-rule event="pull_request_target" (all workflows)`.
    pub fn describe(&self) -> String {
        format!("{}-rule {} ({})", self.kind, self.detail, self.scope)
    }
}

/// Whether a `workflows` list is unscoped (omitted or empty = every workflow).
fn is_unscoped(workflows: &Option<Vec<String>>) -> bool {
    workflows
        .as_ref()
        .is_none_or(|patterns| patterns.is_empty())
}

/// GitHub-style glob match of a rule's `workflows` patterns against a
/// candidate workflow file. Patterns match the bare filename (`deploy.yml`)
/// and the repo-relative path (`.github/workflows/deploy.yml`).
fn workflow_matches(patterns: &[String], filename: &str) -> bool {
    let path = format!(".github/workflows/{filename}");
    patterns.iter().any(|pattern| {
        preloop_gha_parser::glob_match(pattern, filename)
            || preloop_gha_parser::glob_match(pattern, &path)
    })
}

fn scope_of(workflows: &Option<Vec<String>>) -> String {
    match workflows {
        Some(patterns) if !patterns.is_empty() => {
            format!("workflows=[{}]", patterns.join(", "))
        }
        _ => "all workflows".to_owned(),
    }
}

/// Unscoped deny rules for this event/actor. A `Some` hit means the whole
/// event is denied before workflow matching.
pub fn denies_event(
    policy: &ExecutionProtectionConfig,
    event: &str,
    actor: Option<&str>,
) -> Option<RuleHit> {
    for rule in &policy.event_rules {
        if !is_unscoped(&rule.workflows) {
            continue;
        }
        if rule.event == event {
            return Some(RuleHit {
                kind: "event",
                detail: format!("event={event:?}"),
                scope: scope_of(&rule.workflows),
            });
        }
    }
    for rule in &policy.actor_rules {
        if !is_unscoped(&rule.workflows) {
            continue;
        }
        if let Some(actor) = actor {
            if rule.actor.eq_ignore_ascii_case(actor) {
                return Some(RuleHit {
                    kind: "actor",
                    detail: format!("actor={actor:?}"),
                    scope: scope_of(&rule.workflows),
                });
            }
        }
    }
    None
}

/// Scoped deny rules for this event/actor/workflow file. Evaluated where the
/// webhook loop matches candidate workflows to the event.
pub fn denies_workflow(
    policy: &ExecutionProtectionConfig,
    event: &str,
    actor: Option<&str>,
    workflow_file: &str,
) -> Option<RuleHit> {
    for rule in &policy.event_rules {
        if is_unscoped(&rule.workflows) {
            continue;
        }
        if rule.event == event
            && workflow_matches(rule.workflows.as_deref().unwrap_or(&[]), workflow_file)
        {
            return Some(RuleHit {
                kind: "event",
                detail: format!("event={event:?}"),
                scope: scope_of(&rule.workflows),
            });
        }
    }
    for rule in &policy.actor_rules {
        if is_unscoped(&rule.workflows) {
            continue;
        }
        if let Some(actor) = actor {
            if rule.actor.eq_ignore_ascii_case(actor)
                && workflow_matches(rule.workflows.as_deref().unwrap_or(&[]), workflow_file)
            {
                return Some(RuleHit {
                    kind: "actor",
                    detail: format!("actor={actor:?}"),
                    scope: scope_of(&rule.workflows),
                });
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ActorRule, EventRule};

    fn policy_with(
        event_rules: Vec<EventRule>,
        actor_rules: Vec<ActorRule>,
    ) -> ExecutionProtectionConfig {
        ExecutionProtectionConfig {
            mode: crate::config::ProtectionMode::Enforce,
            event_rules,
            actor_rules,
        }
    }

    fn event_rule(event: &str, workflows: Option<Vec<&str>>) -> EventRule {
        EventRule {
            event: event.to_owned(),
            workflows: workflows.map(|patterns| patterns.into_iter().map(str::to_owned).collect()),
            action: crate::config::PolicyRuleAction::Deny,
        }
    }

    fn actor_rule(actor: &str, workflows: Option<Vec<&str>>) -> ActorRule {
        ActorRule {
            actor: actor.to_owned(),
            workflows: workflows.map(|patterns| patterns.into_iter().map(str::to_owned).collect()),
            action: crate::config::PolicyRuleAction::Deny,
        }
    }

    #[test]
    fn unscoped_event_rule_denies_whole_event() {
        let policy = policy_with(vec![event_rule("pull_request_target", None)], vec![]);
        let hit = denies_event(&policy, "pull_request_target", Some("alice"))
            .expect("unscoped pull_request_target rule must deny the event");
        assert_eq!(hit.kind, "event");
        assert!(denies_event(&policy, "push", Some("alice")).is_none());
    }

    #[test]
    fn empty_workflows_list_counts_as_unscoped() {
        let policy = policy_with(vec![event_rule("push", Some(vec![]))], vec![]);
        assert!(denies_event(&policy, "push", None).is_some());
    }

    #[test]
    fn scoped_event_rule_does_not_deny_event() {
        let policy = policy_with(
            vec![event_rule("pull_request_target", Some(vec!["deploy.yml"]))],
            vec![],
        );
        // Scoped rules are per-file: the event itself still flows so other
        // workflows can run.
        assert!(denies_event(&policy, "pull_request_target", Some("alice")).is_none());
        assert!(
            denies_workflow(&policy, "pull_request_target", Some("alice"), "deploy.yml").is_some()
        );
        assert!(denies_workflow(&policy, "pull_request_target", Some("alice"), "ci.yml").is_none());
        // Other events are unaffected even for the scoped file.
        assert!(denies_workflow(&policy, "push", Some("alice"), "deploy.yml").is_none());
    }

    #[test]
    fn unscoped_rule_is_not_evaluated_per_workflow() {
        let policy = policy_with(vec![event_rule("push", None)], vec![]);
        // Handled at the event level; per-file evaluation must not double-hit.
        assert!(denies_workflow(&policy, "push", Some("alice"), "deploy.yml").is_none());
    }

    #[test]
    fn actor_rule_matches_sender_login_case_insensitively() {
        let policy = policy_with(vec![], vec![actor_rule("Mallory", None)]);
        assert!(denies_event(&policy, "workflow_dispatch", Some("mallory")).is_some());
        assert!(denies_event(&policy, "workflow_dispatch", Some("MALLORY")).is_some());
        assert!(denies_event(&policy, "workflow_dispatch", Some("alice")).is_none());
    }

    #[test]
    fn actor_rule_without_sender_never_matches() {
        let policy = policy_with(vec![], vec![actor_rule("mallory", None)]);
        assert!(denies_event(&policy, "workflow_dispatch", None).is_none());
    }

    #[test]
    fn scoped_actor_rule_applies_per_file() {
        let policy = policy_with(
            vec![],
            vec![actor_rule("mallory", Some(vec!["deploy.yml"]))],
        );
        assert!(denies_event(&policy, "workflow_dispatch", Some("mallory")).is_none());
        assert!(
            denies_workflow(&policy, "workflow_dispatch", Some("mallory"), "deploy.yml").is_some()
        );
        assert!(denies_workflow(&policy, "workflow_dispatch", Some("mallory"), "ci.yml").is_none());
        assert!(
            denies_workflow(&policy, "workflow_dispatch", Some("alice"), "deploy.yml").is_none()
        );
    }

    #[test]
    fn workflow_globs_match_filename_and_path() {
        let policy = policy_with(
            vec![event_rule("push", Some(vec!["release/*.yml"]))],
            vec![],
        );
        assert!(denies_workflow(&policy, "push", None, "release/foo.yml").is_some());
        assert!(denies_workflow(&policy, "push", None, "foo.yml").is_none());
        // Full repo-relative path patterns also work.
        let policy = policy_with(
            vec![event_rule(
                "push",
                Some(vec![".github/workflows/deploy.yml"]),
            )],
            vec![],
        );
        assert!(denies_workflow(&policy, "push", None, "deploy.yml").is_some());
    }

    #[test]
    fn empty_policy_denies_nothing() {
        let policy = ExecutionProtectionConfig::default();
        assert!(denies_event(&policy, "pull_request_target", Some("mallory")).is_none());
        assert!(denies_workflow(
            &policy,
            "pull_request_target",
            Some("mallory"),
            "deploy.yml"
        )
        .is_none());
    }

    #[test]
    fn rule_hit_describes_itself() {
        let hit = RuleHit {
            kind: "event",
            detail: "event=\"pull_request_target\"".to_owned(),
            scope: "all workflows".to_owned(),
        };
        assert_eq!(
            hit.describe(),
            "event-rule event=\"pull_request_target\" (all workflows)"
        );
    }

    #[test]
    fn config_parses_from_toml() {
        let config: crate::config::ConfigFile = toml::from_str(
            r#"
[execution_protection]
mode = "enforce"

[[execution_protection.event_rules]]
event = "pull_request_target"

[[execution_protection.event_rules]]
event = "workflow_dispatch"
workflows = ["deploy.yml", "release/*.yml"]

[[execution_protection.actor_rules]]
actor = "mallory"
workflows = ["deploy.yml"]
"#,
        )
        .expect("execution protection config must parse");
        let policy = &config.execution_protection;
        assert_eq!(policy.mode, crate::config::ProtectionMode::Enforce);
        assert_eq!(policy.event_rules.len(), 2);
        assert!(policy.event_rules[0].workflows.is_none());
        assert_eq!(
            policy.event_rules[1].workflows.as_ref().unwrap(),
            &vec!["deploy.yml".to_owned(), "release/*.yml".to_owned()]
        );
        assert_eq!(policy.actor_rules.len(), 1);
        // Omitted action defaults to deny.
        assert_eq!(
            policy.event_rules[0].action,
            crate::config::PolicyRuleAction::Deny
        );
    }

    #[test]
    fn config_defaults_to_evaluate_with_no_rules() {
        let config: crate::config::ConfigFile =
            toml::from_str("").expect("empty config must parse");
        assert_eq!(
            config.execution_protection.mode,
            crate::config::ProtectionMode::Evaluate
        );
        assert!(config.execution_protection.event_rules.is_empty());
        assert!(config.execution_protection.actor_rules.is_empty());
    }

    #[test]
    fn unknown_mode_is_rejected() {
        let result = toml::from_str::<crate::config::ConfigFile>(
            "[execution_protection]\nmode = \"audit\"\n",
        );
        assert!(result.is_err(), "unknown mode must fail closed");
    }

    #[test]
    fn unknown_action_is_rejected() {
        let result = toml::from_str::<crate::config::ConfigFile>(
            "[[execution_protection.event_rules]]\nevent = \"push\"\naction = \"allow\"\n",
        );
        assert!(result.is_err(), "unknown action must fail closed");
    }
}
