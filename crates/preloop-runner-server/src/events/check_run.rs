//! Check run event adapter.
//!
//! `on: check_run` — runs a workflow when activity related to a check run
//! occurs. Valid activity types: `created`, `rerequested`, `completed`,
//! `requested_action`. The `rerequested` type additionally drives the
//! suite-owner rerun flow in the webhook processor (see
//! `github::rerun_for_rerequested`).
//!
//! Per the workflow-trigger reference:
//! - GITHUB_REF / GITHUB_SHA = default branch and its last commit (the
//!   workflow file must live on the default branch for this event to fire).
//! - A malformed payload (missing `check_run` / `head_sha`, unknown action)
//!   projects to no events instead of panicking.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Activity types GitHub accepts for `check_run`.
const VALID_ACTIONS: &[&str] = &["created", "rerequested", "completed", "requested_action"];

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "check_run"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let Some(action) = payload.get("action").and_then(Value::as_str) else {
            return vec![];
        };
        if !VALID_ACTIONS.contains(&action) {
            return vec![];
        }
        let Some(check_run) = payload.get("check_run") else {
            return vec![];
        };
        // The check run identity is what makes the event actionable; without
        // a head SHA there is nothing exact to target.
        let Some(_head_sha) = check_run.get("head_sha").and_then(Value::as_str) else {
            return vec![];
        };

        let default_branch = payload
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(Value::as_str)
            .unwrap_or("main");

        vec![EffectiveEvent {
            event: "check_run".to_owned(),
            git_ref: format!("refs/heads/{default_branch}"),
            sha: None,
            status_check_sha: None,
            activity_type: Some(action.to_owned()),
            trust_tier: Some(TrustTier::Internal),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(action: &str) -> Value {
        serde_json::json!({
            "action": action,
            "check_run": {
                "id": 4,
                "name": "build",
                "head_sha": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                "status": "completed",
                "conclusion": "success",
                "check_suite": {"id": 7, "head_branch": "changes"},
            },
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "sender": {"login": "octocat"},
        })
    }

    #[test]
    fn valid_actions_project_to_default_branch_events() {
        for action in VALID_ACTIONS {
            let events = Adapter.project(&payload(action));
            assert_eq!(events.len(), 1, "{action} must project one event");
            assert_eq!(events[0].event, "check_run");
            assert_eq!(events[0].git_ref, "refs/heads/main");
            assert_eq!(
                events[0].sha, None,
                "the run sha is the default-branch head, resolved by the webhook path"
            );
            assert_eq!(events[0].activity_type.as_deref(), Some(*action));
            assert_eq!(events[0].trust_tier, Some(TrustTier::Internal));
        }
    }

    #[test]
    fn unknown_action_projects_no_events() {
        let mut p = payload("created");
        p["action"] = serde_json::json!("labeled");
        assert!(Adapter.project(&p).is_empty());
    }

    #[test]
    fn missing_action_projects_no_events() {
        let mut p = payload("created");
        p.as_object_mut().unwrap().remove("action");
        assert!(Adapter.project(&p).is_empty());
    }

    #[test]
    fn missing_check_run_projects_no_events() {
        let mut p = payload("created");
        p.as_object_mut().unwrap().remove("check_run");
        assert!(Adapter.project(&p).is_empty());
    }

    #[test]
    fn missing_head_sha_projects_no_events() {
        let mut p = payload("created");
        p["check_run"].as_object_mut().unwrap().remove("head_sha");
        assert!(Adapter.project(&p).is_empty());
    }

    #[test]
    fn non_string_head_sha_projects_no_events() {
        let mut p = payload("created");
        p["check_run"]["head_sha"] = serde_json::json!(1234);
        assert!(Adapter.project(&p).is_empty());
    }

    #[test]
    fn default_branch_comes_from_payload_repository() {
        let mut p = payload("created");
        p["repository"]["default_branch"] = serde_json::json!("trunk");
        let events = Adapter.project(&p);
        assert_eq!(events[0].git_ref, "refs/heads/trunk");
    }

    #[test]
    fn missing_repository_defaults_to_main() {
        let mut p = payload("created");
        p.as_object_mut().unwrap().remove("repository");
        let events = Adapter.project(&p);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].git_ref, "refs/heads/main");
    }
}
