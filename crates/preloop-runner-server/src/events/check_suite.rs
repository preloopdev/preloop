//! Check suite event adapter.
//!
//! `on: check_suite` — runs a workflow when check suite activity occurs.
//!
//! Per the workflow-trigger reference:
//! - `completed` is the *only* activity type that triggers a workflow.
//!   `requested` and `rerequested` exist as webhook activity types but are
//!   not workflow triggers ("Although only the `completed` activity type is
//!   supported, specifying the activity type will keep your workflow
//!   specific if more activity types are added in the future"). GitHub emits
//!   `check_suite.requested` on every push, so projecting it would start a
//!   spurious run per push for every `on: check_suite` workflow.
//! - `rerequested` still drives the suite-owner rerun flow, which the webhook
//!   processor handles ahead of trigger projection — see
//!   `github::rerun_for_rerequested`.
//! - GITHUB_REF / GITHUB_SHA = default branch and its last commit (the
//!   workflow file must live on the default branch for this event to fire).
//! - A malformed payload (missing `check_suite` / `head_sha`, unknown
//!   action) projects to no events instead of panicking.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Activity types that trigger an `on: check_suite` workflow. GitHub lists
/// exactly one.
const VALID_ACTIONS: &[&str] = &["completed"];

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "check_suite"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let Some(action) = payload.get("action").and_then(Value::as_str) else {
            return vec![];
        };
        if !VALID_ACTIONS.contains(&action) {
            return vec![];
        }
        // The suite identity is what makes the event actionable; without a
        // head SHA there is nothing exact to target.
        let Some(_head_sha) = payload
            .get("check_suite")
            .and_then(|suite| suite.get("head_sha"))
            .and_then(Value::as_str)
        else {
            return vec![];
        };

        let default_branch = payload
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(Value::as_str)
            .unwrap_or("main");

        vec![EffectiveEvent {
            event: "check_suite".to_owned(),
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
            "check_suite": {
                "id": 7,
                "head_branch": "changes",
                "head_sha": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                "status": "completed",
                "conclusion": "success",
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
            assert_eq!(events[0].event, "check_suite");
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
        let mut p = payload("completed");
        p["action"] = serde_json::json!("labeled");
        assert!(Adapter.project(&p).is_empty());
    }

    /// `requested` and `rerequested` are webhook activity types, not workflow
    /// triggers. GitHub sends `check_suite.requested` on every push; treating
    /// it as a trigger would start a spurious run per push.
    #[test]
    fn webhook_only_actions_do_not_trigger_workflows() {
        for action in ["requested", "rerequested"] {
            assert!(
                Adapter.project(&payload(action)).is_empty(),
                "{action} must not project a workflow trigger"
            );
        }
    }

    #[test]
    fn missing_action_projects_no_events() {
        let mut p = payload("completed");
        p.as_object_mut().unwrap().remove("action");
        assert!(Adapter.project(&p).is_empty());
    }

    #[test]
    fn non_string_action_projects_no_events() {
        let mut p = payload("completed");
        p["action"] = serde_json::json!(42);
        assert!(Adapter.project(&p).is_empty());
    }

    #[test]
    fn missing_check_suite_projects_no_events() {
        let mut p = payload("completed");
        p.as_object_mut().unwrap().remove("check_suite");
        assert!(Adapter.project(&p).is_empty());
    }

    #[test]
    fn missing_head_sha_projects_no_events() {
        let mut p = payload("completed");
        p["check_suite"].as_object_mut().unwrap().remove("head_sha");
        assert!(Adapter.project(&p).is_empty());
    }

    #[test]
    fn non_string_head_sha_projects_no_events() {
        let mut p = payload("completed");
        p["check_suite"]["head_sha"] = serde_json::json!(["not", "a", "sha"]);
        assert!(Adapter.project(&p).is_empty());
    }

    #[test]
    fn default_branch_comes_from_payload_repository() {
        let mut p = payload("completed");
        p["repository"]["default_branch"] = serde_json::json!("trunk");
        let events = Adapter.project(&p);
        assert_eq!(events[0].git_ref, "refs/heads/trunk");
    }

    #[test]
    fn missing_repository_defaults_to_main() {
        let mut p = payload("completed");
        p.as_object_mut().unwrap().remove("repository");
        let events = Adapter.project(&p);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].git_ref, "refs/heads/main");
    }
}
