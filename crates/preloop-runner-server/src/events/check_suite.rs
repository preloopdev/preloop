//! Check-suite webhook adapter.
//!
//! Check-suite workflow runs execute from the repository default branch. The
//! suite's head SHA remains in the payload for expressions, but is not the
//! checkout revision for the workflow triggered by the event.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

const ACTIONS: [&str; 3] = ["completed", "requested", "rerequested"];

pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "check_suite"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let Some(action) = payload.get("action").and_then(Value::as_str) else {
            return vec![];
        };
        if !ACTIONS.contains(&action) {
            return vec![];
        }
        let Some(suite) = payload.get("check_suite").and_then(Value::as_object) else {
            return vec![];
        };
        if suite.get("head_sha").and_then(Value::as_str).is_none()
            || payload
                .get("repository")
                .and_then(|repo| repo.get("full_name"))
                .and_then(Value::as_str)
                .is_none()
        {
            return vec![];
        }
        let default_branch = payload
            .get("repository")
            .and_then(|repo| repo.get("default_branch"))
            .and_then(Value::as_str)
            .unwrap_or("main");
        vec![EffectiveEvent {
            event: "check_suite".to_owned(),
            git_ref: format!("refs/heads/{default_branch}"),
            sha: None,
            status_check_sha: None,
            activity_type: Some(action.to_owned()),
            trust_tier: Some(TrustTier::Untrusted),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn payload(action: &str) -> Value {
        json!({
            "action": action,
            "repository": {"full_name": "o/r", "default_branch": "trunk"},
            "check_suite": {"head_sha": "a".repeat(40)}
        })
    }

    #[test]
    fn projects_supported_actions_on_default_branch() {
        for action in ACTIONS {
            let events = Adapter.project(&payload(action));
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].activity_type.as_deref(), Some(action));
            assert_eq!(events[0].git_ref, "refs/heads/trunk");
            assert!(events[0].sha.is_none());
        }
    }

    #[test]
    fn rejects_unknown_and_malformed_actions() {
        assert!(Adapter.project(&payload("foobar")).is_empty());
        assert!(Adapter.project(&json!({"action": "requested"})).is_empty());
    }
}
