//! Workflow run event adapter.
//!
//! Reference: MessageController.cs:6287 (default-branch dispatch).
//! The downstream workflow definition, `GITHUB_REF`, and `GITHUB_SHA` use the
//! repository default branch. The upstream run remains available only in the
//! event payload.

use super::has_skip_label;
use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "workflow_run"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        // Honor [skip ci] on the triggering run's head commit, mirroring push.rs.
        // The commit message is in-payload (`workflow_run.head_commit.message`), so
        // this is a pure check with no GitHub API call — unlike pull requests, whose
        // payloads carry only `head.sha` and whose skip gate lives in the dispatcher.
        if payload
            .get("workflow_run")
            .and_then(|wr| wr.get("head_commit"))
            .and_then(|hc| hc.get("message"))
            .and_then(Value::as_str)
            .is_some_and(has_skip_label)
        {
            return vec![];
        }

        let workflow_run = payload.get("workflow_run");

        let default_branch = payload
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(|v| v.as_str())
            .unwrap_or("main");

        let activity_type = payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(str::to_owned);

        let upstream_workflow_names = workflow_run
            .and_then(|wr| wr.get("name"))
            .and_then(|v| v.as_str())
            .map(|name| vec![name.to_owned()])
            .unwrap_or_default();

        vec![EffectiveEvent {
            event: "workflow_run".to_owned(),
            git_ref: format!("refs/heads/{default_branch}"),
            sha: None,
            status_check_sha: None,
            activity_type,
            trust_tier: Some(TrustTier::Internal),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload_with_head_commit(message: Option<&str>) -> Value {
        let head_commit = match message {
            Some(message) => serde_json::json!({ "message": message }),
            None => Value::Null,
        };
        serde_json::json!({
            "action": "completed",
            "repository": { "default_branch": "main" },
            "workflow_run": { "name": "CI", "head_commit": head_commit }
        })
    }

    #[test]
    fn skips_when_head_commit_has_skip_ci() {
        let payload = payload_with_head_commit(Some("tweak docs [skip ci]"));
        assert!(Adapter.project(&payload).is_empty());
    }

    #[test]
    fn runs_when_head_commit_lacks_skip_label() {
        let payload = payload_with_head_commit(Some("real change"));
        assert_eq!(Adapter.project(&payload).len(), 1);
    }

    #[test]
    fn runs_when_head_commit_message_absent() {
        let payload = payload_with_head_commit(None);
        assert_eq!(Adapter.project(&payload).len(), 1);
    }
}
