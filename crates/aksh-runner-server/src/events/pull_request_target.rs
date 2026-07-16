//! Pull request target standalone adapter.
//!
//! This adapter handles manual `pull_request_target` submissions (not the
//! fan-out from `pull_request` webhooks — that's in `pull_request.rs`).
//!
//! Reference: MessageController.cs:6260-6263

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "pull_request_target"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let default_branch = payload
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(|v| v.as_str())
            .unwrap_or("main");

        let head_sha = payload
            .get("pull_request")
            .and_then(|pr| pr.get("head"))
            .and_then(|h| h.get("sha"))
            .and_then(|v| v.as_str());

        // MC.cs:6261: sha = Base.Sha (checkout target), status_check_sha = head.sha
        let base_sha = payload
            .get("pull_request")
            .and_then(|pr| pr.get("base"))
            .and_then(|b| b.get("sha"))
            .and_then(|v| v.as_str());

        let base_ref = payload
            .get("pull_request")
            .and_then(|pr| pr.get("base"))
            .and_then(|b| b.get("ref"))
            .and_then(|v| v.as_str())
            .unwrap_or(default_branch);

        let activity_type = payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());

        vec![EffectiveEvent {
            event: "pull_request_target".to_owned(),
            git_ref: format!("refs/heads/{base_ref}"),
            sha: base_sha.or(head_sha).map(|s| s.to_owned()),
            status_check_sha: head_sha.map(|s| s.to_owned()),
            activity_type,
            trust_tier: Some(TrustTier::PullRequestTarget),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_base_sha_for_checkout() {
        let payload = serde_json::json!({
            "action": "opened",
            "pull_request": {
                "base": { "ref": "main", "sha": "base-sha-123" },
                "head": { "ref": "feature/x", "sha": "head-sha-456" }
            },
            "repository": { "default_branch": "main" }
        });
        let events = Adapter.project(&payload);
        assert_eq!(events.len(), 1);
        // sha = base (checkout target), status_check_sha = head (PR context)
        assert_eq!(events[0].sha, Some("base-sha-123".to_owned()));
        assert_eq!(events[0].status_check_sha, Some("head-sha-456".to_owned()));
    }

    #[test]
    fn falls_back_to_head_when_base_sha_missing() {
        let payload = serde_json::json!({
            "action": "opened",
            "pull_request": {
                "base": { "ref": "main" },
                "head": { "ref": "feature/x", "sha": "head-sha-789" }
            },
            "repository": { "default_branch": "main" }
        });
        let events = Adapter.project(&payload);
        assert_eq!(events[0].sha, Some("head-sha-789".to_owned()));
    }
}
