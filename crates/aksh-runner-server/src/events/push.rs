//! Push event adapter.
//!
//! Reference: MessageController.cs:6276-6278
//! - ref = hook.Ref
//! - sha = hook.After
//! - status_check_sha = hook.After
//! - skip if any commit message matches [skip ci] / [ci skip] / etc.
//! - trust_tier = Trusted for default branch, Internal otherwise

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

/// [skip ci] labels that suppress CI runs.
const SKIP_CI_LABELS: &[&str] = &[
    "[skip ci]",
    "[ci skip]",
    "[no ci]",
    "[skip actions]",
    "[actions skip]",
];

/// Check whether the head commit message contains a skip-CI label.
///
/// GitHub skips CI when the **head commit** of a push contains a skip label,
/// not when any commit in the batch does.  Reference:
/// <https://docs.github.com/en/actions/managing-workflow-runs/skipping-workflow-runs>
fn has_skip_ci(payload: &Value) -> bool {
    // Prefer the explicit head_commit field (always the tip of the push).
    let head_message = payload
        .get("head_commit")
        .and_then(|hc| hc.get("message"))
        .and_then(|m| m.as_str());

    // Fall back to the last element of commits[] (same commit, different
    // representation — lightweight webhooks may omit head_commit).
    let message = head_message.or_else(|| {
        payload
            .get("commits")
            .and_then(|v| v.as_array())
            .and_then(|commits| commits.last())
            .and_then(|commit| commit.get("message"))
            .and_then(|m| m.as_str())
    });

    message.is_some_and(|msg| SKIP_CI_LABELS.iter().any(|label| msg.contains(label)))
}

/// Determine whether this is a push to the default branch.
fn is_default_branch(payload: &Value) -> bool {
    let default_branch = payload
        .get("repository")
        .and_then(|r| r.get("default_branch"))
        .and_then(|v| v.as_str())
        .unwrap_or("main");

    let ref_str = payload.get("ref").and_then(|v| v.as_str()).unwrap_or("");

    ref_str == format!("refs/heads/{default_branch}")
}

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "push"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        // Skip branch-deletion pushes (after = all-zero SHA, no valid commit)
        if payload.get("deleted").and_then(|v| v.as_bool()).unwrap_or(false) {
            return vec![];
        }
        if has_skip_ci(payload) {
            return vec![];
        }

        let git_ref = payload
            .get("ref")
            .and_then(|v| v.as_str())
            .unwrap_or("refs/heads/main")
            .to_owned();

        let sha = payload
            .get("after")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());

        let trust_tier = if is_default_branch(payload) {
            TrustTier::Trusted
        } else {
            TrustTier::Internal
        };

        vec![EffectiveEvent {
            event: "push".to_owned(),
            git_ref,
            sha: sha.clone(),
            status_check_sha: sha,
            activity_type: None,
            trust_tier: Some(trust_tier),
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
    fn push_default_branch_is_trusted() {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": { "default_branch": "main" },
            "commits": []
        });
        let events = Adapter.project(&payload);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].trust_tier, Some(TrustTier::Trusted));
        assert_eq!(events[0].git_ref, "refs/heads/main");
        assert_eq!(events[0].sha, Some("abc123".to_owned()));
    }

    #[test]
    fn push_non_default_branch_is_internal() {
        let payload = serde_json::json!({
            "ref": "refs/heads/feature/x",
            "after": "def456",
            "repository": { "default_branch": "main" },
            "commits": []
        });
        let events = Adapter.project(&payload);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].trust_tier, Some(TrustTier::Internal));
    }

    #[test]
    fn skip_ci_in_commit_message() {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": { "default_branch": "main" },
            "commits": [
                {"message": "fix: update deps [skip ci]"}
            ]
        });
        let events = Adapter.project(&payload);
        assert!(events.is_empty());
    }

    #[test]
    fn skip_ci_in_head_commit() {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": { "default_branch": "main" },
            "head_commit": {"message": "[ci skip] bump version"}
        });
        let events = Adapter.project(&payload);
        assert!(events.is_empty());
    }

    #[test]
    fn no_skip_ci_normal_commit() {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": { "default_branch": "main" },
            "commits": [
                {"message": "fix: update deps"}
            ]
        });
        let events = Adapter.project(&payload);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn skip_ci_only_in_early_commit_does_not_skip() {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": { "default_branch": "main" },
            "commits": [
                {"message": "early commit [skip ci]"},
                {"message": "head commit: real work"}
            ]
        });
        let events = Adapter.project(&payload);
        assert_eq!(
            events.len(),
            1,
            "skip-ci in a non-head commit must not suppress the push"
        );
    }

    #[test]
    fn skip_ci_in_last_commit_skips() {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": { "default_branch": "main" },
            "commits": [
                {"message": "first commit: clean"},
                {"message": "second commit [no ci]"}
            ]
        });
        let events = Adapter.project(&payload);
        assert!(
            events.is_empty(),
            "skip-ci in the head (last) commit must suppress the push"
        );
    }
}
