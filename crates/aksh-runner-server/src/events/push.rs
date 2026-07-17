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
/// Source: Runner.Server/Controllers/MessageController.cs:6258-6259,6283-6285.
const SKIP_CI_LABELS: &[&str] = &[
    "[skip ci]",
    "[ci skip]",
    "[no ci]",
    "[skip actions]",
    "[actions skip]",
    "***NO_CI***",
];

fn has_skip_label(message: &str) -> bool {
    SKIP_CI_LABELS.iter().any(|label| message.contains(label))
}

/// Check whether any commit in the push batch carries a skip-CI label.
///
/// GitHub skips the run when *any* commit in the push carries a skip label,
/// not just the head commit. Reference: `MessageController.cs:6258-6285`
/// (`Any(...)` across the commit batch).
fn has_skip_ci(payload: &Value) -> bool {
    // Check every commit in the batch.
    if let Some(commits) = payload.get("commits").and_then(|v| v.as_array()) {
        if !commits.is_empty() {
            return commits.iter().any(|commit| {
                commit
                    .get("message")
                    .and_then(|m| m.as_str())
                    .is_some_and(has_skip_label)
            });
        }
    }

    // Fall back to head_commit when commits[] is absent or empty
    // (lightweight webhooks may omit commits).
    payload
        .get("head_commit")
        .and_then(|hc| hc.get("message"))
        .and_then(|m| m.as_str())
        .is_some_and(has_skip_label)
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
        if payload
            .get("deleted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
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
    fn skip_ci_in_early_commit_skips() {
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
        assert!(
            events.is_empty(),
            "skip-ci in any commit must suppress the push"
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

    #[test]
    fn skip_ci_in_middle_commit_skips() {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": { "default_branch": "main" },
            "commits": [
                {"message": "first: clean"},
                {"message": "second: [ci skip] tweak"},
                {"message": "third: clean"}
            ]
        });
        let events = Adapter.project(&payload);
        assert!(
            events.is_empty(),
            "skip-ci in a middle commit must suppress the push"
        );
    }

    #[test]
    fn skip_ci_no_skip_in_multi_commit_push() {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": { "default_branch": "main" },
            "commits": [
                {"message": "first: clean"},
                {"message": "second: also clean"},
                {"message": "third: clean too"}
            ]
        });
        let events = Adapter.project(&payload);
        assert_eq!(events.len(), 1, "no skip label must start a run");
    }

    #[test]
    fn skip_ci_head_commit_fallback_when_commits_empty() {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": { "default_branch": "main" },
            "commits": [],
            "head_commit": {"message": "bump [skip actions]"}
        });
        let events = Adapter.project(&payload);
        assert!(
            events.is_empty(),
            "head_commit fallback must suppress when commits is empty"
        );
    }

    #[test]
    fn skip_ci_head_commit_fallback_when_commits_missing() {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": { "default_branch": "main" },
            "head_commit": {"message": "release [actions skip]"}
        });
        let events = Adapter.project(&payload);
        assert!(
            events.is_empty(),
            "head_commit fallback must suppress when commits field is absent"
        );
    }
}
