//! Pull request event adapter.
//!
//! Reference: MessageController.cs:6260-6270
//!
//! Pull request webhook always emits `pull_request_target` AND conditionally
//! emits `pull_request` (gated on AllowPullRequests / non-fork).
//!
//! - pull_request_target: ref = refs/heads/{base.ref}, sha = head.sha
//! - pull_request: ref = refs/pull/{n}/merge, sha = merge_commit_sha
//!   (or refs/pull/{n}/head if no merge pseudo-branch)

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

/// Extract the pull request number from the payload.
fn pr_number(payload: &Value) -> Option<u64> {
    payload.get("number").and_then(|v| v.as_u64()).or_else(|| {
        payload
            .get("pull_request")
            .and_then(|pr| pr.get("number"))
            .and_then(|v| v.as_u64())
    })
}

/// Extract base ref from the PR payload.
fn base_ref(payload: &Value) -> Option<&str> {
    payload
        .get("pull_request")
        .and_then(|pr| pr.get("base"))
        .and_then(|base| base.get("ref"))
        .and_then(|v| v.as_str())
}

/// Extract head SHA from the PR payload.
fn head_sha(payload: &Value) -> Option<&str> {
    payload
        .get("pull_request")
        .and_then(|pr| pr.get("head"))
        .and_then(|head| head.get("sha"))
        .and_then(|v| v.as_str())
}

/// Extract merge commit SHA from the PR payload.
fn merge_commit_sha(payload: &Value) -> Option<&str> {
    payload
        .get("pull_request")
        .and_then(|pr| pr.get("merge_commit_sha"))
        .and_then(|v| v.as_str())
}

/// Extract base SHA from the PR payload.
/// Used as the checkout SHA for pull_request_target (MC.cs:6261 — Base.Sha).
fn base_sha(payload: &Value) -> Option<&str> {
    payload
        .get("pull_request")
        .and_then(|pr| pr.get("base"))
        .and_then(|base| base.get("sha"))
        .and_then(|v| v.as_str())
}

/// Is this PR from a fork?
fn is_fork(payload: &Value) -> bool {
    payload
        .get("pull_request")
        .and_then(|pr| pr.get("head"))
        .and_then(|head| head.get("repo"))
        .and_then(|repo| repo.get("fork"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// Extract the action from the payload.
fn action(payload: &Value) -> Option<String> {
    payload
        .get("action")
        .and_then(|v| v.as_str())
        .map(|s| s.to_owned())
}

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "pull_request"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let number = match pr_number(payload) {
            Some(n) => n,
            None => return vec![],
        };
        let base = base_ref(payload).unwrap_or("main");
        // MC.cs:6261: sha = Base.Sha (checkout target), status_check_sha = head.sha (PR context)
        let base_checkout = base_sha(payload).map(str::to_owned);
        let head = head_sha(payload).unwrap_or("0000000000000000000000000000000000000000");
        let merge = merge_commit_sha(payload);
        let act = action(payload);

        let mut events = Vec::new();

        // Always emit pull_request_target — uses base branch for checkout.
        events.push(EffectiveEvent {
            event: "pull_request_target".to_owned(),
            git_ref: format!("refs/heads/{base}"),
            sha: base_checkout.or_else(|| Some(head.to_owned())),
            status_check_sha: Some(head.to_owned()),
            activity_type: act.clone(),
            trust_tier: Some(TrustTier::PullRequestTarget),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        });

        // Fork pull requests still emit `pull_request`; the trust tier gates
        // secrets and write access rather than deleting the event.
        let (pr_ref, pr_sha) = if let Some(merge_sha) = merge {
            (format!("refs/pull/{number}/merge"), merge_sha.to_owned())
        } else {
            (format!("refs/pull/{number}/head"), head.to_owned())
        };
        let trust_tier = if is_fork(payload) {
            TrustTier::UntrustedForkPullRequest
        } else {
            TrustTier::InternalPullRequest
        };
        events.push(EffectiveEvent {
            event: "pull_request".to_owned(),
            git_ref: pr_ref,
            sha: Some(pr_sha),
            status_check_sha: Some(head.to_owned()),
            activity_type: act,
            trust_tier: Some(trust_tier),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        });

        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pr_emits_both_target_and_pr() {
        let payload = serde_json::json!({
            "action": "opened",
            "number": 42,
            "pull_request": {
                "number": 42,
                "base": { "ref": "main", "sha": "base-sha" },
                "head": { "ref": "feature/x", "sha": "head-sha", "repo": { "fork": false } },
                "merge_commit_sha": "merge-sha"
            }
        });
        let events = Adapter.project(&payload);
        assert_eq!(events.len(), 2);

        // pull_request_target always first
        let target = &events[0];
        assert_eq!(target.event, "pull_request_target");
        assert_eq!(target.git_ref, "refs/heads/main");
        assert_eq!(target.trust_tier, Some(TrustTier::PullRequestTarget));

        // pull_request second
        let pr = &events[1];
        assert_eq!(pr.event, "pull_request");
        assert_eq!(pr.git_ref, "refs/pull/42/merge");
        assert_eq!(pr.trust_tier, Some(TrustTier::InternalPullRequest));
    }

    #[test]
    fn fork_pr_emits_untrusted_pull_request() {
        let payload = serde_json::json!({
            "action": "opened",
            "number": 99,
            "pull_request": {
                "number": 99,
                "base": { "ref": "main" },
                "head": { "ref": "feature/y", "sha": "fork-head-sha", "repo": { "fork": true } },
                "merge_commit_sha": "merge-sha"
            }
        });
        let events = Adapter.project(&payload);
        assert_eq!(events.len(), 2);
        let pr = events
            .iter()
            .find(|event| event.event == "pull_request")
            .unwrap();
        assert_eq!(pr.trust_tier, Some(TrustTier::UntrustedForkPullRequest));
    }

    #[test]
    fn pr_without_merge_sha_uses_head() {
        let payload = serde_json::json!({
            "action": "synchronize",
            "number": 7,
            "pull_request": {
                "number": 7,
                "base": { "ref": "develop" },
                "head": { "ref": "fix/bug", "sha": "bugfix-sha", "repo": { "fork": false } }
            }
        });
        let events = Adapter.project(&payload);
        let pr = events.iter().find(|e| e.event == "pull_request").unwrap();
        assert_eq!(pr.git_ref, "refs/pull/7/head");
        assert_eq!(pr.sha, Some("bugfix-sha".to_owned()));
    }

    #[test]
    fn pr_without_number_returns_empty() {
        let payload = serde_json::json!({
            "action": "opened",
            "pull_request": {
                "base": { "ref": "main" },
                "head": { "ref": "x", "sha": "sha", "repo": { "fork": false } }
            }
        });
        assert!(Adapter.project(&payload).is_empty());
    }
}
