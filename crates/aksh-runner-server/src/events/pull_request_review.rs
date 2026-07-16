//! Pull request review event adapter.
//!
//! Reference: MessageController.cs:6271-6275.
//! `types:` matches the webhook action (`submitted`, `edited`, `dismissed`),
//! while approval remains available as `github.event.review.state`.
use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "pull_request_review"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let number = payload
            .get("pull_request")
            .and_then(|pr| pr.get("number"))
            .and_then(|v| v.as_u64());

        let number = match number {
            Some(n) => n,
            None => return vec![],
        };

        let head_sha = payload
            .get("pull_request")
            .and_then(|pr| pr.get("head"))
            .and_then(|h| h.get("sha"))
            .and_then(|v| v.as_str());

        let merge_sha = payload
            .get("pull_request")
            .and_then(|pr| pr.get("merge_commit_sha"))
            .and_then(|v| v.as_str());

        let (git_ref, sha) = if let Some(merge) = merge_sha {
            (format!("refs/pull/{number}/merge"), merge.to_owned())
        } else {
            (
                format!("refs/pull/{number}/head"),
                head_sha
                    .unwrap_or("0000000000000000000000000000000000000000")
                    .to_owned(),
            )
        };

        let activity_type = payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(str::to_owned);

        let is_fork = payload
            .get("pull_request")
            .and_then(|pr| pr.get("head"))
            .and_then(|head| head.get("repo"))
            .and_then(|repo| repo.get("fork"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true); // fail-closed: missing repo = untrusted

        let trust_tier = if is_fork {
            TrustTier::UntrustedForkPullRequest
        } else {
            TrustTier::InternalPullRequest
        };

        vec![EffectiveEvent {
            event: "pull_request_review".to_owned(),
            git_ref,
            sha: Some(sha.clone()),
            status_check_sha: head_sha.map(|s| s.to_owned()),
            activity_type,
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
    fn review_on_internal_pr_is_trusted() {
        let payload = serde_json::json!({
            "action": "submitted",
            "pull_request": {
                "number": 10,
                "head": { "sha": "abc", "repo": { "fork": false } },
                "merge_commit_sha": "merge-sha"
            },
            "review": { "state": "approved" }
        });
        let events = Adapter.project(&payload);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].trust_tier, Some(TrustTier::InternalPullRequest));
    }

    #[test]
    fn review_on_fork_pr_is_untrusted() {
        let payload = serde_json::json!({
            "action": "submitted",
            "pull_request": {
                "number": 11,
                "head": { "sha": "fork-sha", "repo": { "fork": true } },
                "merge_commit_sha": "merge-sha"
            },
            "review": { "state": "approved" }
        });
        let events = Adapter.project(&payload);
        assert_eq!(events[0].trust_tier, Some(TrustTier::UntrustedForkPullRequest));
    }

    #[test]
    fn review_with_missing_repo_is_untrusted() {
        let payload = serde_json::json!({
            "action": "submitted",
            "pull_request": {
                "number": 12,
                "head": { "sha": "del-sha" }
            },
            "review": { "state": "approved" }
        });
        let events = Adapter.project(&payload);
        assert_eq!(events[0].trust_tier, Some(TrustTier::UntrustedForkPullRequest));
    }
}
