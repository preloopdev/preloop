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

        vec![EffectiveEvent {
            event: "pull_request_review".to_owned(),
            git_ref,
            sha: Some(sha.clone()),
            status_check_sha: head_sha.map(|s| s.to_owned()),
            activity_type,
            trust_tier: Some(TrustTier::InternalPullRequest),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}
