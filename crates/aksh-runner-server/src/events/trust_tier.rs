//! Trust tier classification for workflow runs.
//!
//! Mirrors the trust model implicit in runner.server and the GitHub Actions
//! security model. Each tier corresponds to a combination of event source and
//! repository relationship.

use serde::{Deserialize, Serialize};

/// Trust tier stamped on every webhook-driven run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TrustTier {
    /// Push to the default branch from a trusted source.
    Trusted,
    /// Push to a non-default branch.
    Internal,
    /// Pull request from a branch within the same repository.
    InternalPullRequest,
    /// Pull request from a fork.
    UntrustedForkPullRequest,
    /// `pull_request_target` event — always runs with base-repo trust
    /// regardless of fork status.
    PullRequestTarget,
    /// Manually dispatched via `workflow_dispatch`.
    AdminManual,
    /// Fired by a release or deployment event.
    Deployment,
    /// Fired by the internal schedule executor.
    Schedule,
    /// Fired by any other webhook event with unknown trust.
    Untrusted,
}

impl TrustTier {
    /// Returns true if the tier allows checking out the head commit (i.e.
    /// the untrusted PR code).
    pub fn allows_head_checkout(&self) -> bool {
        matches!(
            self,
            TrustTier::Trusted
                | TrustTier::Internal
                | TrustTier::InternalPullRequest
                | TrustTier::AdminManual
                | TrustTier::Deployment
                | TrustTier::Schedule
        )
    }

    /// Returns true if the server may inject repository secrets for this tier.
    /// `submit_run_inner` applies this policy before a job message is built.
    pub fn allows_secrets(&self) -> bool {
        !matches!(
            self,
            TrustTier::UntrustedForkPullRequest | TrustTier::Untrusted
        )
    }
}
