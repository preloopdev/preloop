//! Trust tier classification for workflow runs.
//!
//! Mirrors the trust model implicit in runner.server and the GitHub Actions
//! security model. Each tier corresponds to a combination of event source and
//! repository relationship.

use std::borrow::Cow;
use std::collections::BTreeMap;

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
    /// Dispatched through the GitHub-compatible REST API by a validated
    /// installation token (the caller proved `actions: write`). Same trust
    /// posture as `AdminManual` — repo secrets are injected, matching
    /// github.com, where an App holding `actions: write` dispatches runs
    /// that receive secrets — but the provenance is distinct so runs from
    /// third-party Apps are auditable.
    AppDispatch,
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
                | TrustTier::AppDispatch
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

    /// Returns true if the tier runs code that GitHub would run with its
    /// restricted fork profile: a `pull_request` from a fork, or a fail-closed
    /// unknown event.
    ///
    /// GitHub gives these jobs a read-only `GITHUB_TOKEN` *regardless* of the
    /// workflow's declared `permissions:` block ("Pull requests from public
    /// forks are still considered a special case and will receive a read
    /// token regardless of these settings" — GitHub Changelog, 2021-04-20),
    /// and never issues OIDC tokens to them. The declared block can only
    /// remove read scopes from the fork profile, never add write.
    pub fn is_fork_restricted(&self) -> bool {
        matches!(
            self,
            TrustTier::UntrustedForkPullRequest | TrustTier::Untrusted
        )
    }
}

/// The single effective job-authorization policy for a trust tier.
///
/// Every job-facing authority decision is derived from one call to
/// [`job_authorization`]: whether stored secrets are injected, what
/// `GITHUB_TOKEN` permission set the runner-visible wire variable and the
/// GitHub App installation-token request carry, and whether `id-token: write`
/// yields an OIDC request URL and token grant. Nothing downstream re-derives
/// the tier ad hoc, so a handler special case cannot drift from this policy.
pub(crate) struct JobAuthorization {
    /// Stored repository secrets may be injected into the job.
    pub(crate) allows_secrets: bool,
    /// The runner-visible `GITHUB_TOKEN` permission set (the
    /// `system.github.token.permissions` wire variable). `id-token` appears
    /// here only for trusted tiers, where the declared `IdToken: write` is
    /// the metadata that accompanies the separately granted OIDC request
    /// URL; fork-restricted tiers never advertise it.
    pub(crate) token_permissions: BTreeMap<String, String>,
    /// The permission set sent to the GitHub App installation-token mint.
    /// Excludes the Actions-only scopes (`id-token`, `models`) that the
    /// installation API rejects, so the registered request carries only real
    /// App repository permissions — for trusted and fork jobs alike.
    pub(crate) app_permissions: BTreeMap<String, String>,
    /// `id-token: write` is honored: an OIDC request URL is emitted and the
    /// `oidctoken` endpoint will mint for this job.
    pub(crate) id_token_granted: bool,
    /// The job runs code from an untrusted source; its token authority is
    /// restricted to the GitHub fork profile and no fallback may widen it.
    pub(crate) fork_restricted: bool,
}

/// Resolve a submission's trust tier.
///
/// Native submissions carry no tier (`None` is therefore trusted); a tier
/// that fails to parse is treated the same way, matching the pre-existing
/// secret policy. The webhook dispatcher is the only producer of tier
/// strings and always writes a serialized [`TrustTier`].
pub(crate) fn tier_of(submission: &preloop_gha_protocol::WorkflowSubmission) -> Option<TrustTier> {
    submission
        .trust_tier
        .as_deref()
        .and_then(|value| serde_json::from_value::<TrustTier>(serde_json::json!(value)).ok())
}

/// Whether the job a results/cache request authenticates as is
/// fork-restricted, resolved from its bearer runtime token.
///
/// `Some(true)`/`Some(false)` when the token names a job that still resolves
/// to a run. `None` when the token does not identify a job at all (system
/// token or another control-plane surface) — those are never fork-restricted.
/// This is what lets the cache write handlers deny fork PR runs the same
/// read-only cache access GitHub gives them (restore allowed, save refused)
/// without touching the read path.
///
/// A job-shaped token whose job no longer resolves — the request was retired
/// or purged while the worker still holds the runtime JWT — fails closed to
/// `Some(true)`: the tier can no longer be proven, so the write is refused
/// rather than granted on the strength of a bookkeeping gap.
pub(crate) async fn fork_restricted_from_token(
    state: &crate::state::AppState,
    token: &str,
) -> Option<bool> {
    let payload = state.verify_local_jwt_claims(token)?;
    let subject = payload.get("sub").and_then(|value| value.as_str());
    let scope = payload.get("scp").and_then(|value| value.as_str());
    // Only job runtime tokens carry the fork restriction. Anything not shaped
    // like one (system token, runner-listen, debug-worker surfaces) belongs
    // to the control plane and keeps its existing access.
    let job_shaped = subject.is_some_and(|sub| sub.starts_with("preloop-job-"))
        && scope.is_some_and(|scope| scope.starts_with("Actions.Results:"));
    // Same agreement rule as `AppState::job_uuid_from_token` — `sub` is the
    // job, `scp` is `Actions.Results:{plan_id}:{job_id}`, both must name the
    // same job — but derived from the payload we already verified, so the
    // HMAC/expiry checks run once per cache write instead of twice.
    let job = subject
        .and_then(|sub| sub.strip_prefix("preloop-job-"))
        .and_then(|sub| sub.parse::<uuid::Uuid>().ok())
        .zip(
            scope
                .and_then(|scope| scope.strip_prefix("Actions.Results:"))
                .and_then(|scope| scope.rsplit(':').next())
                .and_then(|job| job.parse::<uuid::Uuid>().ok()),
        )
        .filter(|(subject_job, scope_job)| subject_job == scope_job)
        .map(|(subject_job, _)| subject_job);
    let Some(job) = job else {
        // Signed like a job token but its subject/scope no longer parse to a
        // job: nothing left to prove it trusted, so refuse.
        return job_shaped.then_some(true);
    };
    let inner = state.inner.lock().await;
    // Every hop of the correlation must survive. When the job is retired or
    // purged mid-flight the worker still holds a valid JWT, and the missing
    // record must widen the denial, not the access.
    let Some(request_id) = inner.agent_job_requests.get(&job).copied() else {
        return Some(true);
    };
    let Some(record) = inner.job_requests.get(&request_id) else {
        return Some(true);
    };
    let Some(run) = inner.runs.get(&record.run_id) else {
        return Some(true);
    };
    // A submission without a tier field is a native (trusted) submission and
    // stays allowed; `tier_of`'s parse-failure-is-trusted convention matches
    // the secret policy.
    Some(tier_of(&run.submission).is_some_and(|tier| tier.is_fork_restricted()))
}

/// Reject a cache write when the calling job is a fork-restricted run.
///
/// GitHub gives fork PR runs read-only cache access (restore allowed, save
/// refused) so a fork cannot poison cache entries that trusted runs later
/// restore. Mirroring that here applies to every cache write surface (the
/// cache v2 Twirp handlers and the legacy `/_apis/artifactcache` ones alike);
/// the read handlers never call this. The system token and other
/// non-job-shaped bearers are the control plane's own calls and always pass;
/// a job-shaped token that no longer resolves fails closed instead.
pub(crate) async fn ensure_cache_write_allowed(
    state: &crate::state::AppState,
    headers: &axum::http::HeaderMap,
) -> Result<(), crate::ApiError> {
    let Some(token) = crate::auth::bearer_from_headers(headers) else {
        return Ok(());
    };
    if fork_restricted_from_token(state, token).await == Some(true) {
        return Err(crate::ApiError::forbidden(
            "cache writes are read-only for fork pull request runs",
        ));
    }
    Ok(())
}

/// The effective job-authorization policy for a submission tier and a job's
/// resolved permission declarations.
///
/// For fork-restricted tiers the declared (or default) permission set is
/// clamped to GitHub's fork profile: every scope is held at `read` and write
/// never survives, `id-token` — a special workflow permission with write/none
/// semantics, not a repository read permission — is dropped from the wire set
/// rather than advertised as `read`, `id-token: write` produces no OIDC
/// grant, and stored secrets stay denied. All other tiers keep the declared
/// set verbatim.
pub(crate) fn job_authorization(
    tier: Option<TrustTier>,
    declared_permissions: Option<&BTreeMap<String, String>>,
    declared_id_token_granted: bool,
) -> JobAuthorization {
    let fork_restricted = tier.is_some_and(|tier| tier.is_fork_restricted());
    let allows_secrets = tier.map(|tier| tier.allows_secrets()).unwrap_or(true);
    let token_permissions = fork_permission_set(
        fork_restricted,
        preloop_gha_parser::effective_token_permissions(declared_permissions),
    );
    let app_permissions = token_permissions
        .iter()
        .filter(|(scope, _)| !crate::github_app::ACTIONS_ONLY_SCOPES.contains(&scope.as_str()))
        .map(|(scope, level)| (scope.clone(), level.clone()))
        .collect();
    JobAuthorization {
        allows_secrets,
        token_permissions,
        app_permissions,
        id_token_granted: declared_id_token_granted && !fork_restricted,
        fork_restricted,
    }
}

/// Clamp an effective permission set to GitHub's read-only fork profile.
///
/// Every non-withheld repository scope is lowered to `read`; `none` (already
/// withheld) is preserved. `id-token` never survives: it is not a repository
/// read permission, so a fork job must not be told it holds `IdToken: read`.
/// The remaining scope set is untouched, so the declared block still removes
/// read scopes exactly as GitHub allows.
fn fork_permission_set(
    fork_restricted: bool,
    effective: Cow<'_, BTreeMap<String, String>>,
) -> BTreeMap<String, String> {
    if !fork_restricted {
        return effective.into_owned();
    }
    effective
        .iter()
        .filter(|(scope, _)| scope.as_str() != "id-token")
        .map(|(scope, level)| {
            let clamped = if level.eq_ignore_ascii_case("none") {
                "none"
            } else {
                "read"
            };
            (scope.clone(), clamped.to_owned())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn perms(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(scope, level)| (scope.to_string(), level.to_string()))
            .collect()
    }

    #[test]
    fn fork_restricted_tiers_clamp_every_write_to_read() {
        for tier in [TrustTier::UntrustedForkPullRequest, TrustTier::Untrusted] {
            let policy = job_authorization(
                Some(tier),
                Some(&perms(&[("checks", "write"), ("pull-requests", "write")])),
                true,
            );
            assert_eq!(
                policy.token_permissions,
                perms(&[("checks", "read"), ("pull-requests", "read")]),
                "{tier:?}: write must not survive"
            );
            assert!(!policy.allows_secrets, "{tier:?}: secrets stay denied");
            assert!(!policy.id_token_granted, "{tier:?}: OIDC stays denied");
            assert!(policy.fork_restricted);
        }
    }

    #[test]
    fn fork_profile_preserves_reads_and_withheld_scopes() {
        let policy = job_authorization(
            Some(TrustTier::UntrustedForkPullRequest),
            Some(&perms(&[
                ("contents", "read"),
                ("pull-requests", "none"),
                ("checks", "write"),
            ])),
            false,
        );
        assert_eq!(
            policy.token_permissions,
            perms(&[
                ("contents", "read"),
                ("pull-requests", "none"),
                ("checks", "read"),
            ]),
            "read stays read, none stays withheld, write clamps to read"
        );
    }

    #[test]
    fn fork_id_token_is_never_advertised_as_read() {
        let policy = job_authorization(
            Some(TrustTier::UntrustedForkPullRequest),
            Some(&perms(&[("checks", "write"), ("id-token", "write")])),
            true,
        );
        assert_eq!(
            policy.token_permissions,
            perms(&[("checks", "read")]),
            "id-token is not a repository read permission: no IdToken key on the wire"
        );
        assert_eq!(
            policy.app_permissions,
            perms(&[("checks", "read")]),
            "the App request map carries no id-token either"
        );
        assert!(
            !policy.id_token_granted,
            "the OIDC grant stays denied for the fork"
        );

        // Declaring *only* id-token leaves an empty set: the fork must not be
        // handed any permission metadata at all.
        let only_oidc = job_authorization(
            Some(TrustTier::UntrustedForkPullRequest),
            Some(&perms(&[("id-token", "write")])),
            true,
        );
        assert!(only_oidc.token_permissions.is_empty());
        assert!(only_oidc.app_permissions.is_empty());
    }

    #[test]
    fn trusted_id_token_stays_wire_metadata_but_not_in_the_app_request() {
        let policy = job_authorization(
            Some(TrustTier::Trusted),
            Some(&perms(&[("checks", "write"), ("id-token", "write")])),
            true,
        );
        assert_eq!(
            policy.token_permissions,
            perms(&[("checks", "write"), ("id-token", "write")]),
            "trusted wire metadata keeps IdToken: write for the OIDC URL/grant"
        );
        assert_eq!(
            policy.app_permissions,
            perms(&[("checks", "write")]),
            "the App installation-token request excludes the non-App id-token scope"
        );
        assert!(policy.id_token_granted);
    }

    #[test]
    fn fork_profile_with_empty_declared_set_stays_empty() {
        let policy = job_authorization(
            Some(TrustTier::UntrustedForkPullRequest),
            Some(&BTreeMap::new()),
            false,
        );
        assert!(
            policy.token_permissions.is_empty(),
            "`permissions: {{}}` on a fork must not gain the default back"
        );
    }

    #[test]
    fn fork_profile_applies_to_the_implicit_default_too() {
        let policy = job_authorization(Some(TrustTier::UntrustedForkPullRequest), None, false);
        assert_eq!(
            policy.token_permissions,
            preloop_gha_parser::DEFAULT_TOKEN_PERMISSIONS
                .iter()
                .map(|&(scope, level)| (scope.to_owned(), level.to_owned()))
                .collect::<BTreeMap<_, _>>(),
            "the default set is already read-only, so the fork keeps it"
        );
    }

    #[test]
    fn trusted_tiers_keep_declared_permissions_and_grants() {
        for tier in [
            None,
            Some(TrustTier::Trusted),
            Some(TrustTier::Internal),
            Some(TrustTier::InternalPullRequest),
            Some(TrustTier::PullRequestTarget),
            Some(TrustTier::AdminManual),
            Some(TrustTier::Deployment),
            Some(TrustTier::Schedule),
        ] {
            let policy = job_authorization(
                tier,
                Some(&perms(&[("checks", "write"), ("contents", "read")])),
                true,
            );
            assert_eq!(
                policy.token_permissions,
                perms(&[("checks", "write"), ("contents", "read")]),
                "{tier:?}: declared writes must survive"
            );
            assert!(policy.allows_secrets, "{tier:?}: secrets allowed");
            assert!(policy.id_token_granted, "{tier:?}: OIDC granted");
            assert!(!policy.fork_restricted);
        }
    }
}
