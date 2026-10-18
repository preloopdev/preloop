//! Local engine configuration file (`~/.preloop/config.toml`).
//!
//! The file holds GitHub credential configuration and stored job secrets.
//! Every field is overridable by the legacy environment variables, which
//! take precedence — the file is the durable store that `preloop setup`
//! and `preloop secret` write, while env vars remain the escape hatch for
//! containerized/deployed engines.
//!
//! Secret values in the `[secrets]` tables are envelope-sealed at rest
//! (see [`SEALED_SECRET_PREFIX`]): the file holds ciphertext, never the
//! plaintext. The data-encryption key lives in the engine's credential store
//! (the private file store by default; see [`crate::credential_store`]), and a
//! 0600 `secrets.key` file next to the config is used only when that store is
//! unreachable (documented below). The config file itself is written
//! with mode 0600 and secret values are never echoed back by
//! `preloop secret list` or `preloop doctor`.

use crate::credential_store::{CredentialRef, CredentialStore, SecretString};
use anyhow::Context;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Env var pointing at the config file. The engine sets it to
/// `{preloop_home}/config.toml`; the default matches when HOME is used.
pub const CONFIG_PATH_ENV: &str = "PRELOOP_CONFIG";

/// Env var overriding the golden's base image at serve time. Always wins
/// over the `[golden]` section, so a deployment can override the persisted
/// choice without rewriting the file.
pub const BASE_IMAGE_ENV: &str = "PRELOOP_RUNNER_BASE_IMAGE";

/// The golden every job VM forks from, as chosen by `preloop init`.
///
/// Only `base_image` is read at serve time; the other keys exist so the
/// wizard can show the previous answer on a re-run and change one step
/// without re-asking the rest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GoldenConfig {
    /// How the base image was chosen: `official`, `oci`, `dockerfile`, or
    /// `file`. Recorded for the wizard's re-run story; `serve` ignores it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Base image `serve` boots its golden from: an OCI reference, a
    /// docker-save `.tar`, or a `.smolmachine`/rootfs path. Absent selects the
    /// packed official GitHub runner golden — the image `runs-on:
    /// ubuntu-latest` means, downloaded and verified, never baked locally.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_image: Option<String>,
    /// Dockerfile a `kind = "dockerfile"` tar was built from, so a re-run can
    /// rebuild the same context without asking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dockerfile: Option<PathBuf>,
}

/// Effective golden base image: `env` (the resolved [`BASE_IMAGE_ENV`] value)
/// when set to a non-blank value, else the `[golden]` section. `None` means
/// the packed official golden.
///
/// An exported-but-blank variable behaves like an unset one — the same rule
/// `PRELOOP_GOLDEN_URL` follows — so a shell that exports an empty value
/// cannot silently configure an empty base image.
pub fn golden_base_image(config: &ConfigFile, env: Option<&str>) -> Option<String> {
    if let Some(value) = env
        && !value.trim().is_empty()
    {
        return Some(value.to_owned());
    }
    config
        .golden
        .base_image
        .clone()
        .filter(|value| !value.trim().is_empty())
}

/// GitHub credential configuration, mirrored 1:1 by the `PRELOOP_GITHUB_*`
/// environment variables.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct GitHubConfig {
    /// GitHub App ID.
    #[serde(default)]
    pub app_id: Option<String>,
    /// GitHub App private key PEM, stored inline (legacy).
    ///
    /// Read from disk and migrated into the selected credential store on the next
    /// `preloop serve`; never populated from the credential store, so a
    /// load/write round-trip can neither lose an unmigrated credential nor
    /// reintroduce plaintext for a migrated one. Use [`GitHubConfig::app_pem`]
    /// to read the effective value.
    #[serde(default, rename = "app_pem")]
    pub legacy_app_pem: Option<String>,
    /// Credential-store reference for the App private key.
    #[serde(default)]
    pub app_pem_ref: Option<String>,
    /// Value behind [`Self::app_pem_ref`], resolved at load. Never persisted.
    #[serde(skip)]
    pub resolved_app_pem: Option<SecretString>,
    /// Mint-failure policy: `local`, `error`, or `pat`.
    #[serde(default)]
    pub mint_failure: Option<String>,
    /// PAT used as the fallback when App minting fails under the `pat`
    /// policy. Also the credential for the `--via pat` setup path.
    /// Stored inline (legacy); see [`Self::legacy_app_pem`].
    ///
    /// A static PAT cannot be narrowed per job. When no GitHub App is
    /// configured, submission introspects the PAT's classic OAuth scopes and
    /// refuses runs whose declared `permissions:` are narrower than the PAT.
    /// Prefer a GitHub App so installation tokens are minted least-privilege.
    #[serde(default, rename = "pat")]
    pub legacy_pat: Option<String>,
    /// Credential-store reference for the PAT.
    #[serde(default)]
    pub pat_ref: Option<String>,
    /// Value behind [`Self::pat_ref`], resolved at load. Never persisted.
    #[serde(skip)]
    pub resolved_pat: Option<SecretString>,
    /// Shared secret for `X-Hub-Signature-256` webhook verification.
    /// Stored inline (legacy); see [`Self::legacy_app_pem`].
    #[serde(default, rename = "webhook_secret")]
    pub legacy_webhook_secret: Option<String>,
    /// Credential-store reference for the webhook secret.
    #[serde(default)]
    pub webhook_secret_ref: Option<String>,
    /// Value behind [`Self::webhook_secret_ref`], resolved at load. Never persisted.
    #[serde(skip)]
    pub resolved_webhook_secret: Option<SecretString>,
    /// GitHub server URL exposed to workflows as `github.server_url` /
    /// `GITHUB_SERVER_URL`. Defaults to `https://github.com`; point it at a
    /// GHES-style host when the engine fronts one. Env: `PRELOOP_GITHUB_SERVER_URL`.
    #[serde(default)]
    pub server_url: Option<String>,
    /// GitHub REST API base URL exposed as `github.api_url` /
    /// `GITHUB_API_URL`. Defaults to `https://api.github.com`. Also used for
    /// remote workflow/action fetches. Env: `PRELOOP_GITHUB_API_URL`.
    #[serde(default)]
    pub api_url: Option<String>,
    /// GitHub GraphQL endpoint exposed as `github.graphql_url` /
    /// `GITHUB_GRAPHQL_URL`. Defaults to `https://api.github.com/graphql`.
    /// Env: `PRELOOP_GITHUB_GRAPHQL_URL`.
    #[serde(default)]
    pub graphql_url: Option<String>,
    /// Additional registered GitHub Apps beyond the legacy single-App env
    /// vars (which remain the default first entry). Lets several Apps
    /// coexist: each gets its own webhook secret in the receiver and its own
    /// installation tokens for minting. Env override: `PRELOOP_GITHUB_APPS_JSON`
    /// (a JSON array of the same shape).
    #[serde(default)]
    pub apps: Vec<AppConfig>,
    /// Auto-PR policy for webhook-driven push runs (see [`PrConfig`]).
    #[serde(default)]
    pub pr: PrConfig,
}

impl GitHubConfig {
    /// Effective App private key: the credential store wins, the inline
    /// legacy value is the pre-migration fallback.
    pub fn app_pem(&self) -> Option<&str> {
        self.resolved_app_pem
            .as_ref()
            .map(|s| s.expose())
            .or(self.legacy_app_pem.as_deref())
    }

    /// Effective PAT. See [`Self::app_pem`].
    pub fn pat(&self) -> Option<&str> {
        self.resolved_pat
            .as_ref()
            .map(|s| s.expose())
            .or(self.legacy_pat.as_deref())
    }

    /// Effective webhook secret. See [`Self::app_pem`].
    pub fn webhook_secret(&self) -> Option<&str> {
        self.resolved_webhook_secret
            .as_ref()
            .map(|s| s.expose())
            .or(self.legacy_webhook_secret.as_deref())
    }
}

/// One additional GitHub App in the multi-App registry (`github.apps`).
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct AppConfig {
    /// Numeric App id, used as the `iss` claim of the App JWT. Accepts a
    /// string or integer on deserialize — GitHub App ids are numbers, and
    /// `github.apps` / `PRELOOP_GITHUB_APPS_JSON` entries are written by
    /// hand, so `app_id: 12345` and `app_id: "12345"` must both load.
    #[serde(deserialize_with = "de_string_or_integer")]
    pub app_id: String,
    /// App private key PEM, stored inline (legacy). See
    /// [`GitHubConfig::legacy_app_pem`]; read via [`AppConfig::pem`].
    #[serde(default, rename = "pem")]
    pub legacy_pem: String,
    /// Credential-store reference for the App private key.
    #[serde(default)]
    pub pem_ref: Option<String>,
    /// Value behind [`Self::pem_ref`], resolved at load. Never persisted.
    #[serde(skip)]
    pub resolved_pem: Option<SecretString>,
    /// Webhook secret for `X-Hub-Signature-256` verification, when the App
    /// has its own (the legacy `PRELOOP_WEBHOOK_SECRET` covers the default
    /// App). Stored inline (legacy); read via [`AppConfig::webhook_secret`].
    #[serde(default, rename = "webhook_secret")]
    pub legacy_webhook_secret: Option<String>,
    /// Credential-store reference for the App webhook secret.
    #[serde(default)]
    pub webhook_secret_ref: Option<String>,
    /// Value behind [`Self::webhook_secret_ref`], resolved at load. Never persisted.
    #[serde(skip)]
    pub resolved_webhook_secret: Option<SecretString>,
    /// Explicit installation id, bypassing installation discovery for
    /// single-installation deployments of this App.
    #[serde(default)]
    pub installation_id: Option<u64>,
}

impl AppConfig {
    /// Effective App private key. See [`GitHubConfig::app_pem`].
    pub fn pem(&self) -> &str {
        self.resolved_pem
            .as_ref()
            .map(|s| s.expose())
            .unwrap_or(&self.legacy_pem)
    }

    /// Effective webhook secret. See [`GitHubConfig::app_pem`].
    pub fn webhook_secret(&self) -> Option<&str> {
        self.resolved_webhook_secret
            .as_ref()
            .map(|s| s.expose())
            .or(self.legacy_webhook_secret.as_deref())
    }
}

/// Accept a string or integer for the numeric `app_id`, normalizing to a
/// string. GitHub App ids are numbers; registry entries are written by hand,
/// so both shapes must load. Anything else is a typo worth failing on.
fn de_string_or_integer<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Visitor;

    struct StringOrInteger;

    impl<'de> Visitor<'de> for StringOrInteger {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a string or integer")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_owned())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_string())
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_string())
        }
    }

    deserializer.deserialize_any(StringOrInteger)
}

/// When a webhook-driven push run succeeds, should the server open a pull
/// request for its branch?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PrAuto {
    /// Open a PR for any branch that is not the repository's default branch,
    /// is not excluded by pattern, and has no open PR yet (the default).
    #[default]
    Feature,
    /// Never open PRs automatically; `[pr]` bypasses only the auto policy.
    /// Default-branch, exclusion, deduplication, and credential checks still
    /// apply.
    Never,
}

/// Auto-PR policy: which successful push runs get a pull request opened on
/// GitHub, and how the PR is created.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrConfig {
    /// When to open PRs. Env: `PRELOOP_GITHUB_PR_AUTO` (`feature|never`).
    pub auto: PrAuto,
    /// Open newly-created PRs as drafts. Drafts keep reviewers out until the
    /// author marks them ready. Env: `PRELOOP_GITHUB_PR_DRAFT`.
    pub draft: bool,
    /// Branch patterns (gitignore-style) never to open a PR for.
    /// Env: `PRELOOP_GITHUB_PR_EXCLUDE` (comma-separated).
    pub exclude: Vec<String>,
}

impl Default for PrConfig {
    fn default() -> Self {
        Self {
            auto: PrAuto::default(),
            // Draft by default: an auto-opened PR should not surprise
            // reviewers before the author marks it ready.
            draft: true,
            exclude: Vec::new(),
        }
    }
}

/// Renders as `<redacted>` / `None` without quoting, for credential fields.
fn redacted(present: bool) -> impl std::fmt::Debug {
    struct Marker(bool);
    impl std::fmt::Debug for Marker {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(if self.0 { "<redacted>" } else { "None" })
        }
    }
    Marker(present)
}

/// Manual `Debug`: this struct carries a PAT and an RSA private key, so the
/// derived impl would disclose both on a single `debug!(?config)`. Presence
/// only — `Serialize` still emits the inline legacy values for the 0600 TOML
/// file, but never the credential-store-resolved ones.
impl std::fmt::Debug for GitHubConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubConfig")
            .field("app_id", &self.app_id)
            .field("app_pem", &redacted(self.app_pem().is_some()))
            .field("mint_failure", &self.mint_failure)
            .field("pat", &redacted(self.pat().is_some()))
            .field("webhook_secret", &redacted(self.webhook_secret().is_some()))
            .field("apps", &self.apps.len())
            .field("pr", &self.pr)
            .finish()
    }
}

/// Checkout object retention policy. Disabled is the compatibility default:
/// workflows fetch directly from their forge exactly as they do today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CheckoutCacheMode {
    #[default]
    Off,
    RunScoped,
    Repository,
}

/// Lenient mode deserialization for the config file: an unknown value warns
/// and falls back to `off` (no retention) instead of refusing to boot. Off
/// is the safe state — unlike secret handling, there is nothing dangerous
/// about retaining less — so a typo must cost caching, never startup.
fn de_checkout_cache_mode<'de, D>(deserializer: D) -> Result<CheckoutCacheMode, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    match parse_checkout_cache_mode(&raw) {
        Ok(mode) => Ok(mode),
        Err(_) => {
            tracing::warn!(
                "unknown checkout cache mode `{raw}` in config file; falling back to `off`"
            );
            Ok(CheckoutCacheMode::Off)
        }
    }
}

/// Effective checkout-cache configuration. Durations are seconds so the file,
/// environment, native API, and eventual UI share one unambiguous wire shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CheckoutCacheConfig {
    #[serde(default, deserialize_with = "de_checkout_cache_mode")]
    pub mode: CheckoutCacheMode,
    pub run_retention_seconds: u64,
    pub repository_retention_seconds: u64,
    pub max_bytes: u64,
}

/// Actions cache (`actions/cache`) retention. Finalized cache entries live in
/// `<state_dir>/cache/`; without a quota they accumulate for the lifetime of
/// the state directory (round-3 §11: a 150 MB blob finalized into a permanent
/// 151 MB entry, no eviction at all).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    /// Total bytes of finalized cache entries the server keeps on disk,
    /// across all namespaces. When a commit pushes the total over this
    /// quota, the least-recently-used entries are evicted until the total
    /// fits again. A single entry larger than the quota is kept by the
    /// commit that created it and evicted by the next commit.
    /// Default 10 GiB, matching GitHub's 10 GB per-repository cache limit.
    /// Env: `PRELOOP_CACHE_TOTAL_QUOTA_BYTES` wins over this key.
    pub total_quota_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            total_quota_bytes: DEFAULT_CACHE_TOTAL_QUOTA_BYTES,
        }
    }
}

/// Default total Actions-cache quota: 10 GiB, matching GitHub's 10 GB
/// per-repository cache limit (this is the server-wide total).
pub const DEFAULT_CACHE_TOTAL_QUOTA_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// One GitHub-configured deployment reviewer (`User` or `Team`), as
/// `GET /repos/{o}/{r}/environments/{name}` reports inside a
/// `required_reviewers` protection rule. Identity is what GitHub uses: a
/// login for users, an org-scoped slug for teams. This field is never read
/// from `[environment_rules]` — preloop's local fallback has no user
/// identities to compare — so the serde surface skips it; it is populated
/// only by the GitHub rules resolver (`environment_resolver`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvironmentReviewer {
    /// A GitHub user, matched by login (case-insensitive).
    User(String),
    /// A GitHub team: org login + team slug, expanded via
    /// `GET /orgs/{org}/teams/{slug}/members` at approval time.
    Team { org: String, slug: String },
}

/// Operator protection rules for one registered environment, mirroring
/// GitHub's environment protection rules. Every field is optional and
/// empty/zero by default: a missing `[environment_rules."owner/repo".env]`
/// table (or an empty one) means "no rules" and preserves today's behavior.
/// Rules are enforced at scheduler admission, before environment secrets are
/// injected — a job that fails the branch policy never sees the environment's
/// secrets.
///
/// The same struct carries rules fetched from GitHub's environments API when
/// a GitHub App is configured (`environment_resolver`); the GitHub-only
/// fields are `skip`ped in serde so a config file cannot set them — the TOML
/// surface stays the single-operator fallback.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EnvironmentRules {
    /// Git branch patterns allowed to deploy to this environment. Compared
    /// against the run's `git_ref` after stripping a leading `refs/heads/`
    /// from both sides, so `main` matches `refs/heads/main`. GitHub treats
    /// deployment branch policies as `File.fnmatch` patterns (a `*` never
    /// crosses `/`), so matching goes through
    /// `preloop_gha_parser::glob_match`. Empty means branches are
    /// unrestricted only when *no* policy is configured — a configured
    /// policy (a non-empty `deployment_branches`/`deployment_tags` or a
    /// GitHub-side `deployment_branch_policy`, which sets
    /// `branch_policy_restricted`) with an empty branch list deploys no
    /// branch. Under GitHub's `protected_branches` mode the resolver expands
    /// the repo's protected branches into this list and sets
    /// `protected_branches_only`; an empty list there means the repository
    /// has no branch protection rules, and GitHub lets every branch deploy.
    /// A run on a disallowed ref fails the job closed at admission.
    #[serde(default)]
    pub deployment_branches: Vec<String>,
    /// Git tag patterns allowed to deploy to this environment, matched
    /// against `refs/tags/*` refs (GitHub's `type: "tag"` deployment branch
    /// policies). Branches are never checked against tag patterns and vice
    /// versa. Empty means tags are unrestricted only when *no* policy is
    /// configured — a configured policy with an empty tag list deploys no
    /// tag, and GitHub's `protected_branches` mode denies every tag.
    #[serde(default)]
    pub deployment_tags: Vec<String>,
    /// Minutes a job waits after becoming eligible before it may start.
    /// The wait is visible (job status `pending`) and cancellable. Zero
    /// means no wait.
    #[serde(default)]
    pub wait_timer_minutes: u64,
    /// Number of explicit approvals required before the job may start.
    /// Approvals are recorded via
    /// `POST /api/v1/runs/:run_id/jobs/:job_id/approve` (system token) or
    /// `preloop approve`. Zero means no approval gate. A job not approved
    /// within 24 hours of entering the gate fails closed.
    ///
    /// Under the local TOML fallback there are no user identities: every
    /// approval is authenticated with the single operator system token, so
    /// values above 1 would imply a separation-of-duties guarantee that
    /// cannot exist and config loading rejects them (fail closed). GitHub
    /// sources this rule differently: its `required_reviewers` protection
    /// rule is a *set* of up to six reviewers of which one approval suffices
    /// (https://docs.github.com/en/rest/deployments/environments), so the
    /// resolver writes `required_reviewers: 1` here and the reviewer list
    /// into `reviewers`.
    #[serde(default)]
    pub required_reviewers: u32,
    /// GitHub `required_reviewers` rule: the reviewers allowed to approve,
    /// as GitHub reports them. Empty `reviewers` with
    /// `required_reviewers > 0` means the local fallback (system-token
    /// approval) — a GitHub-fetched rule always populates this.
    #[serde(skip)]
    pub reviewers: Vec<EnvironmentReviewer>,
    /// GitHub `prevent_self_review`: the run's own actor may not approve.
    #[serde(skip)]
    pub prevent_self_review: bool,
    /// GitHub `deployment_branch_policy` was set (`protected_branches` or
    /// `custom_branch_policies`): deployment refs are restricted. Kept
    /// separate from the pattern lists because GitHub denies *every* ref
    /// when the policy is configured but matches nothing (e.g. a custom
    /// policy with zero patterns).
    #[serde(skip)]
    pub branch_policy_restricted: bool,
    /// GitHub `deployment_branch_policy.protected_branches`: only branches
    /// carrying branch protection rules may deploy. The resolver expands
    /// this to the repo's protected-branch list in `deployment_branches`;
    /// exact matching applies (no globs), and non-branch refs are denied.
    #[serde(skip)]
    pub protected_branches_only: bool,
    /// GitHub custom deployment protection rules enabled on this
    /// environment (third-party App slugs). These are enforced by external
    /// Apps over the `deployment_protection_rule` webhook + callback token —
    /// a protocol preloop cannot impersonate — so a non-empty list fails the
    /// job closed with an explicit message rather than bypass the rule.
    #[serde(skip)]
    pub custom_protection_rules: Vec<String>,
}

/// `[environment_rules]` table shape: `owner/repo` -> environment name ->
/// protection rules.
pub type EnvironmentRulesMap = BTreeMap<String, BTreeMap<String, EnvironmentRules>>;

impl Default for CheckoutCacheConfig {
    fn default() -> Self {
        Self {
            mode: CheckoutCacheMode::Off,
            run_retention_seconds: 3_600,
            repository_retention_seconds: 7 * 24 * 60 * 60,
            max_bytes: 100 * 1024 * 1024 * 1024,
        }
    }
}

/// Permission level for the token permissions ceiling. Strict: an unknown
/// level string fails config parsing rather than silently weakening policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionLevel {
    None,
    #[default]
    Read,
    Write,
    Admin,
}

impl PermissionLevel {
    /// Authority rank: none < read < write < admin. Matches the ranking used
    /// when clamping minted tokens to the App installation's grants.
    pub fn rank(self) -> u8 {
        match self {
            PermissionLevel::None => 0,
            PermissionLevel::Read => 1,
            PermissionLevel::Write => 2,
            PermissionLevel::Admin => 3,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            PermissionLevel::None => "none",
            PermissionLevel::Read => "read",
            PermissionLevel::Write => "write",
            PermissionLevel::Admin => "admin",
        }
    }
}

/// Operator ceiling on `GITHUB_TOKEN` permissions (`[token_permissions_ceiling]`),
/// mirroring GitHub's workflow-permissions defaults as a hard cap: a workflow
/// may declare less than the ceiling, never more. The effective permission
/// per scope is the minimum of the workflow-declared set, this ceiling, the
/// fork-restricted profile (which stays the floor for fork PR jobs), and the
/// GitHub App installation's grants.
///
/// Absent by default: no ceiling is applied and today's behavior is unchanged.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TokenPermissionsCeiling {
    /// Cap applied to scopes not explicitly listed. Default `read`.
    #[serde(default)]
    pub r#default: PermissionLevel,
    /// Per-scope caps, e.g. `contents = "read"`. Remaining keys are scope
    /// names; unknown keys become scope caps (caps only ever reduce).
    #[serde(flatten)]
    pub scopes: BTreeMap<String, PermissionLevel>,
    /// Mirror of GitHub's "Allow GitHub Actions to create and approve pull
    /// requests" toggle. When false (default), `pull-requests` is capped at
    /// `read` no matter what the ceiling table says, so the minted token
    /// cannot create or approve pull requests.
    #[serde(default)]
    pub allow_create_approve_pr: bool,
}

/// Enforcement mode for workflow execution protections.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProtectionMode {
    /// Log what would be denied without denying it. The safe default for
    /// rolling policy out: nothing changes until the operator flips to
    /// `enforce`.
    #[default]
    Evaluate,
    /// Deny matching triggers.
    Enforce,
}

/// The only rule action supported today. Unknown actions fail config
/// parsing so a typo can never silently weaken policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyRuleAction {
    #[default]
    Deny,
}

/// Deny rule on the event that may trigger a workflow, e.g.
/// `pull_request_target`. Mirrors GitHub's event rules.
///
/// Unknown fields are rejected at parse time: a typo must fail closed
/// rather than silently leave the policy weaker than written.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRule {
    /// Event name, e.g. `pull_request_target`, `workflow_dispatch`.
    pub event: String,
    /// Workflow filename globs (e.g. `deploy.yml`, `release/*.yml`),
    /// matched against the bare filename and `.github/workflows/<name>`.
    /// Only `*`, `**`, and `?` are supported — character classes like
    /// `[0-9]` are rejected at config load. Omitted or empty = every workflow.
    #[serde(default)]
    pub workflows: Option<Vec<String>>,
    #[serde(default)]
    pub action: PolicyRuleAction,
}

/// Deny rule on the actor that may trigger a workflow. The actor is the
/// webhook payload's `sender.login`. Mirrors GitHub's actor rules.
///
/// Unknown fields are rejected at parse time, as in [`EventRule`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorRule {
    /// Sender login, compared case-insensitively.
    pub actor: String,
    /// Workflow filename globs, as in [`EventRule::workflows`].
    /// Omitted or empty = every workflow.
    #[serde(default)]
    pub workflows: Option<Vec<String>>,
    #[serde(default)]
    pub action: PolicyRuleAction,
}

/// Workflow execution protections: admin-level deny policy on which events
/// and which actors may trigger workflows. Mirrors GitHub's execution
/// protections (event rules, actor rules, per-file targeting,
/// evaluate/enforce modes).
///
/// Rules live in the operator's server config — never in workflow repos —
/// so workflow authors cannot weaken the policy that constrains them.
/// A `pull_request_target` kill is an event rule with
/// `event = "pull_request_target"`.
///
/// Every fresh config carries one rule by default: GitHub's default
/// `pull_request_target` rule, in evaluate (log-only) mode — see
/// [`ExecutionProtectionConfig::default_pull_request_target_rule`]. The
/// operator removes it with an explicit `event_rules = []`, or through the
/// `DELETE /api/v1/execution-protection/rules/event-0` management endpoint.
///
/// Unknown fields are rejected at parse time, as in [`EventRule`].
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionProtectionConfig {
    #[serde(default)]
    pub mode: ProtectionMode,
    #[serde(default = "ExecutionProtectionConfig::default_event_rules")]
    pub event_rules: Vec<EventRule>,
    #[serde(default)]
    pub actor_rules: Vec<ActorRule>,
}

impl ExecutionProtectionConfig {
    /// GitHub's default execution-protection rule: `pull_request_target`
    /// denied on public repos — evaluate (log-only) mode first, enforced
    /// later. Preloop has no public/private repo distinction, so
    /// the default applies server-wide in evaluate mode: it logs what it
    /// would deny without changing what runs.
    ///
    /// This is a real rule in the operator's config, visible through the
    /// management API as `event-0` and removable with
    /// `DELETE /api/v1/execution-protection/rules/event-0` (the API writes
    /// an explicit empty `event_rules` list back, so the default does not
    /// reappear on the next load) or with `event_rules = []` in the config
    /// file.
    ///
    /// Preloop does not auto-flip this rule to enforce — the
    /// operator flips the whole table to `enforce` mode when they are ready.
    pub fn default_pull_request_target_rule() -> EventRule {
        EventRule {
            event: "pull_request_target".to_owned(),
            workflows: None,
            action: PolicyRuleAction::Deny,
        }
    }

    fn default_event_rules() -> Vec<EventRule> {
        vec![Self::default_pull_request_target_rule()]
    }
}

impl Default for ExecutionProtectionConfig {
    fn default() -> Self {
        Self {
            mode: ProtectionMode::Evaluate,
            event_rules: Self::default_event_rules(),
            actor_rules: Vec::new(),
        }
    }
}

/// Fork pull-request workflow policy, mirroring GitHub's "Fork pull request
/// workflows" admin settings. Rules live in the operator's server config —
/// never in workflow repos — so a fork author cannot weaken the policy that
/// constrains their own workflows.
///
/// Deliberately narrow: GitHub's "send write tokens" and "send secrets"
/// toggles are not knobs here — preloop never sends secrets or write tokens
/// to fork-PR workflows (the [`TrustTier::UntrustedForkPullRequest`] tier
/// hardcodes the safe answer), so there is nothing to configure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ForkPolicyConfig {
    /// Whether workflows may run at all for fork pull-request events
    /// (`pull_request` / `pull_request_review` from a fork). `false` skips
    /// the event in the webhook intake path before any workflow is matched.
    /// Default `true` preserves today's behavior.
    #[serde(default = "default_true")]
    pub run_fork_workflows: bool,
    /// Whether fork-PR workflow runs wait for explicit operator approval
    /// before any job may start. The run is created and held at scheduler
    /// admission; approve it with
    /// `POST /api/v1/runs/:run_id/approve-fork` (system Bearer <redacted>) or
    /// `preloop approve-fork`. A run not approved within 24 hours fails
    /// closed. Default `false` preserves today's behavior.
    #[serde(default)]
    pub require_approval: bool,
}

fn default_true() -> bool {
    true
}

impl Default for ForkPolicyConfig {
    fn default() -> Self {
        Self {
            run_fork_workflows: true,
            require_approval: false,
        }
    }
}

/// The engine configuration file.
#[derive(Clone, Serialize, Deserialize)]
pub struct ConfigFile {
    #[serde(default)]
    pub github: GitHubConfig,
    /// Golden (the VM image every job forks from) chosen by `preloop init`.
    /// `serve` reads it when `PRELOOP_RUNNER_BASE_IMAGE` is unset; the
    /// environment variable always takes precedence.
    #[serde(default)]
    pub golden: GoldenConfig,
    /// Optional checkout-object reuse. `off` is the default and preserves
    /// direct per-job forge checkout.
    #[serde(default)]
    pub checkout_cache: CheckoutCacheConfig,
    /// Actions cache (`actions/cache`) retention (`[cache]`). Quota-bounded
    /// with LRU eviction; default 10 GiB total across all namespaces.
    #[serde(default)]
    pub cache: CacheConfig,
    /// Stored job secrets injected into every trusted job, mirroring
    /// GitHub's org-level secrets.
    #[serde(default)]
    pub secrets: BTreeMap<String, String>,
    /// Per-repository job secrets (`[repo_secrets."owner/repo"]`), mirroring
    /// GitHub's repo-level secrets. A per-repo secret overrides the global
    /// secret of the same name for that repository.
    #[serde(default)]
    pub repo_secrets: BTreeMap<String, BTreeMap<String, String>>,
    /// Per-repository, per-environment job secrets
    /// (`[env_secrets."owner/repo".prod]`), mirroring GitHub's
    /// environment-level secrets. A name here overrides the repo-level and
    /// global secret of the same name for jobs in that environment.
    #[serde(default)]
    pub env_secrets: BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>,
    /// Removed environment registry (`[environments]` mapping `owner/repo` to
    /// a list of environment names).
    ///
    /// GitHub accepts any `environment:` name, auto-creates it on first
    /// reference, and applies whatever protection rules the repository
    /// configured for it — preloop now behaves the same way (rules come from
    /// `[environment_rules]` in local mode and from the repository's
    /// environments API when a GitHub credential exists). A registry that
    /// *rejects* unregistered names would diverge from GitHub, so it no
    /// longer gates submissions; a non-empty table is rejected at config load
    /// with a pointer to this change rather than silently ignored.
    #[serde(default)]
    pub environments: BTreeMap<String, BTreeSet<String>>,
    /// Token permissions ceiling (`[token_permissions_ceiling]`), mirroring
    /// GitHub's workflow-permissions defaults as a hard operator cap.
    /// `None` (absent table) = no ceiling; today's behavior is unchanged.
    #[serde(default)]
    pub token_permissions_ceiling: Option<TokenPermissionsCeiling>,
    /// Per-environment protection rules (`[environment_rules."owner/repo".env]`),
    /// mirroring GitHub's environment protection rules. Each rule set is
    /// optional and empty by default; a missing entry means "no rules" and
    /// preserves today's behavior.
    #[serde(default)]
    pub environment_rules: EnvironmentRulesMap,
    /// Fork pull-request workflow policy (`[fork_policy]`), mirroring
    /// GitHub's "Fork pull request workflows" admin settings. Empty by
    /// default: fork-PR workflows run as today until the operator writes
    /// policy.
    #[serde(default)]
    pub fork_policy: ForkPolicyConfig,
    /// Workflow execution protections (`[execution_protection]`), mirroring
    /// GitHub's admin-level event/actor rules. Carries GitHub's default
    /// `pull_request_target` rule in evaluate (log-only) mode unless the
    /// operator removes it (explicit `event_rules = []`, or the rule
    /// management API). A config change takes effect on restart, like the
    /// other policy tables in the config file.
    #[serde(default)]
    pub execution_protection: ExecutionProtectionConfig,
    /// Run/check/status retention, in days, mirroring GitHub's Actions
    /// retention setting. A background sweep deletes terminal runs — and
    /// their check-run records, commit statuses, artifacts, and logs —
    /// once they are older than this. Default 90 follows GitHub. `0`
    /// disables the sweep: runs accumulate indefinitely, as before.
    /// Env: `PRELOOP_RETENTION_DAYS` wins over this key.
    #[serde(default = "default_retention_days")]
    pub retention_days: u64,
    /// Secrets-store mode: `file` (default; values persist in this file,
    /// mode 0600) or `memory` (values exist only in engine memory for the
    /// current process lifetime — nothing is ever written to the config
    /// file; re-seed after restart, e.g. via a systemd credential).
    /// `PRELOOP_SECRETS_STORE` overrides this key.
    #[serde(default)]
    pub secrets_store: Option<String>,
    /// Credential backend for the engine token and the GitHub App, PAT, and
    /// webhook secrets. For config-backed GitHub credentials, `file` (the
    /// default) stores `0600` files under the config file's parent
    /// `credentials` directory; the engine token remains scoped to its engine
    /// home. `os` uses the native keychain or secret-service.
    /// `PRELOOP_CREDENTIAL_STORE` overrides this key.
    #[serde(default)]
    pub credential_store: Option<String>,
    /// Maximum jobs one run may expand to (after matrix expansion), a
    /// fail-fast DoS guard for the submit path: a run expanding past this
    /// is rejected with 400 before the insert transaction opens, so no
    /// giant transaction ever pins the writer. Default 500; `0` disables
    /// the cap (the operator accepts unbounded submits). The insert itself
    /// is chunked (see `SUBMIT_JOB_CHUNK_SIZE`), so even a raised cap never
    /// holds one writer transaction for the whole insert.
    /// Env: `PRELOOP_MAX_JOBS_PER_RUN` wins over this key.
    #[serde(default = "default_max_jobs_per_run")]
    pub max_jobs_per_run: u64,
    /// Maximum age, in hours, of a diagnostic-log staging directory under
    /// `<state_dir>/blobs/diag/` before the reaper deletes it. Staging
    /// directories whose upload token expired (TTL) or was evicted are
    /// removed promptly on the next reaper tick regardless of this value;
    /// this is the backstop for anything orphaned (a crash between staging
    /// and registration, pre-fix leftovers). `0` disables the age-based
    /// sweep. Default 24 follows the "a day of debug data" intuition.
    /// Env: `PRELOOP_DIAG_STAGING_MAX_AGE_HOURS` wins over this key.
    #[serde(default = "default_diag_staging_max_age_hours")]
    pub diag_staging_max_age_hours: u64,
}

impl Default for ConfigFile {
    fn default() -> Self {
        // Every field carries a serde default, so parsing an empty document
        // is the canonical default config. This keeps `Default` in sync
        // with the documented `#[serde(default = ...)]` values (notably the
        // 90-day retention) instead of Rust's zero default, which would
        // silently disable the retention sweep for a missing config file.
        toml::from_str("").expect("empty TOML parses; every field has a serde default")
    }
}

/// Env override for the secrets-store mode; see [`ConfigFile::secrets_store`].
pub const SECRETS_STORE_ENV: &str = "PRELOOP_SECRETS_STORE";
/// Serves the App-manifest registration pages (`/api/v1/github/register`,
/// `/api/v1/github/callback`); off unless `1`, `true`, or `on`.
pub const APP_REGISTRATION_ENV: &str = "PRELOOP_ENABLE_APP_REGISTRATION";
pub const CHECKOUT_CACHE_MODE_ENV: &str = "PRELOOP_CHECKOUT_CACHE_MODE";
pub const CHECKOUT_CACHE_RUN_RETENTION_ENV: &str = "PRELOOP_CHECKOUT_CACHE_RUN_RETENTION_SECONDS";
pub const CHECKOUT_CACHE_REPOSITORY_RETENTION_ENV: &str =
    "PRELOOP_CHECKOUT_CACHE_REPOSITORY_RETENTION_SECONDS";
pub const CHECKOUT_CACHE_MAX_BYTES_ENV: &str = "PRELOOP_CHECKOUT_CACHE_MAX_BYTES";
/// Env override for run/check/status retention; see [`ConfigFile::retention_days`].
pub const RETENTION_DAYS_ENV: &str = "PRELOOP_RETENTION_DAYS";
/// Default run retention, in days, following GitHub's Actions retention setting.
pub const DEFAULT_RETENTION_DAYS: u64 = 90;
/// Env override for the per-run job cap; see [`ConfigFile::max_jobs_per_run`].
pub const MAX_JOBS_PER_RUN_ENV: &str = "PRELOOP_MAX_JOBS_PER_RUN";
/// Default cap on jobs per run (after matrix expansion). Bounds the submit
/// transaction so one giant workflow cannot pin the writer for minutes.
pub const DEFAULT_MAX_JOBS_PER_RUN: u64 = 500;

/// Env override for the in-place re-run window (see
/// [`rerun_window_days`]): how long a completed run with failed jobs stays
/// in the live control tables so it can be re-run in place.
pub const RERUN_WINDOW_DAYS_ENV: &str = "PRELOOP_RERUN_WINDOW_DAYS";
/// Default in-place re-run window, in days, following GitHub's 30-day
/// re-run limit.
pub const DEFAULT_RERUN_WINDOW_DAYS: u64 = 30;

/// Resolve the archiver's in-place re-run hold from `env` (the resolved
/// `PRELOOP_RERUN_WINDOW_DAYS` value; default [`DEFAULT_RERUN_WINDOW_DAYS`]),
/// in days. A run whose completed
/// attempt has at least one failed/cancelled/timed-out job stays live this
/// long so a re-run can reset its jobs in place; `0` disables the hold and
/// restores the plain 60-second archive policy. An unparseable value is an
/// error — a typo must fail closed rather than hold (or drop) scheduler rows
/// silently; callers warn and disable the hold.
pub fn rerun_window_days(env: Option<&str>) -> anyhow::Result<u64> {
    if let Some(raw) = env.filter(|value| !value.trim().is_empty()) {
        return raw.trim().parse().with_context(|| {
            format!("invalid unsigned integer in {RERUN_WINDOW_DAYS_ENV} (`{raw}`)")
        });
    }
    Ok(DEFAULT_RERUN_WINDOW_DAYS)
}

fn default_retention_days() -> u64 {
    DEFAULT_RETENTION_DAYS
}

fn default_max_jobs_per_run() -> u64 {
    DEFAULT_MAX_JOBS_PER_RUN
}

/// Resolve the effective per-run job cap: `env` (the resolved
/// `PRELOOP_MAX_JOBS_PER_RUN` value) wins over the config file key. An
/// unparseable value is an error — a typo must fail closed rather than
/// silently run uncapped (or capped at zero).
pub fn max_jobs_per_run(config: &ConfigFile, env: Option<&str>) -> anyhow::Result<u64> {
    if let Some(raw) = env.filter(|value| !value.trim().is_empty()) {
        return raw.trim().parse().with_context(|| {
            format!("invalid unsigned integer in {MAX_JOBS_PER_RUN_ENV} (`{raw}`)")
        });
    }
    Ok(config.max_jobs_per_run)
}

/// Resolve the effective run retention: `env` (the resolved
/// `PRELOOP_RETENTION_DAYS` value) wins over the config file key. An
/// unparseable value is an error — a typo must fail closed rather than
/// silently keep (or drop) years of run history.
pub fn retention_days(config: &ConfigFile, env: Option<&str>) -> anyhow::Result<u64> {
    if let Some(raw) = env.filter(|value| !value.trim().is_empty()) {
        return raw.trim().parse().with_context(|| {
            format!("invalid unsigned integer in {RETENTION_DAYS_ENV} (`{raw}`)")
        });
    }
    Ok(config.retention_days)
}

/// Env override for the diag staging max age; see
/// [`ConfigFile::diag_staging_max_age_hours`].
pub const DIAG_STAGING_MAX_AGE_HOURS_ENV: &str = "PRELOOP_DIAG_STAGING_MAX_AGE_HOURS";
/// Default diag staging max age, in hours.
pub const DEFAULT_DIAG_STAGING_MAX_AGE_HOURS: u64 = 24;

fn default_diag_staging_max_age_hours() -> u64 {
    DEFAULT_DIAG_STAGING_MAX_AGE_HOURS
}

/// Resolve the effective diag staging max age, in hours: `env` (the resolved
/// `PRELOOP_DIAG_STAGING_MAX_AGE_HOURS` value) wins over the config file key.
/// An unparseable value is an error — a typo must fail closed rather than
/// silently keep (or delete) staging data on the wrong schedule.
pub fn diag_staging_max_age_hours(config: &ConfigFile, env: Option<&str>) -> anyhow::Result<u64> {
    if let Some(raw) = env.filter(|value| !value.trim().is_empty()) {
        return raw.trim().parse().with_context(|| {
            format!("invalid unsigned integer in {DIAG_STAGING_MAX_AGE_HOURS_ENV} (`{raw}`)")
        });
    }
    Ok(config.diag_staging_max_age_hours)
}

/// Parse a checkout-cache mode. Unknown values are an error from the parser;
/// every caller warns and falls back to `off`, so a typo never silently
/// turns source retention on and never blocks startup either.
pub fn parse_checkout_cache_mode(raw: &str) -> anyhow::Result<CheckoutCacheMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => Ok(CheckoutCacheMode::Off),
        "run-scoped" => Ok(CheckoutCacheMode::RunScoped),
        "repository" => Ok(CheckoutCacheMode::Repository),
        other => anyhow::bail!(
            "invalid checkout cache mode `{other}` (expected off, run-scoped, or repository)"
        ),
    }
}

/// Explicit env-layer inputs for [`checkout_cache_config`]: the resolved
/// values of the `PRELOOP_CHECKOUT_CACHE_*` variables, `None` when unset.
#[derive(Debug, Clone, Default)]
pub struct CheckoutCacheEnv {
    /// Resolved [`CHECKOUT_CACHE_MODE_ENV`] value.
    pub mode: Option<String>,
    /// Resolved [`CHECKOUT_CACHE_RUN_RETENTION_ENV`] value.
    pub run_retention_seconds: Option<String>,
    /// Resolved [`CHECKOUT_CACHE_REPOSITORY_RETENTION_ENV`] value.
    pub repository_retention_seconds: Option<String>,
    /// Resolved [`CHECKOUT_CACHE_MAX_BYTES_ENV`] value.
    pub max_bytes: Option<String>,
}

/// Resolve checkout-cache configuration with the explicit `env` inputs taking
/// precedence over the config file. An unknown mode warns and falls back to
/// `off`: off retains nothing, so a typo costs caching, never startup.
pub fn checkout_cache_config(
    config: &ConfigFile,
    env: &CheckoutCacheEnv,
) -> anyhow::Result<CheckoutCacheConfig> {
    let mut effective = config.checkout_cache.clone();
    if let Some(raw) = env.mode.as_deref().filter(|value| !value.trim().is_empty()) {
        effective.mode = match parse_checkout_cache_mode(raw) {
            Ok(mode) => mode,
            Err(_) => {
                tracing::warn!(
                    "{CHECKOUT_CACHE_MODE_ENV} has unknown mode `{raw}`; falling back to `off`"
                );
                CheckoutCacheMode::Off
            }
        };
    }
    for (name, value, target) in [
        (
            CHECKOUT_CACHE_RUN_RETENTION_ENV,
            env.run_retention_seconds.as_deref(),
            &mut effective.run_retention_seconds,
        ),
        (
            CHECKOUT_CACHE_REPOSITORY_RETENTION_ENV,
            env.repository_retention_seconds.as_deref(),
            &mut effective.repository_retention_seconds,
        ),
        (
            CHECKOUT_CACHE_MAX_BYTES_ENV,
            env.max_bytes.as_deref(),
            &mut effective.max_bytes,
        ),
    ] {
        if let Some(raw) = value.filter(|value| !value.trim().is_empty()) {
            *target = raw
                .trim()
                .parse()
                .with_context(|| format!("invalid unsigned integer in {name}"))?;
        }
    }
    Ok(effective)
}

pub const CACHE_TOTAL_QUOTA_BYTES_ENV: &str = "PRELOOP_CACHE_TOTAL_QUOTA_BYTES";

/// Resolve the Actions-cache quota: `env` (the resolved
/// `PRELOOP_CACHE_TOTAL_QUOTA_BYTES` value) wins over the
/// `[cache] total_quota_bytes` key; an unparseable value is an error rather
/// than silently keeping or dropping cached entries.
pub fn cache_config(config: &ConfigFile, env: Option<&str>) -> anyhow::Result<CacheConfig> {
    let mut effective = config.cache;
    if let Some(raw) = env.filter(|value| !value.trim().is_empty()) {
        effective.total_quota_bytes = raw.trim().parse().with_context(|| {
            format!("invalid unsigned integer in {CACHE_TOTAL_QUOTA_BYTES_ENV} (`{raw}`)")
        })?;
    }
    Ok(effective)
}

#[cfg(test)]
mod cache_quota_config_tests {
    use super::*;

    /// Absent section keeps the 10 GiB default.
    #[test]
    fn cache_quota_defaults_to_10gib() {
        let config: ConfigFile = toml::from_str("").unwrap();
        assert_eq!(config.cache.total_quota_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(
            config.cache.total_quota_bytes,
            DEFAULT_CACHE_TOTAL_QUOTA_BYTES
        );
        assert_eq!(
            cache_config(&config, None).unwrap().total_quota_bytes,
            DEFAULT_CACHE_TOTAL_QUOTA_BYTES
        );
    }

    #[test]
    fn cache_quota_file_value_honored() {
        let config: ConfigFile = toml::from_str("[cache]\ntotal_quota_bytes = 1073741824").unwrap();
        assert_eq!(config.cache.total_quota_bytes, 1024 * 1024 * 1024);
    }

    /// The env layer wins over the file; a blank value counts as unset; an
    /// unparseable value is an error. Inputs are explicit — no process
    /// environment is involved.
    #[test]
    fn cache_quota_env_override_handling() {
        let config: ConfigFile = toml::from_str("[cache]\ntotal_quota_bytes = 1073741824").unwrap();
        assert_eq!(
            cache_config(&config, None).unwrap().total_quota_bytes,
            1073741824
        );
        assert_eq!(
            cache_config(&config, Some("5368709120"))
                .unwrap()
                .total_quota_bytes,
            5368709120
        );
        assert_eq!(
            cache_config(&config, Some("   "))
                .unwrap()
                .total_quota_bytes,
            1073741824
        );
        assert!(cache_config(&config, Some("ten-gib")).is_err());
    }
}

#[cfg(test)]
mod checkout_cache_tests {
    use super::*;

    /// Retaining repository source is opt-in: an absent section keeps every
    /// job checking out directly from its forge.
    #[test]
    fn checkout_cache_defaults_to_off() {
        let config: ConfigFile = toml::from_str("").unwrap();
        assert_eq!(config.checkout_cache.mode, CheckoutCacheMode::Off);
    }

    #[test]
    fn checkout_cache_modes_parse_and_typos_fall_back_to_off() {
        let config: ConfigFile = toml::from_str(
            r#"
[checkout_cache]
mode = "run-scoped"
run_retention_seconds = 60
"#,
        )
        .unwrap();
        assert_eq!(config.checkout_cache.mode, CheckoutCacheMode::RunScoped);
        assert_eq!(config.checkout_cache.run_retention_seconds, 60);
        // Unset keys keep their defaults rather than collapsing to zero.
        assert_eq!(
            config.checkout_cache.repository_retention_seconds,
            CheckoutCacheConfig::default().repository_retention_seconds
        );

        assert_eq!(
            parse_checkout_cache_mode("repository").unwrap(),
            CheckoutCacheMode::Repository
        );
        assert!(parse_checkout_cache_mode("run scoped").is_err());
        assert!(parse_checkout_cache_mode("on").is_err());
        // A typo in the file warns and keeps retention off instead of
        // refusing to boot.
        let typo: ConfigFile = toml::from_str("[checkout_cache]\nmode = \"everything\"").unwrap();
        assert_eq!(typo.checkout_cache.mode, CheckoutCacheMode::Off);
    }

    /// An unknown mode from the env layer warns and keeps retention off
    /// instead of refusing to boot. Input is explicit — no process
    /// environment is involved.
    #[test]
    fn checkout_cache_unknown_override_mode_falls_back_to_off() {
        let env = CheckoutCacheEnv {
            mode: Some("everything".to_owned()),
            ..Default::default()
        };
        let effective =
            checkout_cache_config(&ConfigFile::default(), &env).expect("fallback is not an error");
        assert_eq!(effective.mode, CheckoutCacheMode::Off);
    }
}

#[cfg(test)]
mod retention_config_tests {
    use super::*;

    /// Absent key keeps the GitHub-matching default of 90 days.
    #[test]
    fn retention_days_defaults_to_90() {
        let config: ConfigFile = toml::from_str("").unwrap();
        assert_eq!(config.retention_days, 90);
        assert_eq!(config.retention_days, DEFAULT_RETENTION_DAYS);
    }

    #[test]
    fn retention_days_custom_value_honored() {
        // Field-level only: the resolver's env handling lives in
        // `retention_days_env_handling`, the one test that mutates the var.
        let config: ConfigFile = toml::from_str("retention_days = 30").unwrap();
        assert_eq!(config.retention_days, 30);
        // 0 is the documented escape hatch: the sweep stays off.
        let disabled: ConfigFile = toml::from_str("retention_days = 0").unwrap();
        assert_eq!(disabled.retention_days, 0);
    }

    /// The env layer wins over the file; a blank value counts as unset; an
    /// unparseable value is an error rather than silently keeping or
    /// dropping run history; with no override the file value is used.
    /// Inputs are explicit — no process environment is involved.
    #[test]
    fn retention_days_env_override_handling() {
        let config: ConfigFile = toml::from_str("retention_days = 30").unwrap();
        assert_eq!(retention_days(&config, None).unwrap(), 30);
        assert_eq!(retention_days(&config, Some("7")).unwrap(), 7);
        assert_eq!(retention_days(&config, Some("   ")).unwrap(), 30);
        assert!(retention_days(&config, Some("ninety")).is_err());
    }

    /// A missing config file still gets the GitHub-matching default.
    #[test]
    fn retention_days_default_impl_is_90() {
        assert_eq!(ConfigFile::default().retention_days, 90);
    }
}

#[cfg(test)]
mod max_jobs_per_run_config_tests {
    use super::*;

    /// Absent key keeps the default cap of 500.
    #[test]
    fn max_jobs_per_run_defaults_to_500() {
        let config: ConfigFile = toml::from_str("").unwrap();
        assert_eq!(config.max_jobs_per_run, 500);
        assert_eq!(config.max_jobs_per_run, DEFAULT_MAX_JOBS_PER_RUN);
    }

    #[test]
    fn max_jobs_per_run_custom_value_honored() {
        // Field-level only: the resolver's env handling lives in
        // `max_jobs_per_run_env_handling`, the one test that mutates the var.
        let config: ConfigFile = toml::from_str("max_jobs_per_run = 1000").unwrap();
        assert_eq!(config.max_jobs_per_run, 1000);
        // 0 is the documented escape hatch: the cap stays off.
        let disabled: ConfigFile = toml::from_str("max_jobs_per_run = 0").unwrap();
        assert_eq!(disabled.max_jobs_per_run, 0);
    }

    /// The env layer wins over the file; a blank value counts as unset; an
    /// unparseable value is an error rather than silently running uncapped;
    /// with no override the file value is used. Inputs are explicit — no
    /// process environment is involved.
    #[test]
    fn max_jobs_per_run_env_override_handling() {
        let config: ConfigFile = toml::from_str("max_jobs_per_run = 1000").unwrap();
        assert_eq!(max_jobs_per_run(&config, None).unwrap(), 1000);
        assert_eq!(max_jobs_per_run(&config, Some("250")).unwrap(), 250);
        assert_eq!(max_jobs_per_run(&config, Some("   ")).unwrap(), 1000);
        assert!(max_jobs_per_run(&config, Some("many")).is_err());
    }

    /// A missing config file still gets the default cap.
    #[test]
    fn max_jobs_per_run_default_impl_is_500() {
        assert_eq!(ConfigFile::default().max_jobs_per_run, 500);
    }
}

/// Systemd sets this when the unit mounts any `LoadCredential=`.
pub const CREDENTIALS_ENV: &str = "CREDENTIALS_DIRECTORY";
/// Credential name `preloop server install --systemd-credential` mounts.
pub const CREDENTIAL_NAME: &str = "preloop-secrets";

/// Secrets-store mode: `file` (default) or `memory`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretsStoreMode {
    File,
    Memory,
}

/// Resolve the secrets-store mode: `env` (the resolved `PRELOOP_SECRETS_STORE`
/// value) wins over the config file key. Unknown values are an error — a
/// typo must fail closed (never silently fall back to writing plaintext
/// secrets to the file).
pub fn secrets_store_mode(
    config: &ConfigFile,
    env: Option<&str>,
) -> anyhow::Result<SecretsStoreMode> {
    let raw = env
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| config.secrets_store.clone());
    match raw.as_deref().map(str::trim) {
        None | Some("file") => Ok(SecretsStoreMode::File),
        Some("memory") => Ok(SecretsStoreMode::Memory),
        Some(other) => anyhow::bail!(
            "invalid secrets store mode `{other}` (expected `file` or `memory`, \
             from {SECRETS_STORE_ENV} or `secrets_store` in the config file)"
        ),
    }
}

/// True when stored secrets must stay out of the config file: values live in
/// engine memory only, mutated live through the secrets API and lost on
/// restart. `env` (`PRELOOP_SECRETS_STORE`) wins over the config file key.
pub fn store_memory(config: &ConfigFile, env: Option<&str>) -> anyhow::Result<bool> {
    Ok(secrets_store_mode(config, env)? == SecretsStoreMode::Memory)
}

/// Load the systemd credential mounted at
/// `<credentials_dir>/preloop-secrets`, if any: a TOML fragment with
/// the same `[secrets]` / `[repo_secrets."owner/repo"]` schema as the config
/// file. Missing directory or file yields an empty config; a malformed
/// credential is an error — the operator mounted it explicitly, so a typo
/// must not be silently ignored at startup. `credentials_dir` is the resolved
/// [`CREDENTIALS_ENV`] value.
pub fn load_credential_secrets(credentials_dir: Option<&Path>) -> anyhow::Result<ConfigFile> {
    let Some(dir) = credentials_dir else {
        return Ok(ConfigFile::default());
    };
    load_credential_from(&dir.join(CREDENTIAL_NAME))
}

fn load_credential_from(path: &Path) -> anyhow::Result<ConfigFile> {
    // `exists()` swallows permission errors as "absent", which would let a
    // credential the service cannot read silently no-op — startup would
    // proceed with stale or plaintext secrets. Fail closed: only a
    // genuinely missing credential is treated as empty.
    if !path
        .try_exists()
        .with_context(|| format!("checking credential {}", path.display()))?
    {
        return Ok(ConfigFile::default());
    }
    load_config_from(path)
}

/// Overlay one config's stored secrets onto another, per name; the overlay
/// wins. Used for systemd credentials, which take precedence over the config
/// file's `[secrets]`.
pub fn merge_secret_stores(config: &mut ConfigFile, overlay: ConfigFile) {
    for (name, value) in overlay.secrets {
        config.secrets.insert(name, value);
    }
    for (repo, names) in overlay.repo_secrets {
        config.repo_secrets.entry(repo).or_default().extend(names);
    }
    for (repo, envs) in overlay.env_secrets {
        for (env, names) in envs {
            config
                .env_secrets
                .entry(repo.clone())
                .or_default()
                .entry(env)
                .or_default()
                .extend(names);
        }
    }
}

/// Manual `Debug`: secret values are never printed, only how many exist.
impl std::fmt::Debug for ConfigFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConfigFile {{ github: {:?}, golden: {:?}, secrets: {} names, repo_secrets: {} repos, env_secrets: {} repos, environments: {} repos, environment_rules: {} repos, token_permissions_ceiling: {}, fork_policy: {:?}, execution_protection: {:?} ({} event rules, {} actor rules) }}",
            self.github,
            self.golden,
            self.secrets.len(),
            self.repo_secrets.len(),
            self.env_secrets.len(),
            self.environments.len(),
            self.environment_rules.len(),
            self.token_permissions_ceiling
                .as_ref()
                .map_or("none".to_owned(), |ceiling| format!(
                    "default={} ({} scoped)",
                    ceiling.r#default.as_str(),
                    ceiling.scopes.len()
                )),
            self.fork_policy,
            self.execution_protection.mode,
            self.execution_protection.event_rules.len(),
            self.execution_protection.actor_rules.len()
        )
    }
}

/// Resolve the config file path: `PRELOOP_CONFIG` when set, else
/// `$HOME/.preloop/config.toml`. Startup-boundary helper — resolve once
/// (into [`StartupConfig::config_path`]) and pass the result down; nothing
/// below the startup boundary should call this.
pub fn config_path() -> PathBuf {
    if let Some(path) = std::env::var_os(CONFIG_PATH_ENV) {
        return PathBuf::from(path);
    }
    // Tests must never resolve (or worse: write) the developer's real
    // config through the default path: the default is a per-process pinned
    // temp file instead — the same path for the whole process, so tests that
    // share config through the file still agree.
    #[cfg(any(test, feature = "test-support"))]
    {
        pinned_test_config_path()
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        let home = std::env::var_os("HOME").unwrap_or_default();
        PathBuf::from(home).join(".preloop").join("config.toml")
    }
}

/// Load the config file. A missing or empty file yields the default config;
/// a malformed file is an error so a typo is caught at startup, not when a
/// mint first fails.
pub fn load_config() -> anyhow::Result<ConfigFile> {
    // Serialize config I/O in test builds: the pinned config path is shared
    // by the whole test process, so a concurrent write must not tear a read.
    #[cfg(any(test, feature = "test-support"))]
    let _config_guard = CONFIG_PATH_LOCK.lock();
    load_config_from(&config_path())
}

/// Typed startup configuration for one engine process.
///
/// This is the process-startup boundary: production resolves it once from
/// the inherited environment ([`StartupConfig::from_env`]) plus explicit
/// entry-point values (a CLI flag overrides the field after `from_env`),
/// then hands it to [`crate::AppState::new`]. Nothing below the boundary
/// re-reads the process environment, so one engine is bound to one
/// configuration for its whole life and tests inject values explicitly
/// without touching global state.
///
/// Layering for every setting, preserved from the env-driven era:
/// **explicit field > inherited env > config file key > built-in default**.
/// A blank override value counts as unset everywhere it did before.
#[derive(Clone)]
pub struct StartupConfig {
    // ---- paths ----
    /// Engine config file (see [`config_path`]).
    pub config_path: PathBuf,
    /// Durable store URL (`sqlite://…`, bare path, `postgres://…`).
    /// `None` → SQLite at `<state_dir>/preloop.db`.
    /// `from_env()`: `PRELOOP_STORE_URL`.
    pub store_url: Option<String>,
    /// Engine home for token/credential storage. `None` → `state_dir`.
    /// `from_env()`: `PRELOOP_HOME`.
    pub engine_home: Option<PathBuf>,
    /// systemd `LoadCredential=` mount directory.
    /// `from_env()`: [`CREDENTIALS_ENV`].
    pub credentials_dir: Option<PathBuf>,
    /// Local workflow workspace. `from_env()`: `PRELOOP_LOCAL_WORKSPACE`.
    pub local_workspace: Option<PathBuf>,

    // ---- tokens / credentials (redacted in `Debug`) ----
    /// Native API administrator token. `from_env()`: `PRELOOP_SYSTEM_TOKEN`.
    pub system_token: Option<String>,
    /// Hex-encoded 32-byte runner/job JWT key. Takes precedence over
    /// `hmac_key`; `None` for both loads-or-generates the key file under
    /// `state_dir`. `from_env()` fills `hmac_key` from `PRELOOP_HMAC_KEY`.
    pub local_jwt_key: Option<Vec<u8>>,
    /// Hex-encoded 32-byte runner/job JWT key (`from_env()`:
    /// `PRELOOP_HMAC_KEY`). See `local_jwt_key`.
    pub hmac_key: Option<String>,
    /// GitHub PAT; overrides the config file's `github.pat`.
    /// `from_env()`: `PRELOOP_GITHUB_TOKEN`.
    pub github_token: Option<SecretString>,
    /// GitHub App webhook secret; overrides `github.webhook_secret`.
    /// `from_env()`: `PRELOOP_WEBHOOK_SECRET`.
    pub webhook_secret: Option<String>,
    /// GitHub App id; overrides `github.app_id`.
    /// `from_env()`: `PRELOOP_GITHUB_APP_ID`.
    pub github_app_id: Option<String>,
    /// GitHub App installation id (`PRELOOP_GITHUB_APP_INSTALLATION_ID`).
    pub github_installation_id: Option<String>,
    /// GitHub App private key: `(source label, value-is-path, value)`, same
    /// shape and precedence order as the legacy
    /// `PRELOOP_GITHUB_APP_PEM{,_FILE}` / `PRELOOP_GITHUB_APP_PRIVATE_KEY{,_PATH}`
    /// variables, then the config file's `github.app_pem`.
    pub github_app_pem: Vec<(String, bool, String)>,
    /// Extra GitHub Apps JSON; overrides `github.apps`.
    /// `from_env()`: `PRELOOP_GITHUB_APPS_JSON`.
    pub github_apps_json: Option<String>,
    /// Mint-failure policy (`local|error|pat`); overrides
    /// `github.mint_failure`. `from_env()`: `PRELOOP_GITHUB_APP_MINT_FAILURE`.
    pub github_mint_failure: Option<String>,

    // ---- URLs (normalized: trailing `/` trimmed when stored on AppState) ----
    /// `from_env()`: `PRELOOP_GITHUB_SERVER_URL`; default `https://github.com`.
    pub github_server_url: Option<String>,
    /// `from_env()`: `PRELOOP_GITHUB_API_URL`; default `https://api.github.com`.
    pub github_api_url: Option<String>,
    /// `from_env()`: `PRELOOP_GITHUB_GRAPHQL_URL`;
    /// default `https://api.github.com/graphql`.
    pub github_graphql_url: Option<String>,
    /// Public (GitHub-facing) base URL. `from_env()`: `PRELOOP_PUBLIC_URL`;
    /// default `http://127.0.0.1:9090`.
    pub public_url: Option<String>,
    /// Runner-facing base URL. `None` → the public URL.
    /// `from_env()`: `PRELOOP_RUNNER_URL`.
    pub runner_url: Option<String>,

    // ---- config-file-key overrides (raw values; the pure resolvers keep the
    //      fail-closed / warn-and-keep parse semantics) ----
    /// `PRELOOP_RUNNER_BASE_IMAGE` over `[golden] base_image`.
    pub base_image: Option<String>,
    /// `PRELOOP_SECRETS_STORE` over `secrets_store`.
    pub secrets_store: Option<String>,
    /// `PRELOOP_CACHE_TOTAL_QUOTA_BYTES` over `[cache] total_quota_bytes`.
    pub cache_total_quota_bytes: Option<String>,
    /// `PRELOOP_CHECKOUT_CACHE_MODE` over `[checkout_cache] mode`.
    pub checkout_cache_mode: Option<String>,
    /// `PRELOOP_CHECKOUT_CACHE_RUN_RETENTION_SECONDS`.
    pub checkout_cache_run_retention_seconds: Option<String>,
    /// `PRELOOP_CHECKOUT_CACHE_REPOSITORY_RETENTION_SECONDS`.
    pub checkout_cache_repository_retention_seconds: Option<String>,
    /// `PRELOOP_CHECKOUT_CACHE_MAX_BYTES`.
    pub checkout_cache_max_bytes: Option<String>,
    /// `PRELOOP_RETENTION_DAYS` over `retention_days`.
    pub retention_days: Option<String>,
    /// `PRELOOP_MAX_JOBS_PER_RUN` over `max_jobs_per_run`.
    pub max_jobs_per_run: Option<String>,
    /// `PRELOOP_RERUN_WINDOW_DAYS`.
    pub rerun_window_days: Option<String>,
    /// `PRELOOP_DIAG_STAGING_MAX_AGE_HOURS` over `diag_staging_max_age_hours`.
    pub diag_staging_max_age_hours: Option<String>,
    /// `PRELOOP_CREDENTIAL_STORE` over the config file's `credential_store`.
    pub credential_store: Option<String>,

    // ---- engine knobs ----
    /// `PRELOOP_RUNNER_LIVENESS_TIMEOUT_SECS` (default 1800, floored at
    /// `3 *` the max poll window).
    pub runner_liveness_timeout_secs: Option<u64>,
    /// `PRELOOP_MAX_POLL_WINDOW_SECS` (default 60; values `0`/unparseable
    /// fall back to the default).
    pub max_poll_window_secs: Option<u64>,
    /// `PRELOOP_SNAPSHOT_RETENTION_SECONDS` (default 1800).
    pub snapshot_retention_seconds: Option<u64>,
    /// `PRELOOP_RUNNER_VERSION_DEPRECATED` (`1|true|yes`).
    pub runner_version_deprecated: Option<bool>,
    /// `PRELOOP_REGISTRATION_POLICY` (`permissive` opts in; default strict).
    pub registration_policy: Option<String>,
    /// `PRELOOP_WEBHOOK_WORKERS` (default 8).
    pub webhook_workers: Option<usize>,
    /// `PRELOOP_WEBHOOK_WATCHDOG` (`off|0|false|disabled` disables; default
    /// enabled).
    pub webhook_watchdog_enabled: Option<bool>,
    /// `PRELOOP_WEBHOOK_WATCHDOG_INTERVAL_SECS` (default 300).
    pub webhook_watchdog_interval_secs: Option<u64>,
    /// `PRELOOP_WEBHOOK_WATCHDOG_GRACE_SECS`.
    pub webhook_watchdog_grace_secs: Option<u64>,
    /// `PRELOOP_WEBHOOK_WATCHDOG_MAX_ATTEMPTS` (default 5).
    pub webhook_watchdog_max_attempts: Option<u32>,
    /// `PRELOOP_WEBHOOK_WATCHDOG_MAX_PAGES` (default 5).
    pub webhook_watchdog_max_pages: Option<usize>,
    /// `PRELOOP_TOKEN_TTL_SECS` (default 2999).
    pub token_ttl_secs: Option<u64>,
    /// `PRELOOP_OUTBOX_RETENTION_SECONDS` (default 3600).
    pub outbox_retention_seconds: Option<u64>,
    /// `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` over the manifest default
    /// (`push,pull_request,check_run`).
    pub github_app_default_events: Option<String>,
    /// `PRELOOP_ENABLE_APP_REGISTRATION` (`1`, `true`, or `on`, case
    /// insensitive): serve the App-manifest pages at
    /// `/api/v1/github/register` and `/api/v1/github/callback`. Off by
    /// default — both routes answer `404`.
    pub github_app_registration: Option<bool>,
    /// `PRELOOP_GITHUB_SKIP_WORKFLOWS`: workflow filenames the engine never
    /// runs (comma-separated).
    pub github_skip_workflows: Option<String>,
    /// `PRELOOP_GITHUB_PR_AUTO` (`feature|never`) over `[github.pr] auto`.
    pub github_pr_auto: Option<String>,
    /// `PRELOOP_GITHUB_PR_DRAFT` (`1|true|yes|0|false|no`) over
    /// `[github.pr] draft`.
    pub github_pr_draft: Option<String>,
    /// `PRELOOP_GITHUB_PR_EXCLUDE` (comma-separated patterns) over
    /// `[github.pr] exclude`.
    pub github_pr_exclude: Option<String>,
    /// `PRELOOP_GITHUB_BREAKER` (`off|0|false|disabled` disables tripping).
    pub github_breaker_enabled: Option<bool>,
    /// `PRELOOP_GITHUB_BREAKER_THRESHOLD` (u32 > 0).
    pub github_breaker_threshold: Option<u32>,
}

/// Blank values count as unset, matching every historical override filter.
fn env_override(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// URL environment overrides use the same blank-as-unset rule as other
/// startup values, while removing delimiters callers append paths to.
fn env_url_override(name: &str) -> Option<String> {
    env_override(name)
        .map(|value| value.trim_end_matches('/').to_owned())
        .filter(|value| !value.is_empty())
}

/// `1|true|yes` (case-insensitive) parse used by the boolean overrides.
fn env_flag_value(name: &str) -> Option<bool> {
    std::env::var(name).ok().map(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    })
}

/// `1|true|on` (case-insensitive) parse: the documented spelling of the
/// switches whose contract is "off unless on" rather than
/// [`env_flag_value`]'s `1|true|yes`. Present-but-other counts as off.
fn parse_switch(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "on"
    )
}

/// [`parse_switch`] against the process environment: `None` when the variable
/// is unset, `Some(on)` when it is set.
fn env_switch_value(name: &str) -> Option<bool> {
    std::env::var(name).ok().map(|value| parse_switch(&value))
}

impl StartupConfig {
    /// The production startup boundary: read the inherited process
    /// environment exactly once. Values already present on `self` (explicit
    /// CLI values) win; unset fields fall back to the environment here, then
    /// to the config file / defaults applied by [`crate::AppState::new`].
    ///
    /// Build explicit-over-env with
    /// `StartupConfig { public_url: Some(url), ..StartupConfig::from_env() }`.
    pub fn from_env() -> Self {
        Self {
            config_path: config_path(),
            store_url: env_override(crate::store::STORE_URL_ENV),
            engine_home: std::env::var_os("PRELOOP_HOME")
                .filter(|path| !path.is_empty())
                .map(PathBuf::from),
            credentials_dir: std::env::var_os(CREDENTIALS_ENV).map(PathBuf::from),
            local_workspace: std::env::var_os("PRELOOP_LOCAL_WORKSPACE").map(PathBuf::from),

            system_token: std::env::var("PRELOOP_SYSTEM_TOKEN").ok(),
            local_jwt_key: None,
            hmac_key: env_override(crate::state::HMAC_KEY_ENV),
            github_token: std::env::var("PRELOOP_GITHUB_TOKEN")
                .ok()
                .filter(|value| !value.is_empty())
                .map(SecretString::new),
            webhook_secret: std::env::var("PRELOOP_WEBHOOK_SECRET")
                .ok()
                .filter(|value| !value.is_empty()),
            github_app_id: env_override("PRELOOP_GITHUB_APP_ID"),
            github_installation_id: env_override("PRELOOP_GITHUB_APP_INSTALLATION_ID"),
            github_app_pem: crate::github_app::PRIVATE_KEY_ENV
                .into_iter()
                .filter_map(|(name, is_path)| {
                    env_override(name).map(|value| (name.to_owned(), is_path, value))
                })
                .collect(),
            github_apps_json: env_override("PRELOOP_GITHUB_APPS_JSON"),
            github_mint_failure: env_override("PRELOOP_GITHUB_APP_MINT_FAILURE"),

            github_server_url: env_override("PRELOOP_GITHUB_SERVER_URL"),
            github_api_url: env_url_override("PRELOOP_GITHUB_API_URL"),
            github_graphql_url: env_override("PRELOOP_GITHUB_GRAPHQL_URL"),
            public_url: env_override("PRELOOP_PUBLIC_URL"),
            runner_url: env_override("PRELOOP_RUNNER_URL"),

            base_image: env_override(BASE_IMAGE_ENV),
            secrets_store: env_override(SECRETS_STORE_ENV),
            cache_total_quota_bytes: env_override(CACHE_TOTAL_QUOTA_BYTES_ENV),
            checkout_cache_mode: env_override(CHECKOUT_CACHE_MODE_ENV),
            checkout_cache_run_retention_seconds: env_override(CHECKOUT_CACHE_RUN_RETENTION_ENV),
            checkout_cache_repository_retention_seconds: env_override(
                CHECKOUT_CACHE_REPOSITORY_RETENTION_ENV,
            ),
            checkout_cache_max_bytes: env_override(CHECKOUT_CACHE_MAX_BYTES_ENV),
            retention_days: env_override(RETENTION_DAYS_ENV),
            max_jobs_per_run: env_override(MAX_JOBS_PER_RUN_ENV),
            rerun_window_days: env_override(RERUN_WINDOW_DAYS_ENV),
            diag_staging_max_age_hours: env_override(DIAG_STAGING_MAX_AGE_HOURS_ENV),
            credential_store: env_override(crate::credential_store::CREDENTIAL_STORE_ENV),

            runner_liveness_timeout_secs: env_override("PRELOOP_RUNNER_LIVENESS_TIMEOUT_SECS")
                .and_then(|raw| raw.parse().ok()),
            max_poll_window_secs: env_override("PRELOOP_MAX_POLL_WINDOW_SECS")
                .and_then(|raw| raw.parse().ok())
                .filter(|value| *value > 0),
            snapshot_retention_seconds: env_override("PRELOOP_SNAPSHOT_RETENTION_SECONDS")
                .and_then(|raw| raw.parse().ok()),
            runner_version_deprecated: env_flag_value("PRELOOP_RUNNER_VERSION_DEPRECATED"),
            registration_policy: env_override("PRELOOP_REGISTRATION_POLICY"),
            webhook_workers: env_override("PRELOOP_WEBHOOK_WORKERS")
                .and_then(|raw| raw.parse().ok()),
            webhook_watchdog_enabled: env_override("PRELOOP_WEBHOOK_WATCHDOG").map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "off" | "0" | "false" | "disabled"
                )
            }),
            webhook_watchdog_interval_secs: env_override("PRELOOP_WEBHOOK_WATCHDOG_INTERVAL_SECS")
                .and_then(|raw| raw.parse().ok()),
            webhook_watchdog_grace_secs: env_override("PRELOOP_WEBHOOK_WATCHDOG_GRACE_SECS")
                .and_then(|raw| raw.parse().ok()),
            webhook_watchdog_max_attempts: env_override("PRELOOP_WEBHOOK_WATCHDOG_MAX_ATTEMPTS")
                .and_then(|raw| raw.parse().ok()),
            webhook_watchdog_max_pages: env_override("PRELOOP_WEBHOOK_WATCHDOG_MAX_PAGES")
                .and_then(|raw| raw.parse().ok()),
            token_ttl_secs: env_override("PRELOOP_TOKEN_TTL_SECS").and_then(|raw| raw.parse().ok()),
            outbox_retention_seconds: env_override("PRELOOP_OUTBOX_RETENTION_SECONDS")
                .and_then(|raw| raw.parse().ok()),
            github_app_default_events: env_override("PRELOOP_GITHUB_APP_DEFAULT_EVENTS"),
            github_app_registration: env_switch_value(APP_REGISTRATION_ENV),
            github_skip_workflows: env_override(crate::github::GITHUB_OWNED_WORKFLOWS_ENV),
            github_pr_auto: env_override("PRELOOP_GITHUB_PR_AUTO"),
            github_pr_draft: env_override("PRELOOP_GITHUB_PR_DRAFT"),
            github_pr_exclude: env_override("PRELOOP_GITHUB_PR_EXCLUDE"),
            github_breaker_enabled: env_override("PRELOOP_GITHUB_BREAKER").map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "off" | "0" | "false" | "disabled"
                )
            }),
            github_breaker_threshold: env_override("PRELOOP_GITHUB_BREAKER_THRESHOLD")
                .and_then(|raw| raw.parse::<u32>().ok())
                .filter(|threshold| *threshold > 0),
        }
    }

    /// Deterministic test configuration: the pinned per-process temp config
    /// path (never the developer's real one), the fixed test system token and
    /// JWT key, and no environment-derived values. Extend with
    /// `StartupConfig { github_api_url: Some(stub), ..StartupConfig::for_tests() }`.
    #[cfg(any(test, feature = "test-support"))]
    pub fn for_tests() -> Self {
        Self {
            config_path: pinned_test_config_path(),
            store_url: None,
            engine_home: None,
            credentials_dir: None,
            local_workspace: None,
            system_token: Some(crate::DEFAULT_PRELOOP_SYSTEM_TOKEN.to_owned()),
            local_jwt_key: Some(crate::TEST_LOCAL_JWT_KEY.to_vec()),
            hmac_key: None,
            github_token: None,
            webhook_secret: None,
            github_app_id: None,
            github_installation_id: None,
            github_app_pem: Vec::new(),
            github_apps_json: None,
            github_mint_failure: None,
            github_server_url: None,
            github_api_url: None,
            github_graphql_url: None,
            public_url: None,
            runner_url: None,
            base_image: None,
            secrets_store: None,
            cache_total_quota_bytes: None,
            checkout_cache_mode: None,
            checkout_cache_run_retention_seconds: None,
            checkout_cache_repository_retention_seconds: None,
            checkout_cache_max_bytes: None,
            retention_days: None,
            max_jobs_per_run: None,
            rerun_window_days: None,
            diag_staging_max_age_hours: None,
            credential_store: None,
            runner_liveness_timeout_secs: None,
            max_poll_window_secs: None,
            snapshot_retention_seconds: None,
            runner_version_deprecated: None,
            registration_policy: None,
            webhook_workers: None,
            webhook_watchdog_enabled: None,
            webhook_watchdog_interval_secs: None,
            webhook_watchdog_grace_secs: None,
            webhook_watchdog_max_attempts: None,
            webhook_watchdog_max_pages: None,
            token_ttl_secs: None,
            outbox_retention_seconds: None,
            github_app_default_events: None,
            github_app_registration: None,
            github_skip_workflows: None,
            github_pr_auto: None,
            github_pr_draft: None,
            github_pr_exclude: None,
            github_breaker_enabled: None,
            github_breaker_threshold: None,
        }
    }
}

/// Manual `Debug`: credential material is never printed.
impl std::fmt::Debug for StartupConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartupConfig")
            .field("config_path", &self.config_path)
            .field(
                "store_url",
                &self.store_url.as_deref().map(redact_store_url_debug),
            )
            .field("engine_home", &self.engine_home)
            .field("credentials_dir", &self.credentials_dir)
            .field("local_workspace", &self.local_workspace)
            .field(
                "system_token",
                &self.system_token.as_deref().map(|_| "<redacted>"),
            )
            .field(
                "local_jwt_key",
                &self.local_jwt_key.as_ref().map(|_| "<redacted>"),
            )
            .field("hmac_key", &self.hmac_key.as_deref().map(|_| "<redacted>"))
            .field(
                "github_token",
                &self.github_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "webhook_secret",
                &self.webhook_secret.as_deref().map(|_| "<redacted>"),
            )
            .field("github_app_id", &self.github_app_id)
            .field("github_installation_id", &self.github_installation_id)
            .field(
                "github_app_pem",
                &self
                    .github_app_pem
                    .iter()
                    .map(|(source, ..)| source)
                    .collect::<Vec<_>>(),
            )
            .field(
                "github_apps_json",
                &self
                    .github_apps_json
                    .as_ref()
                    .map(|_| "PRELOOP_GITHUB_APPS_JSON"),
            )
            .field("github_mint_failure", &self.github_mint_failure)
            .field("github_server_url", &self.github_server_url)
            .field("github_api_url", &self.github_api_url)
            .field("github_graphql_url", &self.github_graphql_url)
            .field("public_url", &self.public_url)
            .field("runner_url", &self.runner_url)
            .field("base_image", &self.base_image)
            .field("secrets_store", &self.secrets_store)
            .field("cache_total_quota_bytes", &self.cache_total_quota_bytes)
            .field("checkout_cache_mode", &self.checkout_cache_mode)
            .field(
                "checkout_cache_run_retention_seconds",
                &self.checkout_cache_run_retention_seconds,
            )
            .field(
                "checkout_cache_repository_retention_seconds",
                &self.checkout_cache_repository_retention_seconds,
            )
            .field("checkout_cache_max_bytes", &self.checkout_cache_max_bytes)
            .field("retention_days", &self.retention_days)
            .field("max_jobs_per_run", &self.max_jobs_per_run)
            .field("rerun_window_days", &self.rerun_window_days)
            .field(
                "diag_staging_max_age_hours",
                &self.diag_staging_max_age_hours,
            )
            .field("credential_store", &self.credential_store)
            .field(
                "runner_liveness_timeout_secs",
                &self.runner_liveness_timeout_secs,
            )
            .field("max_poll_window_secs", &self.max_poll_window_secs)
            .field(
                "snapshot_retention_seconds",
                &self.snapshot_retention_seconds,
            )
            .field("runner_version_deprecated", &self.runner_version_deprecated)
            .field("registration_policy", &self.registration_policy)
            .field("webhook_workers", &self.webhook_workers)
            .field("webhook_watchdog_enabled", &self.webhook_watchdog_enabled)
            .field(
                "webhook_watchdog_interval_secs",
                &self.webhook_watchdog_interval_secs,
            )
            .field(
                "webhook_watchdog_grace_secs",
                &self.webhook_watchdog_grace_secs,
            )
            .field(
                "webhook_watchdog_max_attempts",
                &self.webhook_watchdog_max_attempts,
            )
            .field(
                "webhook_watchdog_max_pages",
                &self.webhook_watchdog_max_pages,
            )
            .field("token_ttl_secs", &self.token_ttl_secs)
            .field("outbox_retention_seconds", &self.outbox_retention_seconds)
            .field("github_app_default_events", &self.github_app_default_events)
            .field("github_app_registration", &self.github_app_registration)
            .field("github_skip_workflows", &self.github_skip_workflows)
            .field("github_pr_auto", &self.github_pr_auto)
            .field("github_pr_draft", &self.github_pr_draft)
            .field("github_pr_exclude", &self.github_pr_exclude)
            .field("github_breaker_enabled", &self.github_breaker_enabled)
            .field("github_breaker_threshold", &self.github_breaker_threshold)
            .finish()
    }
}

/// Redact the password portion of a `postgres://user:pass@host/db` store URL
/// for `Debug` output; bare sqlite paths pass through untouched.
fn redact_store_url_debug(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let Some((userinfo, hostport)) = rest.rsplit_once('@') else {
        return url.to_owned();
    };
    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:***@{hostport}"),
        None => url.to_owned(),
    }
}

/// The `credential_store` key of the config file at `path`, read on its own.
///
/// The store has to be chosen before any credential reference can be resolved
/// through it, so this reads only that key: no reference is resolved, and
/// unrelated keys cannot fail the read. A missing file means the key is unset.
pub fn credential_store_setting(path: &Path) -> anyhow::Result<Option<String>> {
    #[derive(Deserialize)]
    struct Setting {
        #[serde(default)]
        credential_store: Option<String>,
    }
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("reading config {}", path.display()));
        }
    };
    let setting: Setting =
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    Ok(setting.credential_store)
}

/// Populate the `resolved_*` fields from the credential store.
///
/// Only the `*_ref` fields are consulted and only the non-persisted
/// `resolved_*` fields are written, so this is invisible to a subsequent
/// [`write_config`]: an unmigrated inline credential survives a load/write
/// round-trip untouched, and a migrated one is never written back as
/// plaintext.
///
/// A backend that cannot be reached at all (a headless Linux host with no
/// secret-service daemon) is a warning, not an error: the engine still has
/// the `PRELOOP_GITHUB_*` environment variables as its escape hatch, and
/// failing the load would take `preloop secret` and server startup down with
/// it. A reachable backend that fails an individual read is still an error.
pub fn resolve_credential_references(
    config: &mut ConfigFile,
    store: &dyn CredentialStore,
) -> anyhow::Result<()> {
    let references_present = config.github.app_pem_ref.is_some()
        || config.github.pat_ref.is_some()
        || config.github.webhook_secret_ref.is_some()
        || config
            .github
            .apps
            .iter()
            .any(|app| app.pem_ref.is_some() || app.webhook_secret_ref.is_some());
    if !references_present {
        return Ok(());
    }
    if let Err(error) = store.available() {
        tracing::warn!(
            %error,
            "config references stored credentials but no credential store is reachable; \
             falling back to inline values and PRELOOP_GITHUB_* environment variables"
        );
        return Ok(());
    }
    if let Some(reference) = &config.github.app_pem_ref {
        config.github.resolved_app_pem = read_credential(store, reference)?;
    }
    if let Some(reference) = &config.github.pat_ref {
        config.github.resolved_pat = read_credential(store, reference)?;
    }
    if let Some(reference) = &config.github.webhook_secret_ref {
        config.github.resolved_webhook_secret = read_credential(store, reference)?;
    }
    for app in &mut config.github.apps {
        if let Some(reference) = &app.pem_ref {
            app.resolved_pem = read_credential(store, reference)?;
        }
        if let Some(reference) = &app.webhook_secret_ref {
            app.resolved_webhook_secret = read_credential(store, reference)?;
        }
    }
    Ok(())
}

fn read_credential(
    store: &dyn CredentialStore,
    reference: &str,
) -> anyhow::Result<Option<SecretString>> {
    let reference = CredentialRef::new(reference.to_owned())?;
    store
        .get(&reference)
        .with_context(|| format!("reading credential reference {reference:?}"))
}

/// Prefix marking a sealed secret value in the config file. The remainder is
/// base64 of [`crate::store::Envelope`] bytes (versioned, HMAC-authenticated
/// ciphertext) produced with the config-secrets data-encryption key.
///
/// Values without the prefix are legacy plaintext: read as-is, re-sealed on
/// the next write. Sealing never weakens a legacy value — it only stops it
/// being readable straight off disk.
pub const SEALED_SECRET_PREFIX: &str = "sealed:v1:";

/// Legacy location of the config-secrets data-encryption key (hex-encoded),
/// written when no credential store was reachable. It is written with mode 0600
/// next to the config file, and anyone who can read it can unseal the secrets,
/// so it protects against a stolen config file or backup, not against a
/// compromised engine home. A leftover file is migrated into the selected store
/// the next time the key is needed. When it disagrees with the store, the key
/// that opens the sealed secrets wins and the superseded file is renamed to
/// `secrets.key.orphan-<unix-ts>`; it is never deleted.
pub const SECRETS_KEY_FILE: &str = "secrets.key";

/// Shortest decoded `sealed:v1:` envelope: format byte, AES-CBC IV, and HMAC
/// tag. Shorter values are plaintext that merely starts with the prefix.
const MIN_SEALED_ENVELOPE_LEN: usize = 1 + 16 + 32;

/// Terminal hint for unlocking a credential store that holds the sealing key.
const UNLOCK_STORE_HINT: &str = if cfg!(target_os = "macos") {
    "unlock the login keychain (macOS over SSH: `security unlock-keychain ~/Library/Keychains/login.keychain-db`) and retry"
} else {
    "unlock the credential store and retry"
};

/// Resolve the 32-byte data-encryption key that seals `[secrets]` values at
/// rest, from `store` (the engine's selected credential store). The reference
/// is scoped by config directory, so two engine homes owned by one OS user do
/// not share a key.
///
/// `sealed` lists the structurally valid `sealed:v1:` values that already exist
/// for this directory. A key is returned only once it authenticates them. A new
/// key is minted only when `sealed` is empty: a key minted over sealed values
/// cannot open them, and it would replace the key that can.
///
/// The 0600 `secrets.key` file is used when the store is unreachable. A file
/// beside a reachable store is migrated into it and retired, unless it
/// disagrees with the store; see [`resolve_conflict`].
fn resolve_config_sealing_key_with_store(
    dir: &Path,
    store: &dyn CredentialStore,
    sealed: &[String],
) -> anyhow::Result<[u8; 32]> {
    let key_path = dir.join(SECRETS_KEY_FILE);
    let file_key = read_sealing_key_file(&key_path)?;
    let resolved = match store.available() {
        Ok(()) => resolve_from_store(dir, &key_path, file_key, store, sealed),
        Err(error) => resolve_without_store(dir, &key_path, file_key, store, &error, sealed),
    }?;
    remember_resolved_sealing_key(dir, resolved)?;
    Ok(resolved)
}

/// The store cannot be reached, so only the legacy file can supply the key.
fn resolve_without_store(
    dir: &Path,
    key_path: &Path,
    file_key: Option<[u8; 32]>,
    store: &dyn CredentialStore,
    unavailable: &anyhow::Error,
    sealed: &[String],
) -> anyhow::Result<[u8; 32]> {
    match file_key {
        Some(key) => {
            if !sealed.is_empty() && !authenticates_any(&key, sealed) {
                anyhow::bail!(
                    "{SECRETS_KEY_FILE} in {} does not open the sealed config secrets, and the {} is unavailable ({unavailable:#}), so it cannot supply the right key; {UNLOCK_STORE_HINT}. Nothing was changed.",
                    dir.display(),
                    store.name(),
                );
            }
            Ok(key)
        }
        // A fresh home: no sealed value depends on a key yet, so the 0600 file
        // holds one until the store is reachable.
        None if sealed.is_empty() => {
            let key = generate_sealing_key();
            write_sealing_key_file(key_path, &key)?;
            Ok(key)
        }
        None => anyhow::bail!(
            "config secrets in {} are sealed with the config-secrets key held in the {}, which is unavailable ({unavailable:#}); {UNLOCK_STORE_HINT}. No key was minted: a new key cannot open these secrets.",
            dir.display(),
            store.name(),
        ),
    }
}

/// The store is reachable. Its copy of the key is authoritative unless the
/// legacy file disagrees, which [`resolve_conflict`] settles by evidence.
fn resolve_from_store(
    dir: &Path,
    key_path: &Path,
    file_key: Option<[u8; 32]>,
    store: &dyn CredentialStore,
    sealed: &[String],
) -> anyhow::Result<[u8; 32]> {
    let reference = config_secrets_key_reference(dir)?;
    let Some(secret) = store
        .get(&reference)
        .with_context(|| format!("reading config-secrets key from {} store", store.name()))?
    else {
        return resolve_missing_from_store(dir, key_path, file_key, store, &reference, sealed);
    };
    let store_key = parse_sealing_key(secret.expose().as_bytes())
        .with_context(|| format!("parsing config-secrets key from {} store", store.name()))?;
    match file_key {
        Some(file_key) if file_key != store_key => {
            resolve_conflict(dir, key_path, store, store_key, file_key, sealed)
        }
        file_key => {
            if !sealed.is_empty() && !authenticates_any(&store_key, sealed) {
                anyhow::bail!(
                    "the config-secrets key in the {} does not open the sealed config secrets in {}; restore the key that sealed them. Nothing was changed.",
                    store.name(),
                    dir.display(),
                );
            }
            if file_key.is_some() {
                // Retire the legacy file only once the store proves it can hold
                // the key. A value that was only read through (a migration whose
                // copy failed) must leave the file in place as the last way to
                // unseal the secrets.
                if store_confirms(store, &reference, &secret) {
                    std::fs::remove_file(key_path)
                        .with_context(|| format!("retiring {}", key_path.display()))?;
                } else {
                    tracing::warn!(
                        store = store.name(),
                        path = %key_path.display(),
                        "could not verify a durable copy of the config-secrets key; \
                         keeping the legacy key file"
                    );
                }
            }
            Ok(store_key)
        }
    }
}

/// The store is reachable but holds no key for this directory.
fn resolve_missing_from_store(
    dir: &Path,
    key_path: &Path,
    file_key: Option<[u8; 32]>,
    store: &dyn CredentialStore,
    reference: &CredentialRef,
    sealed: &[String],
) -> anyhow::Result<[u8; 32]> {
    match file_key {
        Some(key) => {
            if !sealed.is_empty() && !authenticates_any(&key, sealed) {
                anyhow::bail!(
                    "{SECRETS_KEY_FILE} in {} does not open the sealed config secrets, and the {} holds no key for them; if the right key is in a locked store, {UNLOCK_STORE_HINT}, otherwise restore it. Nothing was changed.",
                    dir.display(),
                    store.name(),
                );
            }
            // Migrate the legacy key into the store. The file goes only once the
            // store reads the same key back.
            store_and_verify_key(store, reference, &key)?;
            if key_path.exists() {
                std::fs::remove_file(key_path)
                    .with_context(|| format!("retiring {}", key_path.display()))?;
            }
            Ok(key)
        }
        None if sealed.is_empty() => {
            let key = generate_sealing_key();
            store_and_verify_key(store, reference, &key)?;
            Ok(key)
        }
        None => anyhow::bail!(
            "config secrets in {} are sealed with a config-secrets key that the {} does not hold, and there is no {SECRETS_KEY_FILE} to recover it from; if the key is in a locked store, {UNLOCK_STORE_HINT}, otherwise restore it. No key was minted: a new key cannot open these secrets.",
            dir.display(),
            store.name(),
        ),
    }
}

/// The store and `secrets.key` both hold a key, and they differ. Evidence
/// decides: the key that opens the sealed secrets wins, and the file it
/// displaces is renamed aside, never deleted. With nothing sealed, nothing
/// depends on either key, so the store is the key of record.
fn resolve_conflict(
    dir: &Path,
    key_path: &Path,
    store: &dyn CredentialStore,
    store_key: [u8; 32],
    file_key: [u8; 32],
    sealed: &[String],
) -> anyhow::Result<[u8; 32]> {
    let store_opens = authenticates_any(&store_key, sealed);
    let file_opens = authenticates_any(&file_key, sealed);
    match (sealed.is_empty(), store_opens, file_opens) {
        (true, _, _) | (false, true, false) => {
            let orphan = retire_sealing_key_file(key_path)?;
            tracing::warn!(
                store = store.name(),
                path = %key_path.display(),
                orphan = %orphan.display(),
                "config-secrets key file disagreed with the store; kept the store's key and retired the file"
            );
            Ok(store_key)
        }
        (false, false, true) => {
            tracing::warn!(
                store = store.name(),
                path = %key_path.display(),
                "the store's config-secrets key does not open the sealed config secrets; using the key file, which does"
            );
            Ok(file_key)
        }
        (false, false, false) => anyhow::bail!(
            "config-secrets key conflict in {}: the {} key and {} differ, and neither opens the sealed config secrets; restore the key that sealed them. Nothing was changed.",
            dir.display(),
            store.name(),
            key_path.display(),
        ),
        (false, true, true) => anyhow::bail!(
            "config-secrets key conflict in {}: the {} key and {} both open the sealed config secrets, so the right one cannot be told apart. Nothing was changed.",
            dir.display(),
            store.name(),
            key_path.display(),
        ),
    }
}

/// Write `key` to the store, then require a fresh read to return it unchanged.
fn store_and_verify_key(
    store: &dyn CredentialStore,
    reference: &CredentialRef,
    key: &[u8; 32],
) -> anyhow::Result<()> {
    store
        .set(reference, &SecretString::new(hex::encode(key)))
        .with_context(|| format!("writing config-secrets key to {}", store.name()))?;
    let read_back = store
        .get_fresh(reference)
        .with_context(|| format!("verifying config-secrets key in {} store", store.name()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{} store accepted the config-secrets key but did not return it",
                store.name()
            )
        })?;
    let read_back = parse_sealing_key(read_back.expose().as_bytes()).with_context(|| {
        format!(
            "parsing config-secrets key read from {} store",
            store.name()
        )
    })?;
    anyhow::ensure!(
        read_back == *key,
        "{} store returned a different config-secrets key after write",
        store.name()
    );
    Ok(())
}

/// Move a superseded key file aside as `secrets.key.orphan-<unix-ts>`. It is
/// renamed, never deleted: it may be the only copy of a key that still matters
/// for recovery. A name already taken gets a numeric suffix.
fn retire_sealing_key_file(path: &Path) -> anyhow::Result<PathBuf> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    let base = format!("{SECRETS_KEY_FILE}.orphan-{stamp}");
    let mut orphan = path.with_file_name(&base);
    let mut suffix = 1;
    while orphan.exists() {
        orphan = path.with_file_name(format!("{base}-{suffix}"));
        suffix += 1;
    }
    std::fs::rename(path, &orphan)
        .with_context(|| format!("retiring {} as {}", path.display(), orphan.display()))?;
    Ok(orphan)
}

/// Whether `key` opens at least one of `sealed`. Trial decryption is the only
/// evidence that a key sealed the config, so with nothing sealed this is false.
fn authenticates_any(key: &[u8; 32], sealed: &[String]) -> bool {
    sealed
        .iter()
        .any(|value| matches!(try_unseal_secret_value(key, value), Ok(Some(_))))
}

/// Config-secrets keys this process has resolved, by credential reference.
///
/// A config write can switch stores after the key was read from the old one.
/// The keychain locks mid-session, setup falls back to the file store for a new
/// credential, and the write that records the switch finds the file store
/// without the sealing key. The process still holds the key that opened the
/// sealed secrets, so the write carries that key across instead of failing and
/// stranding the credential it was meant to record. The map stays private to
/// this module and is never logged.
static RESOLVED_SEALING_KEYS: std::sync::LazyLock<parking_lot::Mutex<BTreeMap<String, [u8; 32]>>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(BTreeMap::new()));

/// Record the key this process resolved for `dir`, for [`carry_resolved_sealing_key`].
fn remember_resolved_sealing_key(dir: &Path, key: [u8; 32]) -> anyhow::Result<()> {
    let reference = config_secrets_key_reference(dir)?;
    RESOLVED_SEALING_KEYS
        .lock()
        .insert(reference.as_str().to_owned(), key);
    Ok(())
}

/// Copy a key this process already resolved into `store` when `store` holds no
/// key for `dir` and sealed values need one. Runs before a config write seals,
/// so the write lands in the store that now owns the key. A key that does not
/// open `sealed` is never copied.
fn carry_resolved_sealing_key(
    dir: &Path,
    store: &dyn CredentialStore,
    sealed: &[String],
) -> anyhow::Result<()> {
    if sealed.is_empty() || store.available().is_err() {
        return Ok(());
    }
    let reference = config_secrets_key_reference(dir)?;
    // A store that holds a key, or cannot be read, is left to resolution, which reports it.
    if !matches!(store.get(&reference), Ok(None)) {
        return Ok(());
    }
    let remembered = RESOLVED_SEALING_KEYS
        .lock()
        .get(reference.as_str())
        .copied();
    let Some(key) = remembered.filter(|key| authenticates_any(key, sealed)) else {
        return Ok(());
    };
    store_and_verify_key(store, &reference, &key)?;
    tracing::warn!(
        store = store.name(),
        "the store did not hold the config-secrets key; carried the key this process resolved into it"
    );
    Ok(())
}

/// Whether `store` verifiably holds `value` under `reference`: the write is
/// accepted and a fresh read returns the same bytes. Checked before retiring a
/// legacy `secrets.key`, so a credential that was only read through (a
/// migration whose copy failed) never causes the file to be deleted.
fn store_confirms(
    store: &dyn CredentialStore,
    reference: &CredentialRef,
    value: &SecretString,
) -> bool {
    match store
        .set(reference, value)
        .and_then(|()| store.get_fresh(reference))
    {
        Ok(Some(stored)) => stored.expose() == value.expose(),
        Ok(None) => false,
        Err(error) => {
            tracing::debug!(
                store = store.name(),
                %error,
                "credential store could not confirm a durable copy"
            );
            false
        }
    }
}

/// Credential-store reference for the config-secrets key, scoped by the
/// canonical config directory so distinct engine homes never share a key.
fn config_secrets_key_reference(dir: &Path) -> anyhow::Result<CredentialRef> {
    let absolute = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolving relative config directory")?
            .join(dir)
    };
    let canonical = std::fs::canonicalize(&absolute).unwrap_or(absolute);
    let digest = Sha256::digest(canonical.to_string_lossy().as_bytes());
    CredentialRef::new(format!("preloop-config-secrets-key-{digest:x}"))
}

fn generate_sealing_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    key
}

/// Parse a hex-encoded 32-byte key. Rejects anything else — a truncated or
/// hand-edited key file must fail loudly, never silently seal with the
/// wrong key.
fn parse_sealing_key(hex_bytes: &[u8]) -> anyhow::Result<[u8; 32]> {
    let text = std::str::from_utf8(hex_bytes).context("sealing key is not UTF-8")?;
    let raw = hex::decode(text.trim()).context("sealing key is not valid hex")?;
    raw.try_into()
        .map_err(|_| anyhow::anyhow!("sealing key must be 32 bytes"))
}

fn read_sealing_key_file(path: &Path) -> anyhow::Result<Option<[u8; 32]>> {
    match std::fs::read(path) {
        Ok(bytes) => parse_sealing_key(&bytes)
            .with_context(|| format!("parsing sealing key {}", path.display()))
            .map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

/// Write the key with mode 0600 via temp-file + rename, mirroring the
/// engine token file. Best-effort cleanup of the temp file on failure.
fn write_sealing_key_file(path: &Path, key: &[u8; 32]) -> anyhow::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating key dir {}", parent.display()))?;
    }
    let tmp_path = path.with_extension("key.tmp");
    let result = (|| -> anyhow::Result<()> {
        let mut tmp = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp_path)
            .with_context(|| format!("creating {}", tmp_path.display()))?;
        tmp.write_all(hex::encode(key).as_bytes())
            .with_context(|| format!("writing {}", tmp_path.display()))?;
        tmp.sync_all()
            .with_context(|| format!("syncing {}", tmp_path.display()))?;
        drop(tmp);
        std::fs::rename(&tmp_path, path)
            .with_context(|| format!("installing {}", path.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    result
}

/// Seal one secret value for storage in the config file.
fn seal_secret_value(key: &[u8; 32], plaintext: &str) -> anyhow::Result<String> {
    let sealed = crate::store::Envelope::new(key).seal(plaintext.as_bytes())?;
    Ok(format!(
        "{SEALED_SECRET_PREFIX}{}",
        BASE64_STANDARD.encode(&sealed)
    ))
}

/// Try to unseal one `sealed:v1:` config value. A prefix with malformed
/// base64 is treated as an ordinary plaintext value; a structurally valid
/// envelope must authenticate or the load fails closed.
fn try_unseal_secret_value(key: &[u8; 32], stored: &str) -> anyhow::Result<Option<String>> {
    let Some(encoded) = stored.strip_prefix(SEALED_SECRET_PREFIX) else {
        return Ok(None);
    };
    let Ok(sealed) = BASE64_STANDARD.decode(encoded) else {
        return Ok(None);
    };
    // SNAPSHOT_FORMAT + AES-CBC IV + HMAC tag. Short prefixed plaintext is
    // not a candidate ciphertext and must remain readable.
    if sealed.len() < MIN_SEALED_ENVELOPE_LEN {
        return Ok(None);
    }
    let plaintext = crate::store::Envelope::new(key).unseal(&sealed)?;
    Ok(Some(
        String::from_utf8(plaintext).context("sealed config value is not valid UTF-8")?,
    ))
}

/// Apply `f` to every stored secret value across the three scopes (global,
/// per-repo, per-environment).
fn for_each_secret_value(
    config: &mut ConfigFile,
    mut f: impl FnMut(&mut String) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    for value in config.secrets.values_mut() {
        f(value)?;
    }
    for repo in config.repo_secrets.values_mut() {
        for value in repo.values_mut() {
            f(value)?;
        }
    }
    for envs in config.env_secrets.values_mut() {
        for repo in envs.values_mut() {
            for value in repo.values_mut() {
                f(value)?;
            }
        }
    }
    Ok(())
}

/// Every structurally valid `sealed:v1:` value in `config`, across all scopes.
fn sealed_envelopes(config: &ConfigFile) -> Vec<String> {
    config
        .secrets
        .values()
        .chain(config.repo_secrets.values().flat_map(|repo| repo.values()))
        .chain(
            config
                .env_secrets
                .values()
                .flat_map(|envs| envs.values().flat_map(|repo| repo.values())),
        )
        .filter(|value| is_sealed_envelope(value))
        .cloned()
        .collect()
}

/// Whether `stored` is a structurally valid sealed value. Anything else is
/// plaintext, which is not evidence about any key.
fn is_sealed_envelope(stored: &str) -> bool {
    stored
        .strip_prefix(SEALED_SECRET_PREFIX)
        .and_then(|encoded| BASE64_STANDARD.decode(encoded).ok())
        .is_some_and(|sealed| sealed.len() >= MIN_SEALED_ENVELOPE_LEN)
}

/// Structurally valid sealed values in the config text. Valid TOML is read
/// through its secret tables, so a comment or an unrelated field can never
/// count as a sealed value. Malformed TOML has no tables to read, so it is
/// scanned as raw text instead: a damaged file must not hide its sealed values
/// from the key check.
fn sealed_envelopes_in_text(text: &str) -> Vec<String> {
    match toml::from_str::<toml::Table>(text) {
        Ok(document) => sealed_envelopes_in_document(&document),
        Err(_) => sealed_envelopes_scanned(text),
    }
}

/// The sealed values in a parsed config, in the scopes `for_each_secret_value`
/// covers: global, per-repo, and per-environment.
fn sealed_envelopes_in_document(document: &toml::Table) -> Vec<String> {
    let mut found = Vec::new();
    let mut take = |value: &toml::Value| {
        if let Some(text) = value.as_str().filter(|text| is_sealed_envelope(text)) {
            found.push(text.to_owned());
        }
    };
    if let Some(global) = document.get("secrets").and_then(toml::Value::as_table) {
        global.values().for_each(&mut take);
    }
    if let Some(per_repo) = document.get("repo_secrets").and_then(toml::Value::as_table) {
        for repo in per_repo.values().filter_map(toml::Value::as_table) {
            repo.values().for_each(&mut take);
        }
    }
    if let Some(per_repo) = document.get("env_secrets").and_then(toml::Value::as_table) {
        for envs in per_repo.values().filter_map(toml::Value::as_table) {
            for repo in envs.values().filter_map(toml::Value::as_table) {
                repo.values().for_each(&mut take);
            }
        }
    }
    found
}

/// Raw-text scan: the fallback for a file that is not valid TOML.
fn sealed_envelopes_scanned(text: &str) -> Vec<String> {
    text.match_indices(SEALED_SECRET_PREFIX)
        .filter_map(|(start, _)| {
            let rest = &text[start + SEALED_SECRET_PREFIX.len()..];
            let run = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '=')))
                .unwrap_or(rest.len());
            let candidate = &text[start..start + SEALED_SECRET_PREFIX.len() + run];
            is_sealed_envelope(candidate).then(|| candidate.to_owned())
        })
        .collect()
}

/// Sealed values already stored in the config file at `path`.
fn sealed_envelopes_on_disk(path: &Path) -> anyhow::Result<Vec<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(sealed_envelopes_in_text(&text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

fn seal_config_secrets(
    config: &mut ConfigFile,
    dir: &Path,
    store: &dyn CredentialStore,
    on_disk: &[String],
) -> anyhow::Result<()> {
    let has_secrets = !config.secrets.is_empty()
        || config.repo_secrets.values().any(|repo| !repo.is_empty())
        || config
            .env_secrets
            .values()
            .any(|envs| envs.values().any(|repo| !repo.is_empty()));
    if !has_secrets {
        return Ok(());
    }
    // Values already sealed, on disk or in memory, must open with the key that
    // seals new ones.
    let mut sealed = on_disk.to_vec();
    sealed.extend(sealed_envelopes(config));
    carry_resolved_sealing_key(dir, store, &sealed)?;
    let key = resolve_config_sealing_key_with_store(dir, store, &sealed)?;
    for_each_secret_value(config, |value| {
        // Only authenticated envelopes are already sealed. A plaintext secret
        // is allowed to begin with the marker; malformed marker-prefixed
        // values must be encrypted rather than silently left on disk.
        if try_unseal_secret_value(&key, value)?.is_some() {
            return Ok(());
        }
        *value = seal_secret_value(&key, value)?;
        Ok(())
    })
}

/// Unseal stored secrets after loading. Resolves the key lazily — only when
/// at least one structurally valid sealed value is actually present — so
/// legacy plaintext configs and marker-prefixed plaintext never touch the
/// keychain or key file.
fn unseal_config_secrets(
    config: &mut ConfigFile,
    dir: &Path,
    store: &dyn CredentialStore,
) -> anyhow::Result<()> {
    let sealed = sealed_envelopes(config);
    if sealed.is_empty() {
        return Ok(());
    }
    let key = resolve_config_sealing_key_with_store(dir, store, &sealed)?;
    for_each_secret_value(config, |value| {
        if let Some(plaintext) = try_unseal_secret_value(&key, value)? {
            *value = plaintext;
        }
        Ok(())
    })
}

/// Load the config file at `path`.
///
/// A missing or empty file yields the default config; a malformed file is an
/// error. The credential-store env layer (`PRELOOP_CREDENTIAL_STORE`) is read
/// at the config-load entry point, and passed down explicitly; startup
/// callers that already resolved it use [`load_config_from_with`] so nothing
/// below the [`StartupConfig`] boundary re-reads the process environment.
pub fn load_config_from(path: &Path) -> anyhow::Result<ConfigFile> {
    load_config_from_with(
        path,
        std::env::var(crate::credential_store::CREDENTIAL_STORE_ENV)
            .ok()
            .as_deref(),
    )
}

/// [`load_config_from`] with the `PRELOOP_CREDENTIAL_STORE` env layer passed
/// explicitly (blank counts as unset, matching the historical filter).
pub fn load_config_from_with(
    path: &Path,
    credential_store_env: Option<&str>,
) -> anyhow::Result<ConfigFile> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ConfigFile::default());
        }
        Err(error) => return Err(error).context(format!("reading config {}", path.display())),
    };
    let mut config: ConfigFile =
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    validate_environment_rules(&config)
        .with_context(|| format!("validating config {}", path.display()))?;
    validate_execution_protection(&config)
        .with_context(|| format!("validating config {}", path.display()))?;
    let store = crate::credential_store::selected_store_for_config(
        path,
        credential_store_env.filter(|value| !value.trim().is_empty()),
        config.credential_store.as_deref(),
    )?;
    resolve_credential_references(&mut config, store.as_ref())?;
    // Unseal stored job secrets; legacy plaintext values pass through and
    // are re-sealed on the next write.
    if let Some(dir) = path.parent() {
        unseal_config_secrets(&mut config, dir, store.as_ref())
            .with_context(|| format!("unsealing secrets in config {}", path.display()))?;
    }
    Ok(config)
}

/// Reject environment protection rules that promise more than the
/// single-operator trust model can deliver.
///
/// Approvals are authenticated only with the shared system token, so
/// `required_reviewers > 1` cannot mean distinct reviewers: one token holder
/// could satisfy the quorum alone by calling the approval endpoint
/// repeatedly. Fail the config load closed rather than run with a
/// misleading gate.
///
/// Wait timers are also range-checked: the gate converts minutes to i64
/// nanoseconds, and a `u64` value above `i64::MAX` would cast to a negative
/// delay (bypassing the wait). TOML integers top out at `i64::MAX`, so no
/// config file can trigger this today, but the bound is enforced anyway so
/// the cast site stays total.
fn validate_environment_rules(config: &ConfigFile) -> anyhow::Result<()> {
    /// Largest whole minutes representable as i64 nanoseconds.
    const MAX_WAIT_TIMER_MINUTES: u64 = (i64::MAX as u64) / 60_000_000_000;
    anyhow::ensure!(
        config.environments.is_empty(),
        "the `[environments]` registry is no longer supported: GitHub accepts any \
         `environment:` name and auto-creates it unprotected, so preloop no longer \
         rejects unregistered names. Delete the table; environment secrets stay keyed \
         by name under `[env_secrets]`, and protection rules come from the repository's \
         environment configuration (or `[environment_rules]` in local mode)"
    );
    for (repo, envs) in &config.environment_rules {
        for (env, rules) in envs {
            anyhow::ensure!(
                rules.required_reviewers <= 1,
                "environment_rules[{repo}][{env}]: required_reviewers = {} is not supported; \
                 preloop has no user identities, so at most 1 operator approval can be required",
                rules.required_reviewers
            );
            anyhow::ensure!(
                rules.wait_timer_minutes <= MAX_WAIT_TIMER_MINUTES,
                "environment_rules[{repo}][{env}]: wait_timer_minutes = {} is too large; \
                 the wait must fit in i64 nanoseconds (max {MAX_WAIT_TIMER_MINUTES} minutes)",
                rules.wait_timer_minutes
            );
        }
    }
    Ok(())
}

/// Reject execution-protection rules the matcher cannot honor.
///
/// `workflows` patterns are matched with [`preloop_gha_parser::glob_match`],
/// which implements `*`, `**`, and `?` only. A GitHub-style character class
/// like `deploy-[0-9].yml` would parse but match literally, silently missing
/// the intended workflow — so fail the config load closed instead.
fn validate_execution_protection(config: &ConfigFile) -> anyhow::Result<()> {
    validate_execution_protection_rules(&config.execution_protection)
}

/// Reject execution-protection rules the matcher cannot honor.
///
/// `workflows` patterns are matched with [`preloop_gha_parser::glob_match`],
/// which implements `*`, `**`, and `?` only. A GitHub-style character class
/// like `deploy-[0-9].yml` would parse but match literally, silently missing
/// the intended workflow — so fail the config load closed instead.
///
/// Shared by config load and the rule-management API: a rule the API accepts
/// must also survive a restart, so both run the same check.
pub(crate) fn validate_execution_protection_rules(
    policy: &ExecutionProtectionConfig,
) -> anyhow::Result<()> {
    let mut rules: Vec<(&str, &str, &Option<Vec<String>>)> = Vec::new();
    for rule in &policy.event_rules {
        rules.push(("event rule", rule.event.as_str(), &rule.workflows));
    }
    for rule in &policy.actor_rules {
        rules.push(("actor rule", rule.actor.as_str(), &rule.workflows));
    }
    for (kind, target, workflows) in rules {
        if let Some(patterns) = workflows {
            for pattern in patterns {
                anyhow::ensure!(
                    !pattern.contains(['[', ']']),
                    "execution_protection {kind} ({target:?}): workflow pattern \
                     {pattern:?} uses character classes, which the matcher does \
                     not support (only `*`, `**`, `?`); rewrite the pattern or \
                     remove the rule",
                );
            }
        }
    }
    Ok(())
}

/// Atomically write the config file with mode 0600.
pub fn write_config(config: &ConfigFile) -> anyhow::Result<PathBuf> {
    #[cfg(any(test, feature = "test-support"))]
    let _config_guard = CONFIG_PATH_LOCK.lock();
    let path = config_path();
    write_config_to(&path, config)?;
    Ok(path)
}

/// Atomically write `config` to `path` with mode 0600.
///
/// The credential-store env layer is read here exactly as in
/// [`load_config_from`]; startup callers that resolved it use
/// [`write_config_to_with`].
pub fn write_config_to(path: &Path, config: &ConfigFile) -> anyhow::Result<()> {
    write_config_to_with(
        path,
        config,
        std::env::var(crate::credential_store::CREDENTIAL_STORE_ENV)
            .ok()
            .as_deref(),
    )
}

/// [`write_config_to`] with the `PRELOOP_CREDENTIAL_STORE` env layer passed
/// explicitly.
pub fn write_config_to_with(
    path: &Path,
    config: &ConfigFile,
    credential_store_env: Option<&str>,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating config dir {}", parent.display()))?;
    }
    // Seal secrets before serializing: the file on disk must never hold
    // plaintext secret values. Works on a clone so the caller's in-memory
    // config keeps plaintext for the running process.
    let mut sealed = config.clone();
    if let Some(dir) = path.parent() {
        // Same store selection as `load_config_from`; sealing with a different
        // backend than unsealing makes the file unreadable.
        let store = crate::credential_store::selected_store_for_config(
            path,
            credential_store_env.filter(|value| !value.trim().is_empty()),
            sealed.credential_store.as_deref(),
        )?;
        let on_disk = sealed_envelopes_on_disk(path)?;
        seal_config_secrets(&mut sealed, dir, store.as_ref(), &on_disk)
            .with_context(|| format!("sealing secrets in config {}", path.display()))?;
    }
    let text = toml::to_string_pretty(&sealed).context("serializing config")?;
    // Unique per write: a fixed temp name lets a second writer truncate the
    // first writer's half-written file before either rename lands. Same
    // reason the action tarball download uses a per-request temp path.
    let tmp = path.with_extension(format!("toml.{}.tmp", uuid::Uuid::new_v4()));
    let write = || -> std::io::Result<()> {
        let mut file = std::fs::File::create(&tmp)?;
        // Permissions before content: the file must never be readable by
        // others while it holds secrets.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(text.as_bytes())?;
        file.sync_all()
    };
    if let Err(error) = write() {
        let _ = std::fs::remove_file(&tmp);
        return Err(error).with_context(|| format!("writing {}", tmp.display()));
    }
    if let Err(error) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error).with_context(|| format!("writing {}", path.display()));
    }
    Ok(())
}

/// Serializes config-file I/O against the shared pinned test config path so
/// concurrent tests cannot tear each other's read-modify-write cycles.
#[cfg(any(test, feature = "test-support"))]
static CONFIG_PATH_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// The pinned per-process test config path. Test builds never resolve (or
/// write) the developer's real config through the default path: the default
/// is one temp file per test process, shared so tests that pass config
/// through the file still agree.
#[cfg(any(test, feature = "test-support"))]
pub fn pinned_test_config_path() -> PathBuf {
    use std::sync::LazyLock;
    static TEST_CONFIG_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
        let dir = std::env::temp_dir().join(format!("preloop-test-config-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        dir.join("config.toml")
    });
    TEST_CONFIG_PATH.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential_store::WriteOnlyCredentialStore;

    /// The App-registration flag accepts exactly the documented on-values:
    /// `PRELOOP_ENABLE_APP_REGISTRATION` is read once by [`StartupConfig::from_env`],
    /// so the mapping is a pure parse and needs no process-wide variable.
    #[test]
    fn app_registration_switch_accepts_only_the_documented_on_values() {
        for value in ["1", "true", "TRUE", " on ", "On"] {
            assert!(parse_switch(value), "{value:?} should enable registration");
        }
        for value in ["", "0", "false", "off", "yes"] {
            assert!(
                !parse_switch(value),
                "{value:?} should not enable registration"
            );
        }
    }

    // These tests use the path-taking helpers rather than pinning
    // `PRELOOP_CONFIG`: mutating a process-wide env var raced every other test
    // in the binary, so they only passed under `--test-threads=1`.
    fn populated_config() -> ConfigFile {
        ConfigFile {
            github: GitHubConfig {
                app_id: Some("123".into()),
                legacy_app_pem: Some(
                    "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----\n".into(),
                ),
                mint_failure: Some("pat".into()),
                legacy_pat: Some("ghp_secret".into()),
                ..Default::default()
            },
            secrets: BTreeMap::from([("DOCKERHUB_TOKEN".into(), "abc123".into())]),
            repo_secrets: BTreeMap::from([(
                "owner/repo".into(),
                BTreeMap::from([("REPO_ONLY".into(), "xyz789".into())]),
            )]),
            env_secrets: BTreeMap::from([(
                "owner/repo".into(),
                BTreeMap::from([(
                    "prod".into(),
                    BTreeMap::from([("DEPLOY_KEY".into(), "env-secret".into())]),
                )]),
            )]),
            environments: BTreeMap::new(),
            token_permissions_ceiling: None,
            environment_rules: BTreeMap::new(),
            fork_policy: ForkPolicyConfig::default(),
            execution_protection: ExecutionProtectionConfig::default(),
            secrets_store: None,
            credential_store: None,
            checkout_cache: CheckoutCacheConfig::default(),
            cache: CacheConfig::default(),
            retention_days: DEFAULT_RETENTION_DAYS,
            max_jobs_per_run: DEFAULT_MAX_JOBS_PER_RUN,
            diag_staging_max_age_hours: DEFAULT_DIAG_STAGING_MAX_AGE_HOURS,
            golden: GoldenConfig::default(),
        }
    }

    fn write_sealed_config(
        path: &Path,
        config: &ConfigFile,
        store: &dyn CredentialStore,
    ) -> anyhow::Result<()> {
        let mut sealed = config.clone();
        if let Some(dir) = path.parent() {
            let on_disk = sealed_envelopes_on_disk(path)?;
            seal_config_secrets(&mut sealed, dir, store, &on_disk)?;
        }
        std::fs::write(path, toml::to_string_pretty(&sealed)?)?;
        Ok(())
    }

    #[test]
    fn missing_file_is_empty_config() {
        let dir = tempfile::tempdir().unwrap();
        let config = load_config_from(&dir.path().join("config.toml")).unwrap();
        assert!(config.github.app_id.is_none());
        assert!(config.secrets.is_empty());
    }

    #[test]
    fn round_trips_github_and_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_config_to(&path, &populated_config()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "config file must be 0600");
        }
        let loaded = load_config_from(&path).unwrap();
        assert_eq!(loaded.github.app_id.as_deref(), Some("123"));
        assert_eq!(loaded.github.pat(), Some("ghp_secret"));
        assert_eq!(
            loaded.secrets.get("DOCKERHUB_TOKEN").map(String::as_str),
            Some("abc123")
        );
        assert_eq!(
            loaded
                .repo_secrets
                .get("owner/repo")
                .and_then(|map| map.get("REPO_ONLY"))
                .map(String::as_str),
            Some("xyz789")
        );
    }

    #[test]
    fn malformed_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "not [ valid toml ==").unwrap();
        assert!(load_config_from(&path).is_err());
    }

    #[test]
    fn required_reviewers_above_one_is_rejected() {
        // One system token can approve repeatedly, so a quorum above 1 would
        // imply distinct reviewers that cannot exist. Fail closed at load.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "[environment_rules.\"owner/repo\".prod]\nrequired_reviewers = 2\n",
        )
        .unwrap();
        let err = load_config_from(&path).unwrap_err();
        assert!(
            format!("{err:?}").contains("required_reviewers"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn required_reviewers_zero_or_one_is_accepted() {
        for reviewers in [0, 1] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("config.toml");
            std::fs::write(
                &path,
                format!(
                    "[environment_rules.\"owner/repo\".prod]\nrequired_reviewers = {reviewers}\n"
                ),
            )
            .unwrap();
            let config = load_config_from(&path).unwrap();
            assert_eq!(
                config.environment_rules["owner/repo"]["prod"].required_reviewers,
                reviewers
            );
        }
    }

    #[test]
    fn wait_timer_minutes_overflow_is_rejected() {
        // The gate casts minutes to i64 nanoseconds; a u64 above i64::MAX
        // would cast negative and bypass the wait. TOML integers top out at
        // i64::MAX so no config file can hit this today, but the bound is
        // enforced at load so the cast site stays total.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        // i64::MAX minutes is TOML-representable but not as i64 nanoseconds.
        std::fs::write(
            &path,
            "[environment_rules.\"owner/repo\".prod]\nwait_timer_minutes = 9223372036854775807\n",
        )
        .unwrap();
        let err = load_config_from(&path).unwrap_err();
        assert!(
            format!("{err:?}").contains("wait_timer_minutes"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn wait_timer_minutes_boundary_is_accepted() {
        // Largest whole minutes that fit in i64 nanoseconds: the cast in the
        // gate is the identity and saturating_mul cannot saturate.
        let max_minutes = (i64::MAX as u64) / 60_000_000_000;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            format!(
                "[environment_rules.\"owner/repo\".prod]\nwait_timer_minutes = {max_minutes}\n"
            ),
        )
        .unwrap();
        let config = load_config_from(&path).unwrap();
        assert_eq!(
            config.environment_rules["owner/repo"]["prod"].wait_timer_minutes,
            max_minutes
        );
        assert_eq!(
            (max_minutes as i64).saturating_mul(60_000_000_000) / 60_000_000_000,
            max_minutes as i64
        );
    }

    use crate::credential_store::{
        CredentialWriter, FileCredentialStore, MemoryCredentialStore, UnavailableCredentialStore,
        Written, credential_store_dir, github_reference,
    };

    fn config_with_refs(store: &MemoryCredentialStore) -> ConfigFile {
        let pem_ref = github_reference("app-pem", Some("123")).unwrap();
        let pat_ref = github_reference("pat", None).unwrap();
        store
            .set(
                &pem_ref,
                &preloop_gha_protocol::SecretString::new("STORED-PEM"),
            )
            .unwrap();
        store
            .set(
                &pat_ref,
                &preloop_gha_protocol::SecretString::new("STORED-PAT"),
            )
            .unwrap();
        ConfigFile {
            github: GitHubConfig {
                app_id: Some("123".into()),
                app_pem_ref: Some(pem_ref.as_str().to_owned()),
                pat_ref: Some(pat_ref.as_str().to_owned()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// The regression that motivated splitting `legacy_*` from `resolved_*`:
    /// resolving used to overwrite the inline fields, so the next
    /// `write_config` serialized the credential store's plaintext straight
    /// back into config.toml — which every `preloop secret set` does.
    #[test]
    fn resolved_credentials_are_never_written_back_as_plaintext() {
        let store = MemoryCredentialStore::default();
        let mut config = config_with_refs(&store);
        resolve_credential_references(&mut config, &store).unwrap();
        assert_eq!(config.github.app_pem(), Some("STORED-PEM"));
        assert_eq!(config.github.pat(), Some("STORED-PAT"));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_config_to(&path, &config).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("STORED-PEM"), "PEM written back: {text}");
        assert!(!text.contains("STORED-PAT"), "PAT written back: {text}");
        assert!(text.contains("app_pem_ref"), "reference dropped: {text}");
        assert!(text.contains("pat_ref"), "reference dropped: {text}");
    }

    /// An operator who has not migrated yet must not silently lose their
    /// credential when an unrelated command rewrites the file.
    #[test]
    fn unmigrated_inline_credentials_survive_a_round_trip() {
        let store = MemoryCredentialStore::default();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut config = ConfigFile {
            github: GitHubConfig {
                app_id: Some("123".into()),
                legacy_pat: Some("ghp_inline".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        resolve_credential_references(&mut config, &store).unwrap();
        write_config_to(&path, &config).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("ghp_inline"), "inline PAT lost: {text}");
        assert_eq!(
            load_config_from(&path).unwrap().github.pat(),
            Some("ghp_inline")
        );
    }

    /// A headless host has no secret-service daemon. Failing the load there
    /// would take `preloop secret` and server startup down with it, even
    /// though `PRELOOP_GITHUB_*` could still supply the credentials.
    #[test]
    fn an_unreachable_store_degrades_instead_of_failing_the_load() {
        let mut config = config_with_refs(&MemoryCredentialStore::default());
        resolve_credential_references(&mut config, &UnavailableCredentialStore).unwrap();
        assert_eq!(config.github.app_pem(), None);
        assert_eq!(config.github.pat(), None);
    }

    /// No references means no reason to touch the backend at all.
    #[test]
    fn a_config_without_references_never_consults_the_store() {
        let mut config = ConfigFile::default();
        resolve_credential_references(&mut config, &UnavailableCredentialStore).unwrap();
        assert_eq!(config.github.app_pem(), None);
    }

    /// Registry entries resolve too, and the store wins over a stale inline
    /// value left behind by a partial migration.
    #[test]
    fn registry_entries_prefer_the_store_over_inline_values() {
        let store = MemoryCredentialStore::default();
        let pem_ref = github_reference("app-pem", Some("456")).unwrap();
        store
            .set(
                &pem_ref,
                &preloop_gha_protocol::SecretString::new("STORED-PEM"),
            )
            .unwrap();
        let mut config = ConfigFile {
            github: GitHubConfig {
                apps: vec![AppConfig {
                    app_id: "456".into(),
                    legacy_pem: "INLINE-PEM".into(),
                    pem_ref: Some(pem_ref.as_str().to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        };
        resolve_credential_references(&mut config, &store).unwrap();
        assert_eq!(config.github.apps[0].pem(), "STORED-PEM");
    }
    #[test]
    fn unreachable_store_degrades_registry_apps_to_empty() {
        let mut config = ConfigFile {
            github: GitHubConfig {
                apps: vec![AppConfig {
                    app_id: "456".into(),
                    pem_ref: Some("github-app-pem-456".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        };
        resolve_credential_references(&mut config, &UnavailableCredentialStore).unwrap();
        assert_eq!(config.github.apps[0].pem(), "");
    }

    #[test]
    fn host_scoped_credential_references_resolve() {
        let store = MemoryCredentialStore::default();
        let pat_ref = crate::credential_store::github_reference_with_host(
            "pat",
            Some("https://ghe.internal"),
            None,
        )
        .unwrap();
        store
            .set(
                &pat_ref,
                &preloop_gha_protocol::SecretString::new("GHE-PAT"),
            )
            .unwrap();
        let mut config = ConfigFile {
            github: GitHubConfig {
                pat_ref: Some(pat_ref.as_str().to_owned()),
                ..Default::default()
            },
            ..Default::default()
        };
        resolve_credential_references(&mut config, &store).unwrap();
        assert_eq!(config.github.pat(), Some("GHE-PAT"));
    }

    /// A `debug!(?config)` must never disclose the PAT or the private key.
    #[test]
    fn debug_redacts_credentials() {
        let config = populated_config();
        let github = format!("{:?}", config.github);
        assert!(!github.contains("ghp_secret"), "PAT leaked: {github}");
        assert!(
            !github.contains("BEGIN RSA PRIVATE KEY"),
            "PEM leaked: {github}"
        );
        assert!(!github.contains("abc"), "PEM body leaked: {github}");
        assert!(github.contains("app_id: Some(\"123\")"), "{github}");
        assert!(github.contains("app_pem: <redacted>"), "{github}");
        assert!(github.contains("pat: <redacted>"), "{github}");

        let whole = format!("{config:?}");
        assert!(!whole.contains("abc123"), "secret value leaked: {whole}");
        assert!(!whole.contains("xyz789"), "repo secret leaked: {whole}");
        assert!(whole.contains("secrets: 1 names"), "{whole}");
        assert!(whole.contains("repo_secrets: 1 repos"), "{whole}");
    }

    #[test]
    fn startup_debug_redacts_inline_apps_json() {
        let startup = StartupConfig {
            github_apps_json: Some(
                r#"[{"app_id":"1","pem":"PEM-SECRET","webhook_secret":"HOOK-SECRET"}]"#.to_owned(),
            ),
            ..StartupConfig::for_tests()
        };
        let debug = format!("{startup:?}");
        assert!(!debug.contains("PEM-SECRET"), "private key leaked: {debug}");
        assert!(
            !debug.contains("HOOK-SECRET"),
            "webhook secret leaked: {debug}"
        );
        assert!(
            debug.contains("github_apps_json: Some(\"PRELOOP_GITHUB_APPS_JSON\")"),
            "source metadata missing: {debug}"
        );
    }

    /// Store-mode resolution over explicit inputs: the env layer wins over
    /// the config file, blanks count as unset, and unknown values fail
    /// closed. No process env is involved.
    #[test]
    fn secrets_store_mode_resolves_config_field_and_rejects_unknowns() {
        let file_mode = ConfigFile {
            secrets_store: Some("file".into()),
            ..ConfigFile::default()
        };
        let memory_mode = ConfigFile {
            secrets_store: Some("memory".into()),
            ..ConfigFile::default()
        };
        assert_eq!(
            secrets_store_mode(&file_mode, None).unwrap(),
            SecretsStoreMode::File
        );
        assert_eq!(
            secrets_store_mode(&memory_mode, None).unwrap(),
            SecretsStoreMode::Memory
        );
        assert_eq!(
            secrets_store_mode(&ConfigFile::default(), None).unwrap(),
            SecretsStoreMode::File
        );
        assert_eq!(
            secrets_store_mode(&file_mode, Some("memory")).unwrap(),
            SecretsStoreMode::Memory,
            "the env layer wins over the config file"
        );
        assert_eq!(
            secrets_store_mode(&memory_mode, Some("  ")).unwrap(),
            SecretsStoreMode::Memory,
            "a blank override counts as unset"
        );
        // Fail closed: a typo in either source must error, never silently
        // fall back to writing plaintext secrets to the file.
        let typo = ConfigFile {
            secrets_store: Some("memroy".into()),
            ..ConfigFile::default()
        };
        assert!(secrets_store_mode(&typo, None).is_err());
        assert!(secrets_store_mode(&file_mode, Some("memroy")).is_err());
        assert!(store_memory(&typo, None).is_err());
        assert!(store_memory(&memory_mode, None).unwrap());
        assert!(!store_memory(&file_mode, None).unwrap());
    }

    #[test]
    fn credential_load_merges_per_name_and_repo() {
        let dir = tempfile::tempdir().unwrap();
        let cred_path = dir.path().join(CREDENTIAL_NAME);
        std::fs::write(
            &cred_path,
            r#"
[secrets]
OVERLAY = "from-credential"
SHARED = "credential-wins"

[repo_secrets."owner/repo"]
REPO_OVERLAY = "repo-from-credential"
"#,
        )
        .unwrap();
        let overlay = load_credential_from(&cred_path).unwrap();
        let mut config = ConfigFile {
            secrets: BTreeMap::from([
                ("SHARED".into(), "file-value".into()),
                ("FILE_ONLY".into(), "stays".into()),
            ]),
            repo_secrets: BTreeMap::from([(
                "owner/repo".into(),
                BTreeMap::from([("REPO_SHARED".into(), "file-repo".into())]),
            )]),
            ..ConfigFile::default()
        };
        merge_secret_stores(&mut config, overlay);

        assert_eq!(config.secrets.get("OVERLAY").unwrap(), "from-credential");
        assert_eq!(config.secrets.get("SHARED").unwrap(), "credential-wins");
        assert_eq!(config.secrets.get("FILE_ONLY").unwrap(), "stays");
        assert_eq!(
            config.repo_secrets["owner/repo"]
                .get("REPO_OVERLAY")
                .unwrap(),
            "repo-from-credential"
        );
        assert_eq!(
            config.repo_secrets["owner/repo"]
                .get("REPO_SHARED")
                .unwrap(),
            "file-repo"
        );
    }

    #[test]
    fn credential_load_treats_missing_file_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let overlay = load_credential_from(&dir.path().join(CREDENTIAL_NAME)).unwrap();
        assert!(overlay.secrets.is_empty());
        assert!(overlay.repo_secrets.is_empty());
        // A malformed credential must fail loudly, not silently no-op.
        let bad = dir.path().join("bad");
        std::fs::write(&bad, "[secrets\nnot-valid").unwrap();
        assert!(load_credential_from(&bad).is_err());
    }

    // --- config-file secret sealing ---

    /// The file on disk must hold ciphertext, never plaintext secret values.
    #[test]
    fn secrets_are_sealed_at_rest() {
        let store = MemoryCredentialStore::default();
        let dir = tempfile::tempdir().unwrap();
        let mut config = populated_config();
        seal_config_secrets(&mut config, dir.path(), &store, &[]).unwrap();
        let text = toml::to_string_pretty(&config).unwrap();
        assert!(!text.contains("abc123"), "global secret leaked: {text}");
        assert!(!text.contains("xyz789"), "repo secret leaked: {text}");
        assert!(
            !text.contains("env-secret"),
            "environment secret leaked: {text}"
        );
        assert!(
            text.contains(SEALED_SECRET_PREFIX),
            "no sealed values written: {text}"
        );
        // The sealed form round-trips back to the same plaintext.
        unseal_config_secrets(&mut config, dir.path(), &store).unwrap();
        assert_eq!(
            config.secrets.get("DOCKERHUB_TOKEN").map(String::as_str),
            Some("abc123")
        );
        assert_eq!(
            config.repo_secrets["owner/repo"]
                .get("REPO_ONLY")
                .map(String::as_str),
            Some("xyz789")
        );
        assert_eq!(
            config.env_secrets["owner/repo"]["prod"]
                .get("DEPLOY_KEY")
                .map(String::as_str),
            Some("env-secret")
        );
    }

    /// Legacy configs held plaintext. They must keep loading, and the next
    /// write must re-seal them — no silent loss, no plaintext left behind.
    #[test]
    fn legacy_plaintext_secrets_are_read_and_resealed() {
        let dir = tempfile::tempdir().unwrap();
        // Simulate a legacy file: plaintext values, no key ever generated.
        let plain_text = toml::to_string_pretty(&populated_config()).unwrap();
        assert!(!plain_text.contains(SEALED_SECRET_PREFIX));
        // Loading legacy plaintext never touches the key backend.
        let mut config: ConfigFile = toml::from_str(&plain_text).unwrap();
        unseal_config_secrets(&mut config, dir.path(), &UnavailableCredentialStore).unwrap();
        assert_eq!(
            config.secrets.get("DOCKERHUB_TOKEN").map(String::as_str),
            Some("abc123")
        );
        // The next write seals them.
        let store = MemoryCredentialStore::default();
        seal_config_secrets(&mut config, dir.path(), &store, &[]).unwrap();
        let sealed_text = toml::to_string_pretty(&config).unwrap();
        assert!(
            !sealed_text.contains("abc123"),
            "still plaintext: {sealed_text}"
        );
        assert!(sealed_text.contains(SEALED_SECRET_PREFIX));
    }

    #[test]
    fn credential_store_read_errors_are_not_treated_as_missing() {
        let dir = tempfile::tempdir().unwrap();
        let error =
            resolve_config_sealing_key_with_store(dir.path(), &WriteOnlyCredentialStore, &[])
                .expect_err("unreadable credential store must fail closed");
        assert!(
            error.to_string().contains("reading config-secrets key"),
            "unexpected error: {error:#}"
        );
        assert!(
            !dir.path().join(SECRETS_KEY_FILE).exists(),
            "read failure generated a fallback key"
        );
    }

    /// Retired `secrets.key.orphan-*` files in `dir`.
    fn orphan_key_files(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("secrets.key.orphan-"))
            })
            .collect()
    }

    /// `populated_config`, sealed under the key `store` holds for `dir`.
    fn sealed_under(dir: &Path, store: &MemoryCredentialStore) -> ConfigFile {
        let mut config = populated_config();
        seal_config_secrets(&mut config, dir, store, &[]).unwrap();
        config
    }

    /// A locked store with sealed values and no `secrets.key` fails the load.
    /// Minting here would replace the only key that opens the secrets.
    #[test]
    fn locked_store_never_mints_over_sealed_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = sealed_under(dir.path(), &MemoryCredentialStore::default());
        let error =
            unseal_config_secrets(&mut sealed.clone(), dir.path(), &UnavailableCredentialStore)
                .expect_err("a locked store must not yield a fresh key");
        let message = format!("{error:#}");
        assert!(
            message.contains("sealed with the config-secrets key"),
            "{message}"
        );
        assert!(message.contains("which is unavailable"), "{message}");
        assert!(message.contains("unlock"), "{message}");
        assert!(message.contains("No key was minted"), "{message}");
        assert!(
            !dir.path().join(SECRETS_KEY_FILE).exists(),
            "a key file was written over sealed secrets"
        );
    }

    /// The write path sees sealed values only on disk, so a locked store must
    /// stop the mint there as well.
    #[test]
    fn locked_store_never_mints_on_the_write_path() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = sealed_under(dir.path(), &MemoryCredentialStore::default());
        let on_disk = sealed_envelopes(&sealed);
        assert_eq!(on_disk.len(), 3, "expected one sealed value per scope");
        let mut config = populated_config();
        let error = seal_config_secrets(
            &mut config,
            dir.path(),
            &UnavailableCredentialStore,
            &on_disk,
        )
        .expect_err("a locked store must not seal under a fresh key");
        assert!(
            format!("{error:#}").contains("No key was minted"),
            "{error:#}"
        );
        assert!(!dir.path().join(SECRETS_KEY_FILE).exists());
    }

    /// A genuinely fresh home has no sealed values, so a locked store still
    /// gets the legacy file key.
    #[test]
    fn fresh_home_with_a_locked_store_mints_the_file_key() {
        let dir = tempfile::tempdir().unwrap();
        let key =
            resolve_config_sealing_key_with_store(dir.path(), &UnavailableCredentialStore, &[])
                .unwrap();
        assert_eq!(
            read_sealing_key_file(&dir.path().join(SECRETS_KEY_FILE)).unwrap(),
            Some(key)
        );
    }

    /// A reachable store with no entry is no reason to mint either: the sealed
    /// values would become unrecoverable.
    #[test]
    fn reachable_store_without_an_entry_never_mints_over_sealed_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = sealed_under(dir.path(), &MemoryCredentialStore::default());
        let store = MemoryCredentialStore::default();
        let error = unseal_config_secrets(&mut sealed.clone(), dir.path(), &store)
            .expect_err("an empty store must not yield a fresh key");
        assert!(format!("{error:#}").contains("does not hold"), "{error:#}");
        let reference = config_secrets_key_reference(dir.path()).unwrap();
        assert!(
            store.get(&reference).unwrap().is_none(),
            "a key was minted into the store"
        );
        assert!(!dir.path().join(SECRETS_KEY_FILE).exists());
    }

    /// A key file that opens nothing is neither migrated into an empty store nor
    /// retired while the right key may still be locked away.
    #[test]
    fn unopened_key_file_is_kept_out_of_an_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = sealed_under(dir.path(), &MemoryCredentialStore::default());
        let key_path = dir.path().join(SECRETS_KEY_FILE);
        write_sealing_key_file(&key_path, &[0x22; 32]).unwrap();
        let store = MemoryCredentialStore::default();
        let error = unseal_config_secrets(&mut sealed.clone(), dir.path(), &store)
            .expect_err("a key file that opens nothing must not be trusted");
        assert!(format!("{error:#}").contains("does not open"), "{error:#}");
        assert!(key_path.exists(), "the key file was retired");
        let reference = config_secrets_key_reference(dir.path()).unwrap();
        assert!(
            store.get(&reference).unwrap().is_none(),
            "the key file was migrated"
        );
    }

    /// `secrets.key` from an earlier build disagrees with the store, and the
    /// store's key opens the sealed secrets: the store wins and the file is
    /// renamed aside, never deleted.
    #[test]
    fn orphan_key_file_is_renamed_aside_when_the_store_key_opens_the_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryCredentialStore::default();
        let sealed = sealed_under(dir.path(), &store);
        let orphan_key = [0x22; 32];
        let key_path = dir.path().join(SECRETS_KEY_FILE);
        write_sealing_key_file(&key_path, &orphan_key).unwrap();

        let mut loaded = sealed;
        unseal_config_secrets(&mut loaded, dir.path(), &store).unwrap();
        assert_eq!(loaded.secrets["DOCKERHUB_TOKEN"], "abc123");
        assert!(!key_path.exists(), "the orphan key file was not retired");
        let orphans = orphan_key_files(dir.path());
        assert_eq!(
            orphans.len(),
            1,
            "expected one retired key file: {orphans:?}"
        );
        assert_eq!(
            std::fs::read_to_string(&orphans[0]).unwrap(),
            hex::encode(orphan_key),
            "the retired file must keep its key"
        );
    }

    /// The store's key does not open the sealed secrets but `secrets.key` does:
    /// the file key is used, and it stays where it is.
    #[test]
    fn file_key_is_used_when_the_store_key_does_not_open_the_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let file_key = [0x33; 32];
        let reference = config_secrets_key_reference(dir.path()).unwrap();
        let sealing = MemoryCredentialStore::default();
        sealing
            .set(&reference, &SecretString::new(hex::encode(file_key)))
            .unwrap();
        let sealed = sealed_under(dir.path(), &sealing);

        let store = MemoryCredentialStore::default();
        store
            .set(&reference, &SecretString::new(hex::encode([0x44; 32])))
            .unwrap();
        let key_path = dir.path().join(SECRETS_KEY_FILE);
        write_sealing_key_file(&key_path, &file_key).unwrap();

        let mut loaded = sealed;
        unseal_config_secrets(&mut loaded, dir.path(), &store).unwrap();
        assert_eq!(loaded.secrets["DOCKERHUB_TOKEN"], "abc123");
        assert!(
            key_path.exists(),
            "the key file that opens the secrets was retired"
        );
        assert!(orphan_key_files(dir.path()).is_empty());
    }

    /// With nothing sealed, neither key is depended on: the store's key is the
    /// key of record and the superseded file is retired.
    #[test]
    fn conflicting_keys_without_sealed_values_prefer_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryCredentialStore::default();
        let reference = config_secrets_key_reference(dir.path()).unwrap();
        let store_key = [0x11; 32];
        let file_key = [0x22; 32];
        store
            .set(&reference, &SecretString::new(hex::encode(store_key)))
            .unwrap();
        let key_path = dir.path().join(SECRETS_KEY_FILE);
        write_sealing_key_file(&key_path, &file_key).unwrap();

        assert_eq!(
            resolve_config_sealing_key_with_store(dir.path(), &store, &[]).unwrap(),
            store_key
        );
        assert!(
            !key_path.exists(),
            "the superseded key file was not retired"
        );
        let orphans = orphan_key_files(dir.path());
        assert_eq!(
            orphans.len(),
            1,
            "expected one retired key file: {orphans:?}"
        );
        assert_eq!(
            std::fs::read_to_string(&orphans[0]).unwrap(),
            hex::encode(file_key)
        );
    }

    /// Neither source opens the sealed secrets: the conflict is an error that
    /// names both, and nothing is renamed or minted.
    #[test]
    fn conflicting_keys_that_neither_open_the_secrets_fail_naming_both() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = sealed_under(dir.path(), &MemoryCredentialStore::default());
        let store = MemoryCredentialStore::default();
        let reference = config_secrets_key_reference(dir.path()).unwrap();
        store
            .set(&reference, &SecretString::new(hex::encode([0x11; 32])))
            .unwrap();
        let key_path = dir.path().join(SECRETS_KEY_FILE);
        write_sealing_key_file(&key_path, &[0x22; 32]).unwrap();

        let error = unseal_config_secrets(&mut sealed.clone(), dir.path(), &store)
            .expect_err("conflicting key sources must not silently choose one");
        let message = format!("{error:#}");
        assert!(message.contains("key conflict"), "{message}");
        assert!(message.contains("neither opens"), "{message}");
        assert!(message.contains(SECRETS_KEY_FILE), "{message}");
        assert!(
            key_path.exists(),
            "an unresolved conflict retired the key file"
        );
        assert!(orphan_key_files(dir.path()).is_empty());
    }

    /// The write path reads sealed values from raw text, so a damaged file still
    /// counts. Plaintext that merely starts with the prefix does not.
    #[test]
    fn sealed_values_are_found_in_raw_text_even_when_the_file_is_damaged() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = sealed_under(dir.path(), &MemoryCredentialStore::default());
        let mut text = toml::to_string_pretty(&sealed).unwrap();
        text.push_str("\n[damaged\nTOKEN = \"sealed:v1:not-a-ciphertext\"\n");
        let mut found = sealed_envelopes_in_text(&text);
        let mut expected = sealed_envelopes(&sealed);
        found.sort();
        expected.sort();
        assert_eq!(found.len(), 3);
        assert_eq!(found, expected);
    }

    /// A keychain stand-in that locks mid-test: every operation fails while locked.
    #[derive(Clone, Default)]
    struct LockableStore {
        inner: MemoryCredentialStore,
        locked: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl LockableStore {
        fn lock(&self) {
            self.locked.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn check(&self) -> anyhow::Result<()> {
            if self.locked.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("keychain is locked");
            }
            Ok(())
        }
    }

    impl CredentialStore for LockableStore {
        fn get(&self, reference: &CredentialRef) -> anyhow::Result<Option<SecretString>> {
            self.check()?;
            self.inner.get(reference)
        }

        fn set(&self, reference: &CredentialRef, value: &SecretString) -> anyhow::Result<()> {
            self.check()?;
            self.inner.set(reference, value)
        }

        fn delete(&self, reference: &CredentialRef) -> anyhow::Result<()> {
            self.check()?;
            self.inner.delete(reference)
        }

        fn name(&self) -> &'static str {
            "lockable keychain stand-in"
        }

        fn available(&self) -> anyhow::Result<()> {
            self.check()
        }
    }

    /// Regression (#655 follow-up, P1): setup falls back to the file store after
    /// the keychain locks. The config write that records the switch must carry
    /// the sealing key this process resolved into the file store. Failing instead
    /// would strand the App key the fallback just stored.
    #[test]
    fn fallback_to_the_file_store_carries_the_resolved_sealing_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let keychain = LockableStore::default();
        let config = ConfigFile {
            credential_store: Some("os".to_owned()),
            secrets: BTreeMap::from([("DOCKERHUB_TOKEN".to_owned(), "abc123".to_owned())]),
            ..ConfigFile::default()
        };
        // Sealing mints the key into the keychain; loading the file resolves it.
        write_sealed_config(&path, &config, &keychain).unwrap();
        let mut loaded: ConfigFile =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        unseal_config_secrets(&mut loaded, dir.path(), &keychain).unwrap();
        assert_eq!(loaded.secrets["DOCKERHUB_TOKEN"], "abc123");

        // The keychain locks mid-setup. The new App key falls back to the file
        // store, and the setting flips the way `persist_secret` flips it.
        keychain.lock();
        let file = FileCredentialStore::new(credential_store_dir(&path));
        let pem_ref = github_reference("app-pem", Some("123")).unwrap();
        let pem = "-----BEGIN RSA PRIVATE KEY-----\nAPP-KEY\n-----END RSA PRIVATE KEY-----\n";
        let mut writer =
            CredentialWriter::with_fallback(std::sync::Arc::new(keychain.clone()), file.clone());
        let written = writer.write(&pem_ref, &SecretString::new(pem)).unwrap();
        assert!(
            matches!(written, Written::FileFallback { .. }),
            "{written:?}"
        );
        loaded.credential_store = Some("file".to_owned());
        loaded.github.app_pem_ref = Some(pem_ref.as_str().to_owned());

        // The write that records the switch succeeds: it carries the sealing key
        // into the file store, then references the App key.
        write_sealed_config(&path, &loaded, &file)
            .expect("the switch to the file store must not strand the App key");
        assert_eq!(
            file.get(&pem_ref)
                .unwrap()
                .map(|secret| secret.expose().to_owned()),
            Some(pem.to_owned())
        );
        let mut reread: ConfigFile =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(reread.credential_store.as_deref(), Some("file"));
        assert_eq!(reread.github.app_pem_ref.as_deref(), Some(pem_ref.as_str()));
        // The file store alone opens the secrets now, and no key file was minted.
        unseal_config_secrets(&mut reread, dir.path(), &file).unwrap();
        assert_eq!(reread.secrets["DOCKERHUB_TOKEN"], "abc123");
        assert!(!dir.path().join(SECRETS_KEY_FILE).exists());
    }

    /// Regression (#655 follow-up, P2): a commented-out ciphertext sealed under
    /// another key is not a sealed value. Counting it made both keys look like
    /// they opened the secrets, so an ordinary config update failed as an
    /// ambiguous conflict.
    #[test]
    fn commented_out_ciphertext_is_not_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let store = MemoryCredentialStore::default();
        write_sealed_config(&path, &populated_config(), &store).unwrap();
        // An old value under another key, kept as a comment and in an unrelated table.
        let other_key = [0x66; 32];
        let old = seal_secret_value(&other_key, "retired-token").unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(
            &path,
            format!("{text}\n# OLD_TOKEN = \"{old}\"\n\n[notes]\nold = \"{old}\"\n"),
        )
        .unwrap();
        // A secrets.key from an earlier build holds the other key: the sources conflict.
        write_sealing_key_file(&dir.path().join(SECRETS_KEY_FILE), &other_key).unwrap();

        assert_eq!(
            sealed_envelopes_on_disk(&path).unwrap().len(),
            3,
            "only the real secrets are evidence"
        );
        // The store's key opens the real secrets, so it wins and the superseded
        // file is renamed aside.
        write_sealed_config(&path, &populated_config(), &store)
            .expect("a commented-out ciphertext must not make the conflict ambiguous");
        assert!(!dir.path().join(SECRETS_KEY_FILE).exists());
        assert_eq!(orphan_key_files(dir.path()).len(), 1);
    }

    /// Evidence is read from the secret tables of valid TOML, so it matches the
    /// sealed values in every scope and ignores comments and unrelated tables.
    #[test]
    fn sealed_evidence_comes_from_the_secret_tables_of_valid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = sealed_under(dir.path(), &MemoryCredentialStore::default());
        let mut text = toml::to_string_pretty(&sealed).unwrap();
        let stray = seal_secret_value(&[0x77; 32], "value").unwrap();
        text.push_str(&format!(
            "# OLD = \"{stray}\"\n[notes]\nold = \"{stray}\"\n"
        ));
        let mut found = sealed_envelopes_in_text(&text);
        let mut expected = sealed_envelopes(&sealed);
        found.sort();
        expected.sort();
        assert_eq!(found.len(), 3, "one sealed value per scope");
        assert_eq!(found, expected);
    }

    #[test]
    fn plaintext_marker_prefix_is_not_misclassified_as_ciphertext() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryCredentialStore::default();
        let mut config = ConfigFile {
            secrets: BTreeMap::from([(
                "TOKEN".to_owned(),
                format!("{SEALED_SECRET_PREFIX}not-a-ciphertext"),
            )]),
            ..ConfigFile::default()
        };

        unseal_config_secrets(&mut config, dir.path(), &UnavailableCredentialStore).unwrap();
        assert_eq!(
            config.secrets["TOKEN"],
            format!("{SEALED_SECRET_PREFIX}not-a-ciphertext")
        );
        seal_config_secrets(&mut config, dir.path(), &store, &[]).unwrap();
        assert_ne!(
            config.secrets["TOKEN"],
            format!("{SEALED_SECRET_PREFIX}not-a-ciphertext")
        );
        let mut restored = config.clone();
        unseal_config_secrets(&mut restored, dir.path(), &store).unwrap();
        assert_eq!(
            restored.secrets["TOKEN"],
            format!("{SEALED_SECRET_PREFIX}not-a-ciphertext")
        );
    }

    /// The key is stable per config directory and distinct across directories.
    #[test]
    fn sealing_key_is_stable_per_directory() {
        let store = MemoryCredentialStore::default();
        let dir = tempfile::tempdir().unwrap();
        let first = resolve_config_sealing_key_with_store(dir.path(), &store, &[]).unwrap();
        let second = resolve_config_sealing_key_with_store(dir.path(), &store, &[]).unwrap();
        assert_eq!(first, second, "key changed between resolutions");
        let other = tempfile::tempdir().unwrap();
        let other_key = resolve_config_sealing_key_with_store(other.path(), &store, &[]).unwrap();
        assert_ne!(first, other_key, "directories share a key");
    }

    /// Headless fallback: the key file is created 0600 and reused.
    #[test]
    fn sealing_key_file_fallback_is_private_and_stable() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join(SECRETS_KEY_FILE);
        assert!(!key_path.exists());
        let first =
            resolve_config_sealing_key_with_store(dir.path(), &UnavailableCredentialStore, &[])
                .unwrap();
        assert!(key_path.exists(), "key file was not created");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&key_path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "key file must be 0600");
        }
        let second =
            resolve_config_sealing_key_with_store(dir.path(), &UnavailableCredentialStore, &[])
                .unwrap();
        assert_eq!(first, second, "key file was not reused");
        // A file key migrates into the store when one becomes reachable, and
        // the file is retired so the key cannot silently fork.
        let store = MemoryCredentialStore::default();
        let migrated = resolve_config_sealing_key_with_store(dir.path(), &store, &[]).unwrap();
        assert_eq!(first, migrated, "migration changed the key");
        assert!(!key_path.exists(), "stale key file was not retired");
    }

    /// Ciphertext sealed under one key must fail closed under another — the
    /// HMAC rejects it instead of yielding garbage.
    #[test]
    fn sealed_values_fail_closed_on_wrong_key() {
        let store_a = MemoryCredentialStore::default();
        let store_b = MemoryCredentialStore::default();
        let dir = tempfile::tempdir().unwrap();
        let mut config = populated_config();
        seal_config_secrets(&mut config, dir.path(), &store_a, &[]).unwrap();
        let result = unseal_config_secrets(&mut config, dir.path(), &store_b);
        assert!(result.is_err(), "unsealed with the wrong key");
    }

    /// Config writes without secrets (e.g. `preloop setup`) must not create
    /// key material or touch the credential store as a side effect.
    #[test]
    fn secretless_writes_never_touch_the_key_backend() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = ConfigFile::default();
        seal_config_secrets(&mut config, dir.path(), &UnavailableCredentialStore, &[]).unwrap();
        assert!(
            !dir.path().join(SECRETS_KEY_FILE).exists(),
            "key file created for a secretless config"
        );
    }

    /// The `credential_store` key is written and read back, and the unset default stays out of the file.
    #[test]
    fn credential_store_key_survives_a_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let config = ConfigFile {
            credential_store: Some("file".into()),
            ..ConfigFile::default()
        };
        write_config_to(&path, &config).unwrap();
        assert_eq!(
            credential_store_setting(&path).unwrap().as_deref(),
            Some("file")
        );
        assert_eq!(
            load_config_from(&path).unwrap().credential_store.as_deref(),
            Some("file")
        );
        write_config_to(&path, &ConfigFile::default()).unwrap();
        assert_eq!(credential_store_setting(&path).unwrap(), None);
    }

    /// The store key is read on its own, so a reference that cannot resolve
    /// cannot hide it. The full load does resolve references, so it still fails.
    #[test]
    fn credential_store_key_is_read_without_resolving_references() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "credential_store = \"file\"\n[github]\napp_pem_ref = \"not a valid reference!\"\n",
        )
        .unwrap();
        assert_eq!(
            credential_store_setting(&path).unwrap().as_deref(),
            Some("file")
        );
        assert!(load_config_from(&path).is_err());
        assert_eq!(
            credential_store_setting(&dir.path().join("missing.toml")).unwrap(),
            None
        );
    }

    #[test]
    fn unknown_credential_store_fails_the_config_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "credential_store = \"keychain\"\n").unwrap();
        let error = format!("{:#}", load_config_from(&path).unwrap_err());
        assert!(error.contains("credential_store"), "{error}");
        assert!(error.contains("\"file\" or \"os\""), "{error}");
    }

    /// A PEM held by the file store reads back through the normal config load
    /// byte-for-byte, with the file store selected by default.
    #[tokio::test]
    async fn pem_in_the_file_store_loads_through_the_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA\nabc/def+ghi=\n-----END RSA PRIVATE KEY-----\n";
        let pem_ref = github_reference("app-pem", Some("123")).unwrap();
        crate::credential_store::FileCredentialStore::new(
            crate::credential_store::FileCredentialStore::default_dir(dir.path()),
        )
        .set(&pem_ref, &SecretString::new(pem))
        .unwrap();
        let config = ConfigFile {
            github: GitHubConfig {
                app_id: Some("123".into()),
                app_pem_ref: Some(pem_ref.as_str().to_owned()),
                ..Default::default()
            },
            ..ConfigFile::default()
        };
        let path = dir.path().join("config.toml");
        write_config_to(&path, &config).unwrap();
        let loaded = load_config_from_with(&path, None).unwrap();
        assert_eq!(loaded.github.app_pem(), Some(pem));
        assert_eq!(loaded.credential_store, None);
    }
    /// A setup fallback remains readable when the explicit environment
    /// override still selects the OS-primary store on the next load.
    #[tokio::test]
    async fn os_env_reads_pem_and_pat_from_the_file_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let credentials = FileCredentialStore::new(credential_store_dir(&config_path));
        let pem_ref = github_reference("app-pem", Some("777")).unwrap();
        let pat_ref = github_reference("pat", None).unwrap();
        let pem = "-----BEGIN RSA PRIVATE KEY-----\nFALLBACK-PEM\n-----END RSA PRIVATE KEY-----\n";
        credentials.set(&pem_ref, &SecretString::new(pem)).unwrap();
        credentials
            .set(&pat_ref, &SecretString::new("fallback-pat"))
            .unwrap();

        let config = ConfigFile {
            credential_store: Some("file".to_owned()),
            github: GitHubConfig {
                app_id: Some("777".to_owned()),
                app_pem_ref: Some(pem_ref.as_str().to_owned()),
                pat_ref: Some(pat_ref.as_str().to_owned()),
                ..Default::default()
            },
            ..ConfigFile::default()
        };
        write_config_to(&config_path, &config).unwrap();

        let loaded = load_config_from_with(&config_path, Some("os")).unwrap();
        assert_eq!(loaded.github.app_pem(), Some(pem));
        assert_eq!(loaded.github.pat(), Some("fallback-pat"));
    }

    /// Writers and readers use the config's parent when `PRELOOP_CONFIG` is
    /// outside `PRELOOP_HOME`, for both App PEMs and PATs.
    #[tokio::test]
    async fn explicit_config_path_uses_its_sibling_credentials_directory() {
        let home = tempfile::tempdir().unwrap();
        // The engine home is an explicit input now: it is what a resolved
        // `PRELOOP_HOME` becomes at the startup boundary. A writer keyed to a
        // config file elsewhere must not place credentials under it.
        let _home_scope = crate::credential_store::engine_token_dir(home.path(), Some(home.path()));
        let config_root = tempfile::tempdir().unwrap();
        let config_path = config_root.path().join("config.toml");
        let mut writer = CredentialWriter::for_config(&config_path, None, None).unwrap();
        let pem_ref = github_reference("app-pem", Some("888")).unwrap();
        let pat_ref = github_reference("pat", None).unwrap();
        writer
            .write(&pem_ref, &SecretString::new("external-pem"))
            .unwrap();
        writer
            .write(&pat_ref, &SecretString::new("external-pat"))
            .unwrap();

        let config = ConfigFile {
            github: GitHubConfig {
                app_id: Some("888".to_owned()),
                app_pem_ref: Some(pem_ref.as_str().to_owned()),
                pat_ref: Some(pat_ref.as_str().to_owned()),
                ..Default::default()
            },
            ..ConfigFile::default()
        };
        write_config_to(&config_path, &config).unwrap();
        assert!(credential_store_dir(&config_path).exists());
        assert!(!home.path().join("credentials").exists());

        let loaded = load_config_from_with(&config_path, None).unwrap();
        assert_eq!(loaded.github.app_pem(), Some("external-pem"));
        assert_eq!(loaded.github.pat(), Some("external-pat"));
    }

    /// Sealed secrets survive the move to the file store: a key an earlier
    /// build kept in the OS store is found, copied, and reused, not replaced.
    #[test]
    fn sealing_key_is_migrated_from_a_legacy_os_entry() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = MemoryCredentialStore::default();
        let reference = config_secrets_key_reference(dir.path()).unwrap();
        let key = [0x5a; 32];
        legacy
            .set(&reference, &SecretString::new(hex::encode(key)))
            .unwrap();
        let store = crate::credential_store::FileCredentialStore::new(
            crate::credential_store::FileCredentialStore::default_dir(dir.path()),
        )
        .migrating_from(std::sync::Arc::new(legacy));
        assert_eq!(
            resolve_config_sealing_key_with_store(dir.path(), &store, &[]).unwrap(),
            key
        );
        assert!(!dir.path().join(SECRETS_KEY_FILE).exists());
    }

    /// A store that reads a value but cannot keep one — the shape a failed
    /// OS-to-file migration copy leaves behind.
    struct ReadOnlyStore {
        value: String,
    }

    impl CredentialStore for ReadOnlyStore {
        fn get(&self, _reference: &CredentialRef) -> anyhow::Result<Option<SecretString>> {
            Ok(Some(SecretString::new(self.value.clone())))
        }

        fn set(&self, _reference: &CredentialRef, _value: &SecretString) -> anyhow::Result<()> {
            anyhow::bail!("credentials directory is not writable")
        }

        fn delete(&self, _reference: &CredentialRef) -> anyhow::Result<()> {
            Ok(())
        }

        fn name(&self) -> &'static str {
            "read-only store"
        }
    }

    /// A durable copy retires the legacy `secrets.key`: the store writes the
    /// key and reads it back before the file goes away.
    #[test]
    fn secrets_key_file_is_retired_once_the_store_confirms_the_copy() {
        let dir = tempfile::tempdir().unwrap();
        let key = [0x5a; 32];
        let key_path = dir.path().join(SECRETS_KEY_FILE);
        write_sealing_key_file(&key_path, &key).unwrap();
        let legacy = MemoryCredentialStore::default();
        let reference = config_secrets_key_reference(dir.path()).unwrap();
        legacy
            .set(&reference, &SecretString::new(hex::encode(key)))
            .unwrap();
        let store = FileCredentialStore::new(dir.path().join("credentials"))
            .migrating_from(std::sync::Arc::new(legacy));
        assert_eq!(
            resolve_config_sealing_key_with_store(dir.path(), &store, &[]).unwrap(),
            key
        );
        assert!(
            !key_path.exists(),
            "a confirmed durable copy may retire the legacy file"
        );
    }

    /// A store that cannot keep the copy must not retire `secrets.key`: the
    /// file is then the only durable way to unseal the config secrets.
    #[test]
    fn secrets_key_file_survives_a_store_that_cannot_keep_the_copy() {
        let dir = tempfile::tempdir().unwrap();
        let key = [0x5a; 32];
        let key_path = dir.path().join(SECRETS_KEY_FILE);
        write_sealing_key_file(&key_path, &key).unwrap();
        let store = ReadOnlyStore {
            value: hex::encode(key),
        };
        assert_eq!(
            resolve_config_sealing_key_with_store(dir.path(), &store, &[]).unwrap(),
            key
        );
        assert!(
            key_path.exists(),
            "an unconfirmed copy must leave the legacy key file in place"
        );
    }
}
