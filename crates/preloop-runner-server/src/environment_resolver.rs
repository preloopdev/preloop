//! Environment protection rules resolved from GitHub.
//!
//! When a GitHub App (or `PRELOOP_GITHUB_TOKEN`) is configured, the protection
//! rules for a job's `environment:` live on the repository, not in preloop's
//! config: `GET /repos/{o}/{r}/environments/{name}` carries `wait_timer`,
//! `required_reviewers` (+ `prevent_self_review`) and the branch-policy mode,
//! `GET .../deployment-branch-policies` carries the fnmatch patterns, and
//! `GET .../deployment_protection_rules` carries enabled custom protection
//! rules (third-party Apps — enforced by failing the job, never bypassed).
//! The operator's `[environment_rules]` TOML table remains the source only
//! when no GitHub credential exists at all (local mode).
//!
//! Results are cached for [`RULES_TTL`]: GitHub-side rule edits propagate on
//! the next sweep tick rather than mid-evaluation. A repository's *source* is
//! probed once (`repo_sources`): the App that covers the repo decides where
//! its rules come from, and a repo no configured credential can reach keeps
//! its TOML rules. Environments GitHub has never heard of resolve to no
//! rules — GitHub auto-creates an environment the first time a workflow
//! references it, with zero protection
//! (https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments).
//!
//! Transactions cannot await, so the synchronous lookup
//! ([`EnvironmentResolver::lookup_sync`]) reads only cached state: a key that
//! has never resolved returns [`EnvironmentLookup::Pending`], which arms the
//! fail-closed gate and queues the key for the reaper's [`resolve`] pass —
//! the job holds until GitHub answers rather than deploying unprotected on a
//! cold cache.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::config::{EnvironmentReviewer, EnvironmentRules, EnvironmentRulesMap};

/// How long a fetched rule set stays fresh. Short enough that an operator
/// editing rules on GitHub sees the change on the same push cadence the
/// reaper already runs at (10s ticks), long enough to keep a matrix fan-out
/// to one request set per environment.
const RULES_TTL: Duration = Duration::from_secs(60);

/// Branch-policy pages walked per environment (100 patterns each); a paging
/// bug cannot spin the resolver forever.
const MAX_POLICY_PAGES: u32 = 10;
/// Protected-branch pages walked for `protected_branches` policy expansion.
const MAX_PROTECTED_PAGES: u32 = 10;
/// Team-member pages walked per reviewer team.
const MAX_MEMBER_PAGES: u32 = 10;
/// Team membership stays cached this long; reviewer-set churn is rare.
const MEMBERS_TTL: Duration = Duration::from_secs(300);

/// What the synchronous gate path knows about one `(repo, environment)`.
#[derive(Debug, Clone)]
pub enum EnvironmentLookup {
    /// Rules resolved — `None` when the environment has no protection rules
    /// (or does not exist on GitHub, which auto-creates it unprotected).
    Resolved(Option<Arc<EnvironmentRules>>),
    /// The repo is GitHub-sourced but this environment has never been
    /// fetched; the gate must hold (fail closed) until `resolve` completes.
    Pending,
}

/// Where a repository's environment rules come from, probed once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RulesSource {
    /// No GitHub credential covers the repo: the operator TOML applies.
    Toml,
    /// A GitHub App or PAT covers the repo: GitHub's environments API.
    Github,
}

struct ResolvedEntry {
    rules: Arc<EnvironmentRules>,
    fetched_at: Instant,
}

/// Rule resolution with a TTL cache over GitHub's environments API.
///
/// `Default` yields the pure-local resolver: no GitHub credential is ever
/// probed and `lookup_sync` answers from `toml` alone, which is what the
/// control backends hold until bootstrap installs the real one.
pub struct EnvironmentResolver {
    /// `[environment_rules]` from the config file — the fallback for
    /// repositories no GitHub credential covers, and the whole story in
    /// local mode.
    toml: EnvironmentRulesMap,
    /// Whether any GitHub App is registered. Set once at boot from
    /// `AppState`'s registry; flips `lookup_sync` misses from "TOML" to
    /// "pending fetch" for unprobed repos.
    github_configured: AtomicBool,
    /// Per-repo source decision, filled by `resolve`.
    repo_sources: parking_lot::RwLock<HashMap<String, RulesSource>>,
    /// GitHub-fetched rules, `(repo, env)` → entry.
    entries: parking_lot::RwLock<HashMap<(String, String), ResolvedEntry>>,
    /// Keys awaiting a fetch (first-seen misses + refresh failures). The
    /// reaper drains this every tick.
    pending: parking_lot::Mutex<BTreeSet<(String, String)>>,
    /// In-flight fetches, one per key, so concurrent submissions for the
    /// same environment collapse onto one API round-trip.
    inflight: tokio::sync::Mutex<HashMap<(String, String), Arc<tokio::sync::Mutex<()>>>>,
    /// `(org, team_slug)` → member logins, for reviewer authorization.
    team_members: parking_lot::RwLock<HashMap<(String, String), (Instant, Vec<String>)>>,
}

impl EnvironmentResolver {
    /// The resolver `AppState` installs: TOML fallback plus GitHub lookups.
    /// `github_configured` is whether any App (or PAT) is registered — it is
    /// recomputed on every `resolve` too, so late-set registries still work.
    pub fn new(toml: EnvironmentRulesMap) -> Self {
        Self {
            toml,
            github_configured: AtomicBool::new(false),
            repo_sources: parking_lot::RwLock::new(HashMap::new()),
            entries: parking_lot::RwLock::new(HashMap::new()),
            pending: parking_lot::Mutex::new(BTreeSet::new()),
            inflight: tokio::sync::Mutex::new(HashMap::new()),
            team_members: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// A resolver pinned to the TOML map — tests and pure-local operation.
    pub fn local(toml: EnvironmentRulesMap) -> Arc<Self> {
        Arc::new(Self::new(toml))
    }

    /// Record that GitHub credentials exist (`AppState` calls this after the
    /// registry loads). After it, a miss on an unprobed repo is `Pending`,
    /// never silently TOML.
    pub fn set_github_configured(&self) {
        self.github_configured.store(true, Ordering::Release);
    }

    /// The TOML fallback for one environment (the `Option` distinguishes
    /// "no table" from "empty table" — both mean no gates, identically).
    fn toml_rules(&self, repository: &str, environment: &str) -> Option<Arc<EnvironmentRules>> {
        self.toml
            .get(repository)
            .and_then(|envs| envs.get(environment))
            .cloned()
            .map(Arc::new)
    }

    /// The sync lookup the gate evaluation uses inside transactions.
    ///
    /// Cached entries answer even when expired — the last-known rules are
    /// better than a `Pending` hold for an established environment — but the
    /// key is re-queued for the refresh pass so staleness stays bounded.
    /// `Pending` is only ever "never resolved", so `Pending` + a held job
    /// always resolves on the next reaper tick.
    pub fn lookup_sync(&self, repository: &str, environment: &str) -> EnvironmentLookup {
        let key = (repository.to_owned(), environment.to_owned());
        if let Some(entry) = self
            .entries
            .read()
            .get(&key)
            .map(|entry| (entry.rules.clone(), entry.fetched_at))
        {
            if entry.1.elapsed() > RULES_TTL {
                self.pending.lock().insert(key);
            }
            return EnvironmentLookup::Resolved(Some(entry.0));
        }
        match self.repo_sources.read().get(repository).copied() {
            Some(RulesSource::Github) => {
                self.pending.lock().insert(key);
                EnvironmentLookup::Pending
            }
            Some(RulesSource::Toml) => {
                EnvironmentLookup::Resolved(self.toml_rules(repository, environment))
            }
            // Source unprobed. With no GitHub credentials configured there is
            // nothing to fetch — answer from TOML directly. With credentials,
            // `resolve` must probe the repo first, so hold as pending.
            None if self.github_configured.load(Ordering::Acquire) => {
                self.pending.lock().insert(key);
                EnvironmentLookup::Pending
            }
            None => EnvironmentLookup::Resolved(self.toml_rules(repository, environment)),
        }
    }

    /// Keys queued by `Pending` lookups or expired entries.
    pub fn pending_keys(&self) -> Vec<(String, String)> {
        self.pending.lock().iter().cloned().collect()
    }

    /// Resolve `(repo, environment)` rules, fetching from GitHub when the
    /// repo is covered by a configured credential.
    ///
    /// Fetch failures are retried by the caller (`refresh_stale`) — the key
    /// stays in `pending` so a held gate re-evaluates once GitHub answers.
    /// A `404` on the environment resolves to *no rules*: GitHub auto-creates
    /// referenced environments unprotected.
    pub async fn resolve(
        &self,
        shared: &crate::state::SharedState,
        repository: &str,
        environment: &str,
    ) -> anyhow::Result<Arc<EnvironmentRules>> {
        self.resolve_at(
            &crate::github::github_api_base(),
            shared,
            repository,
            environment,
        )
        .await
    }

    /// [`resolve`] against an explicit API base (tests).
    pub async fn resolve_at(
        &self,
        api_base: &str,
        shared: &crate::state::SharedState,
        repository: &str,
        environment: &str,
    ) -> anyhow::Result<Arc<EnvironmentRules>> {
        let key = (repository.to_owned(), environment.to_owned());
        if let Some(entry) = self.entries.read().get(&key)
            && entry.fetched_at.elapsed() <= RULES_TTL
        {
            return Ok(entry.rules.clone());
        }
        // Collapse concurrent resolutions for the same environment.
        let gate = {
            let mut inflight = self.inflight.lock().await;
            inflight.entry(key.clone()).or_default().clone()
        };
        let _guard = gate.lock().await;
        if let Some(entry) = self.entries.read().get(&key)
            && entry.fetched_at.elapsed() <= RULES_TTL
        {
            self.pending.lock().remove(&key);
            return Ok(entry.rules.clone());
        }

        let app = crate::github_app::select_app_for_repo(shared, repository).await;
        let pat = std::env::var("PRELOOP_GITHUB_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let result: anyhow::Result<Arc<EnvironmentRules>> = if let Some(creds) = &app {
            self.github_configured.store(true, Ordering::Release);
            self.repo_sources
                .write()
                .insert(repository.to_owned(), RulesSource::Github);
            let token = environment_token(creds, repository).await?;
            fetch_environment_rules(api_base, &token, repository, environment)
                .await
                .map(Arc::new)
        } else if let Some(token) = pat {
            self.github_configured.store(true, Ordering::Release);
            self.repo_sources
                .write()
                .insert(repository.to_owned(), RulesSource::Github);
            fetch_environment_rules(api_base, &token, repository, environment)
                .await
                .map(Arc::new)
        } else {
            // No credential covers this repository: the local TOML table is
            // the whole story (the brief's local-mode fallback).
            self.repo_sources
                .write()
                .insert(repository.to_owned(), RulesSource::Toml);
            Ok(self.toml_rules(repository, environment).unwrap_or_default())
        };
        match result {
            Ok(rules) => {
                self.entries.write().insert(
                    key.clone(),
                    ResolvedEntry {
                        rules: rules.clone(),
                        fetched_at: Instant::now(),
                    },
                );
                self.pending.lock().remove(&key);
                Ok(rules)
            }
            Err(error) => {
                // A resolved-but-stale entry keeps its last-known rules; a
                // never-resolved key stays pending so the gate holds.
                if self.entries.read().get(&key).is_none() {
                    self.pending.lock().insert(key);
                } else {
                    self.pending.lock().remove(&key);
                }
                Err(error)
            }
        }
    }

    /// Re-resolve every queued key (first-seen misses and expired entries).
    /// Called once per reaper tick, ahead of the gate sweep, so a `Pending`
    /// hold never outlives the fetch that unblocks it.
    pub async fn refresh_stale(&self, shared: &crate::state::SharedState) {
        // Expired resolved entries re-queue for refresh without becoming
        // `Pending` (stale rules keep answering in the meantime).
        let stale: Vec<(String, String)> = self
            .entries
            .read()
            .iter()
            .filter(|(_, entry)| entry.fetched_at.elapsed() > RULES_TTL)
            .map(|(key, _)| key.clone())
            .collect();
        let mut keys = self.pending_keys();
        keys.extend(stale);
        for (repo, env) in keys {
            if let Err(error) = self.resolve(shared, &repo, &env).await {
                tracing::warn!(
                    repository = %repo,
                    environment = %env,
                    %error,
                    "environment rules refresh failed; gate keeps last-known state"
                );
            }
        }
    }

    /// Whether `sender` may approve `environment`'s reviewer gate on behalf
    /// of `run_actor` — GitHub's `required_reviewers` set plus
    /// `prevent_self_review`, evaluated against freshly fetched rules (the
    /// caller `resolve`s first; this consults only cached state).
    pub async fn reviewer_authorized(
        &self,
        shared: &crate::state::SharedState,
        repository: &str,
        environment: &str,
        sender: &str,
        run_actor: &str,
    ) -> anyhow::Result<bool> {
        let EnvironmentLookup::Resolved(Some(rules)) = self.lookup_sync(repository, environment)
        else {
            // No reviewer rule resolved: nothing to authorize against.
            return Ok(false);
        };
        if rules.prevent_self_review && sender.eq_ignore_ascii_case(run_actor) {
            return Ok(false);
        }
        for reviewer in &rules.reviewers {
            match reviewer {
                EnvironmentReviewer::User(login) if login.eq_ignore_ascii_case(sender) => {
                    return Ok(true);
                }
                EnvironmentReviewer::Team { org, slug } => {
                    if self
                        .team_member(shared, repository, org, slug, sender)
                        .await
                        .unwrap_or(false)
                    {
                        return Ok(true);
                    }
                }
                _ => {}
            }
        }
        Ok(false)
    }

    /// Whether `sender` is a member of `(org, slug)`, expanding the team via
    /// `GET /orgs/{org}/teams/{slug}/members` under the token minted for the
    /// run's repository (`members:read` is an org permission an installation
    /// token keeps even when repo-scoped). Fetch failures fail closed.
    async fn team_member(
        &self,
        shared: &crate::state::SharedState,
        repository: &str,
        org: &str,
        slug: &str,
        sender: &str,
    ) -> anyhow::Result<bool> {
        let key = (org.to_owned(), slug.to_owned());
        if let Some((fetched_at, members)) = self.team_members.read().get(&key)
            && fetched_at.elapsed() <= MEMBERS_TTL
        {
            return Ok(members.iter().any(|m| m.eq_ignore_ascii_case(sender)));
        }
        // Team membership reads need the org-level `Members: read`
        // permission (docs.github.com/en/rest/authentication/
        // permissions-required-for-github-apps). An App mints it clamped to
        // the installation's grants; the PAT fallback is the operator's own
        // credential and needs no mint.
        let app = crate::github_app::select_app_for_repo(shared, repository).await;
        let token = if let Some(creds) = app {
            let mut permissions = std::collections::BTreeMap::new();
            permissions.insert("members".to_owned(), "read".to_owned());
            permissions.insert("contents".to_owned(), "read".to_owned());
            crate::github_app::get_or_mint_token(&creds, repository, &permissions).await?
        } else {
            std::env::var("PRELOOP_GITHUB_TOKEN")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!("no GitHub credential covers {repository} for team expansion")
                })?
        };
        let members =
            fetch_team_members(&crate::github::github_api_base(), &token, org, slug).await?;
        self.team_members
            .write()
            .insert(key, (Instant::now(), members.clone()));
        Ok(members.iter().any(|m| m.eq_ignore_ascii_case(sender)))
    }
}

/// Percent-encode one URL path segment (environment names are arbitrary
/// strings; `prod eu` must reach GitHub as `prod%20eu`).
fn url_path_segment(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, percent_encoding::NON_ALPHANUMERIC).to_string()
}

/// Mint a repository-scoped token able to read environments. `contents:read`
/// is the read floor every installation holds; GitHub's own GET endpoints
/// for environments, branch policies and protection rules need only repo
/// read access (docs list no finer-grained requirement for the GETs).
async fn environment_token(
    creds: &crate::github_app::GitHubAppCredentials,
    repository: &str,
) -> anyhow::Result<String> {
    let mut permissions = std::collections::BTreeMap::new();
    permissions.insert("contents".to_owned(), "read".to_owned());
    crate::github_app::get_or_mint_token(creds, repository, &permissions).await
}

/// GET `path` under the token, decoding JSON. `Ok(None)` on 404.
async fn github_get(api_base: &str, token: &str, path: &str) -> anyhow::Result<Option<Value>> {
    let res = crate::shared_http::CLIENT
        .get(format!("{api_base}{path}"))
        .header("User-Agent", "preloop")
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2026-03-10")
        .send()
        .await?;
    if res.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        anyhow::bail!("GET {path} failed with status {status}: {body}");
    }
    Ok(Some(res.json().await?))
}

/// Fetch and map an environment's full protection rule set.
///
/// Three reads: the environment record (protection rules + branch-policy
/// mode), the custom branch/tag name patterns (only when
/// `custom_branch_policies` is set — GitHub 404s the endpoint otherwise),
/// and the enabled custom deployment protection rules. `protected_branches`
/// mode expands to the repo's protected-branch names, matched exactly.
async fn fetch_environment_rules(
    api_base: &str,
    token: &str,
    repository: &str,
    environment: &str,
) -> anyhow::Result<EnvironmentRules> {
    let encoded_env = url_path_segment(environment);
    let env_path = format!("/repos/{repository}/environments/{encoded_env}");
    let Some(env_json) = github_get(api_base, token, &env_path).await? else {
        // GitHub auto-creates an environment the first time a workflow
        // references it, with no protection — so a 404 resolves to no rules.
        return Ok(EnvironmentRules::default());
    };

    let mut rules = EnvironmentRules {
        branch_policy_restricted: env_json
            .get("deployment_branch_policy")
            .is_some_and(|policy| !policy.is_null()),
        ..EnvironmentRules::default()
    };
    for rule in env_json
        .get("protection_rules")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        match rule.get("type").and_then(Value::as_str) {
            Some("wait_timer") => {
                rules.wait_timer_minutes =
                    rule.get("wait_timer").and_then(Value::as_u64).unwrap_or(0);
            }
            Some("required_reviewers") => {
                // GitHub semantics: one approval from the reviewer set
                // suffices — the rule is a *set*, not a quorum
                // (docs.github.com/en/rest/deployments/environments).
                rules.required_reviewers = 1;
                rules.prevent_self_review = rule
                    .get("prevent_self_review")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                rules.reviewers = rule
                    .get("reviewers")
                    .and_then(Value::as_array)
                    .map(|reviewers| reviewers.iter().filter_map(parse_reviewer).collect())
                    .unwrap_or_default();
            }
            _ => {}
        }
    }

    if rules.branch_policy_restricted {
        let policy = &env_json["deployment_branch_policy"];
        if policy
            .get("protected_branches")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            rules.protected_branches_only = true;
            rules.deployment_branches =
                fetch_protected_branches(api_base, token, repository).await?;
        } else if policy
            .get("custom_branch_policies")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            for policy in fetch_branch_policies(api_base, token, repository, &encoded_env).await? {
                match policy.get("type").and_then(Value::as_str) {
                    Some("branch") => rules
                        .deployment_branches
                        .push(policy["name"].as_str().unwrap_or_default().to_owned()),
                    Some("tag") => rules
                        .deployment_tags
                        .push(policy["name"].as_str().unwrap_or_default().to_owned()),
                    _ => {}
                }
            }
            rules.deployment_branches.retain(|name| !name.is_empty());
            rules.deployment_tags.retain(|name| !name.is_empty());
        }
    }

    // Custom deployment protection rules are third-party GitHub Apps GitHub
    // calls through `deployment_protection_rule` webhooks + callback tokens.
    // preloop cannot impersonate that contract, so enabled rules are recorded
    // and the gate fails the job closed with an explicit message.
    let rules_path = format!("{env_path}/deployment_protection_rules");
    if let Some(json) = github_get(api_base, token, &rules_path).await? {
        rules.custom_protection_rules = json
            .get("custom_deployment_protection_rules")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .filter(|rule| {
                        rule.get("enabled")
                            .and_then(Value::as_bool)
                            .unwrap_or(false)
                    })
                    .filter_map(|rule| {
                        rule.pointer("/app/slug")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .or_else(|| {
                                rule.get("id")
                                    .and_then(Value::as_u64)
                                    .map(|id| format!("id:{id}"))
                            })
                    })
                    .collect()
            })
            .unwrap_or_default();
    }
    Ok(rules)
}

/// One `required_reviewers` reviewer entry → our identity form.
fn parse_reviewer(value: &Value) -> Option<EnvironmentReviewer> {
    let kind = value.get("type").and_then(Value::as_str)?;
    let reviewer = value.get("reviewer")?;
    match kind {
        "User" => reviewer
            .get("login")
            .and_then(Value::as_str)
            .map(|login| EnvironmentReviewer::User(login.to_owned())),
        "Team" => {
            // Team reviewers carry `slug` plus their owning `organization`.
            let slug = reviewer.get("slug").and_then(Value::as_str)?.to_owned();
            let org = reviewer
                .pointer("/organization/login")
                .and_then(Value::as_str)
                .map(str::to_owned)?;
            Some(EnvironmentReviewer::Team { org, slug })
        }
        _ => None,
    }
}

/// Page through `GET .../environments/{env}/deployment-branch-policies`.
async fn fetch_branch_policies(
    api_base: &str,
    token: &str,
    repository: &str,
    encoded_env: &str,
) -> anyhow::Result<Vec<Value>> {
    let mut policies = Vec::new();
    for page in 1..=MAX_POLICY_PAGES {
        let path = format!(
            "/repos/{repository}/environments/{encoded_env}/deployment-branch-policies\
             ?per_page=100&page={page}"
        );
        let Some(json) = github_get(api_base, token, &path).await? else {
            break;
        };
        let page_policies: Vec<Value> = json
            .get("branch_policies")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let reached_end = page_policies.len() < 100;
        policies.extend(page_policies);
        if reached_end {
            break;
        }
    }
    Ok(policies)
}

/// The repo's protected branch names (exact-match expansion of GitHub's
/// `protected_branches` policy mode).
async fn fetch_protected_branches(
    api_base: &str,
    token: &str,
    repository: &str,
) -> anyhow::Result<Vec<String>> {
    let mut names = Vec::new();
    for page in 1..=MAX_PROTECTED_PAGES {
        let path = format!("/repos/{repository}/branches?protected=true&per_page=100&page={page}");
        let Some(json) = github_get(api_base, token, &path).await? else {
            break;
        };
        let Some(list) = json.as_array() else { break };
        names.extend(
            list.iter()
                .filter_map(|branch| branch.get("name").and_then(Value::as_str))
                .map(str::to_owned),
        );
        if list.len() < 100 {
            break;
        }
    }
    Ok(names)
}

/// Page through `GET /orgs/{org}/teams/{slug}/members`.
async fn fetch_team_members(
    api_base: &str,
    token: &str,
    org: &str,
    slug: &str,
) -> anyhow::Result<Vec<String>> {
    let mut logins = Vec::new();
    for page in 1..=MAX_MEMBER_PAGES {
        let path = format!(
            "/orgs/{org}/teams/{}/members?per_page=100&page={page}",
            url_path_segment(slug)
        );
        let Some(json) = github_get(api_base, token, &path).await? else {
            // Team not visible to the installation → deny membership.
            return Ok(Vec::new());
        };
        let Some(list) = json.as_array() else { break };
        logins.extend(
            list.iter()
                .filter_map(|member| member.get("login").and_then(Value::as_str))
                .map(str::to_owned),
        );
        if list.len() < 100 {
            break;
        }
    }
    Ok(logins)
}

#[cfg(test)]
#[allow(unsafe_code)] // SAFETY: env writes confined to serialized tests.
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde_json::json;

    /// In-process GitHub API stub for the fixed repository/environment used
    /// by these tests.
    struct StubApi {
        base: String,
    }

    impl StubApi {
        async fn serve(env_json: Value, branch_policies: Value, protection_rules: Value) -> Self {
            let stub = Router::new()
                .route(
                    "/repos/owner/repo/environments/prod/deployment-branch-policies",
                    get(move || {
                        let policies = branch_policies.clone();
                        async move { Json(policies) }
                    }),
                )
                .route(
                    "/repos/owner/repo/environments/prod/deployment_protection_rules",
                    get(move || {
                        let rules = protection_rules.clone();
                        async move { Json(rules) }
                    }),
                )
                .route(
                    "/repos/owner/repo/environments/prod",
                    get(move || {
                        let env = env_json.clone();
                        async move {
                            if env.is_null() {
                                return Err(axum::http::StatusCode::NOT_FOUND);
                            }
                            Ok(Json(env))
                        }
                    }),
                )
                .route(
                    "/repos/owner/repo/branches",
                    get(|| async { Json(json!([{"name": "main"}, {"name": "release/1.0"}])) }),
                )
                .route(
                    "/orgs/acme/teams/deployers/members",
                    get(|| async { Json(json!([{"login": "teammate"}, {"login": "octocat"}])) }),
                );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });
            Self { base }
        }
    }

    async fn shared() -> Arc<crate::state::SharedState> {
        let temp = tempfile::tempdir().unwrap();
        let state = crate::AppState::new(temp.path().join("state"))
            .await
            .unwrap();
        Arc::new(crate::state::SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// PAT-mode resolve (no App configured): `PRELOOP_GITHUB_TOKEN` +
    /// `PRELOOP_GITHUB_API_URL` point the fetch at the stub. Held for the
    /// test — the vars are process-global.
    async fn pat_env(api_base: &str) -> tokio::sync::MutexGuard<'static, ()> {
        let lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        unsafe {
            std::env::set_var("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
            std::env::set_var("PRELOOP_GITHUB_API_URL", api_base);
        }
        lock
    }

    #[tokio::test]
    async fn github_environment_maps_to_rules() {
        let stub = StubApi::serve(
            json!({
                "protection_rules": [
                    {"type": "wait_timer", "wait_timer": 15},
                    {
                        "type": "required_reviewers",
                        "prevent_self_review": true,
                        "reviewers": [
                            {"type": "User", "reviewer": {"login": "octocat"}},
                            {"type": "Team", "reviewer": {"slug": "deployers", "organization": {"login": "acme"}}},
                        ],
                    },
                ],
                "deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true},
            }),
            json!({"branch_policies": [
                {"type": "branch", "name": "main"},
                {"type": "tag", "name": "v*"},
            ]}),
            json!({"custom_deployment_protection_rules": [
                {"enabled": true, "app": {"slug": "datadog-gate"}},
                {"enabled": false, "app": {"slug": "disabled-rule"}},
            ]}),
        )
        .await;
        let _env = pat_env(&stub.base).await;
        let shared = shared().await;
        let resolver = EnvironmentResolver::local(EnvironmentRulesMap::new());
        resolver.set_github_configured();

        let rules = resolver
            .resolve_at(&stub.base, &shared, "owner/repo", "prod")
            .await
            .unwrap();
        assert_eq!(rules.wait_timer_minutes, 15);
        // GitHub's required_reviewers is a set; one approval suffices.
        assert_eq!(rules.required_reviewers, 1);
        assert!(rules.prevent_self_review);
        assert_eq!(
            rules.reviewers,
            vec![
                EnvironmentReviewer::User("octocat".to_owned()),
                EnvironmentReviewer::Team {
                    org: "acme".to_owned(),
                    slug: "deployers".to_owned()
                },
            ]
        );
        assert!(rules.branch_policy_restricted);
        assert_eq!(rules.deployment_branches, vec!["main"]);
        assert_eq!(rules.deployment_tags, vec!["v*"]);
        assert_eq!(rules.custom_protection_rules, vec!["datadog-gate"]);
    }

    #[tokio::test]
    async fn unknown_environment_resolves_to_no_rules() {
        let stub = StubApi::serve(Value::Null, json!({}), json!({})).await;
        let _env = pat_env(&stub.base).await;
        let shared = shared().await;
        let resolver = EnvironmentResolver::local(EnvironmentRulesMap::new());
        resolver.set_github_configured();

        let rules = resolver
            .resolve_at(&stub.base, &shared, "owner/repo", "never-created")
            .await
            .unwrap();
        assert_eq!(
            *rules,
            EnvironmentRules::default(),
            "GitHub auto-creates environments unprotected: a 404 is no rules"
        );
    }

    #[tokio::test]
    async fn unprobed_repo_reports_pending_until_resolved() {
        let stub = StubApi::serve(json!({"protection_rules": []}), json!({}), json!({})).await;
        let _env = pat_env(&stub.base).await;
        let shared = shared().await;
        let resolver = EnvironmentResolver::local(EnvironmentRulesMap::new());
        resolver.set_github_configured();

        // Nothing fetched yet: the gate must hold, not silently answer TOML.
        assert!(matches!(
            resolver.lookup_sync("owner/repo", "prod"),
            EnvironmentLookup::Pending
        ));
        assert_eq!(
            resolver.pending_keys(),
            vec![("owner/repo".to_owned(), "prod".to_owned())]
        );
        resolver
            .resolve_at(&stub.base, &shared, "owner/repo", "prod")
            .await
            .unwrap();
        assert!(matches!(
            resolver.lookup_sync("owner/repo", "prod"),
            EnvironmentLookup::Resolved(_)
        ));
        assert!(resolver.pending_keys().is_empty());
    }

    #[tokio::test]
    async fn reviewer_authorization_matches_users_teams_and_self_review() {
        let stub = StubApi::serve(
            json!({
                "protection_rules": [{
                    "type": "required_reviewers",
                    "prevent_self_review": true,
                    "reviewers": [
                        {"type": "User", "reviewer": {"login": "octocat"}},
                        {"type": "Team", "reviewer": {"slug": "deployers", "organization": {"login": "acme"}}},
                    ],
                }],
            }),
            json!({}),
            json!({}),
        )
        .await;
        let _env = pat_env(&stub.base).await;
        let shared = shared().await;
        let resolver = EnvironmentResolver::local(EnvironmentRulesMap::new());
        resolver.set_github_configured();
        resolver
            .resolve_at(&stub.base, &shared, "owner/repo", "prod")
            .await
            .unwrap();

        assert!(
            resolver
                .reviewer_authorized(&shared, "owner/repo", "prod", "octocat", "ci-bot")
                .await
                .unwrap(),
            "a named user reviewer is authorized"
        );
        assert!(
            resolver
                .reviewer_authorized(&shared, "owner/repo", "prod", "teammate", "ci-bot")
                .await
                .unwrap(),
            "a reviewer team's member is authorized via the members API"
        );
        assert!(
            !resolver
                .reviewer_authorized(&shared, "owner/repo", "prod", "stranger", "ci-bot")
                .await
                .unwrap(),
            "an unrelated login is denied"
        );
        assert!(
            !resolver
                .reviewer_authorized(&shared, "owner/repo", "prod", "octocat", "octocat")
                .await
                .unwrap(),
            "prevent_self_review denies the run's own actor"
        );
    }
}
