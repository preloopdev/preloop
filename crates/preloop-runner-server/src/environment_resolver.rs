//! Environment protection rules resolved from GitHub.
//!
//! When a GitHub App (or a configured static PAT: `PRELOOP_GITHUB_TOKEN`, else
//! the config file's `github.pat`) is configured, the protection
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
//! Because GitHub also answers 404 for resources a credential cannot see, a
//! missing environment is only accepted as "unprotected" when the same
//! credential can read the repository's Actions surface; otherwise the
//! fetch fails and the gate holds.
//!
//! Transactions cannot await, so the synchronous lookup
//! ([`EnvironmentResolver::lookup_sync`]) reads only cached state: a key that
//! has never resolved returns [`EnvironmentLookup::Pending`], which arms the
//! fail-closed gate and queues the key for the reaper's [`resolve`] pass —
//! the job holds until GitHub answers rather than deploying unprotected on a
//! cold cache.
//!
//! The cache only tracks environments jobs are using: every lookup stamps
//! its key, the reaper refreshes only keys a lookup re-queued, and keys idle
//! for [`IDLE_EVICT`] are dropped. Workflow authors (fork PRs included) pick
//! environment names freely, so an unbounded cache that refreshed every key
//! forever would let a matrix of made-up names pin memory and drain the
//! GitHub API budget.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt;
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
/// A cached key (resolved entry or pending fetch) no lookup has touched for
/// this long is evicted. Held jobs look their key up on every sweep tick, so
/// only environments nothing is waiting on age out; a later lookup simply
/// re-resolves (a GitHub-sourced key holds as `Pending` for one tick).
const IDLE_EVICT: Duration = Duration::from_secs(600);

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
    /// Last time a lookup or resolve touched the key (eviction clock).
    last_used: parking_lot::Mutex<Instant>,
}

impl ResolvedEntry {
    fn touch(&self) {
        *self.last_used.lock() = Instant::now();
    }
}

/// `(repository, environment)` — the resolver's cache key.
type ResolverKey = (String, String);
/// One in-flight fetch per key, so concurrent lookups collapse.
type InflightMap = tokio::sync::Mutex<HashMap<ResolverKey, Arc<tokio::sync::Mutex<()>>>>;
/// `(org, team_slug)` → `(fetched_at, member logins)`.
type TeamMembers = parking_lot::RwLock<HashMap<ResolverKey, (Instant, Vec<String>)>>;

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
    /// Keys awaiting a fetch (first-seen misses, expired entries a lookup
    /// touched, refresh failures) → when a lookup last asked for them. The
    /// reaper drains this every tick; keys nobody asks for again expire.
    pending: parking_lot::Mutex<BTreeMap<(String, String), Instant>>,
    /// In-flight fetches, one per key, so concurrent submissions for the
    /// same environment collapse onto one API round-trip.
    inflight: InflightMap,
    /// `(org, team_slug)` → member logins, for reviewer authorization.
    team_members: TeamMembers,
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
            pending: parking_lot::Mutex::new(BTreeMap::new()),
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
        let cached = self.entries.read().get(&key).map(|entry| {
            entry.touch();
            (entry.rules.clone(), entry.fetched_at)
        });
        if let Some((rules, fetched_at)) = cached {
            if fetched_at.elapsed() > RULES_TTL {
                self.pending.lock().insert(key, Instant::now());
            }
            return EnvironmentLookup::Resolved(Some(rules));
        }
        match self.repo_sources.read().get(repository).copied() {
            Some(RulesSource::Github) => {
                self.pending.lock().insert(key, Instant::now());
                EnvironmentLookup::Pending
            }
            Some(RulesSource::Toml) => {
                EnvironmentLookup::Resolved(self.toml_rules(repository, environment))
            }
            // Source unprobed. With no GitHub credentials configured there is
            // nothing to fetch — answer from TOML directly. With credentials,
            // `resolve` must probe the repo first, so hold as pending.
            None if self.github_configured.load(Ordering::Acquire) => {
                self.pending.lock().insert(key, Instant::now());
                EnvironmentLookup::Pending
            }
            None => EnvironmentLookup::Resolved(self.toml_rules(repository, environment)),
        }
    }

    /// Keys queued by `Pending` lookups or expired entries.
    pub fn pending_keys(&self) -> Vec<(String, String)> {
        self.pending.lock().keys().cloned().collect()
    }

    /// Resolve `(repo, environment)` rules, fetching from GitHub when the
    /// repo is covered by a configured credential.
    ///
    /// Fetch failures are retried by the caller (`refresh_stale`) — the key
    /// stays in `pending` so a held gate re-evaluates once GitHub answers.
    /// A `404` on the environment resolves to *no rules* (GitHub auto-creates
    /// referenced environments unprotected) only when the credential can
    /// demonstrably read the repository's Actions surface; an unreadable
    /// repository 404s the same way, and that case errors (fail closed).
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
            entry.touch();
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
            entry.touch();
            self.pending.lock().remove(&key);
            return Ok(entry.rules.clone());
        }

        // A lookup that failed without a definitive "no App is installed"
        // answer must not fall through to the TOML fallback: a multi-App
        // registry whose lookups all timed out would otherwise overwrite an
        // established GitHub-sourced entry with local rules (or resolve a
        // never-fetched key to "no rules"), letting a protected environment
        // run unprotected. The key stays pending and stale GitHub rules keep
        // answering.
        let candidates =
            match crate::github_app::candidate_apps_for_repo_inner(shared, repository).await {
                Ok(candidates) => candidates,
                Err(crate::github_app::AppLookupError::NotInstalled) => Vec::new(),
                Err(crate::github_app::AppLookupError::Transient(error)) => {
                    return Err(error.context(
                        "GitHub App installation lookup failed; the repository's \
                     environment rules cannot be sourced",
                    ));
                }
            };
        // The configured PAT (`PRELOOP_GITHUB_TOKEN` or the config file's
        // `github.pat`) — the same credential `AppState` hands the rest of
        // the server, so a config-file-only PAT covers resolution too.
        let pat = shared.state.static_github_pat();
        let result: anyhow::Result<Arc<EnvironmentRules>> = if !candidates.is_empty() {
            self.github_configured.store(true, Ordering::Release);
            // Every installed candidate mints the same scoped token for the
            // repository, so try each in turn (the push path does the same):
            // a mint or fetch failure under one App must not fail the
            // repository while another App still covers it.
            let mut last_error = None;
            let mut rules = None;
            for creds in &candidates {
                let fetched = match environment_token(creds, repository).await {
                    Ok(token) => {
                        fetch_environment_rules(api_base, &token, repository, environment).await
                    }
                    Err(error) => Err(error),
                };
                match fetched {
                    Ok(fetched_rules) => {
                        rules = Some(Arc::new(fetched_rules));
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            if rules.is_some() {
                self.repo_sources
                    .write()
                    .insert(repository.to_owned(), RulesSource::Github);
            }
            match rules {
                Some(rules) => Ok(rules),
                None => Err(last_error.unwrap_or_else(|| {
                    anyhow::anyhow!(
                        "no candidate GitHub App could read {repository}'s environment rules"
                    )
                })),
            }
        } else if let Some(token) = pat {
            self.github_configured.store(true, Ordering::Release);
            let rules = fetch_environment_rules(api_base, &token, repository, environment)
                .await
                .map(Arc::new);
            if rules.is_ok() {
                self.repo_sources
                    .write()
                    .insert(repository.to_owned(), RulesSource::Github);
            }
            rules
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
                        last_used: parking_lot::Mutex::new(Instant::now()),
                    },
                );
                self.pending.lock().remove(&key);
                Ok(rules)
            }
            Err(error) => {
                // A resolved-but-stale entry keeps its last-known rules; a
                // never-resolved key stays pending so the gate holds. The
                // pending stamp keeps the time a lookup last *asked* — a
                // failing fetch nobody waits on must still expire.
                if self.entries.read().get(&key).is_none() {
                    self.pending.lock().entry(key).or_insert_with(Instant::now);
                } else {
                    self.pending.lock().remove(&key);
                }
                Err(error)
            }
        }
    }

    /// Evict idle keys, then re-resolve every queued key (first-seen misses
    /// and expired entries a lookup touched). Called once per reaper tick,
    /// ahead of the gate sweep, so a `Pending` hold never outlives the fetch
    /// that unblocks it.
    ///
    /// Only keys a lookup asked for are refreshed: an expired entry nothing
    /// looks up is not fetched again, and is evicted once idle for
    /// [`IDLE_EVICT`]. Held jobs look their key up on every sweep, so their
    /// rules stay fresh while unused names cost nothing.
    pub async fn refresh_stale(&self, shared: &crate::state::SharedState) {
        self.evict_idle(Instant::now()).await;
        let keys = self.pending_keys();
        // Bounded concurrency: each resolve is a few API round-trips, and
        // the reaper's sweep waits on this pass — serialized fetches would
        // scale the whole gate sweep with the stale set (and a hung
        // endpoint would starve every other key). `resolve` collapses
        // concurrent same-key fetches internally, so overlap is safe.
        futures::stream::iter(keys)
            .map(|(repo, env)| async move {
                if let Err(error) = self.resolve(shared, &repo, &env).await {
                    tracing::warn!(
                        repository = %repo,
                        environment = %env,
                        %error,
                        "environment rules refresh failed; gate keeps last-known state"
                    );
                }
            })
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;
    }

    /// Drop resolved entries, pending keys, team-member lists and in-flight
    /// slots no lookup has used within [`IDLE_EVICT`] of `now`.
    async fn evict_idle(&self, now: Instant) {
        let idle = |at: Instant| now.saturating_duration_since(at) > IDLE_EVICT;
        self.entries
            .write()
            .retain(|_, entry| !idle(*entry.last_used.lock()));
        self.pending.lock().retain(|_, asked_at| !idle(*asked_at));
        self.team_members
            .write()
            .retain(|_, (fetched_at, _)| !idle(*fetched_at));
        // A slot whose only owner is the map has no fetch in flight.
        self.inflight
            .lock()
            .await
            .retain(|_, slot| Arc::strong_count(slot) > 1);
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
                EnvironmentReviewer::Team { org, slug }
                    if self
                        .team_member(shared, repository, org, slug, sender)
                        .await
                        .unwrap_or(false) =>
                {
                    return Ok(true);
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
            shared.state.static_github_pat().ok_or_else(|| {
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

/// Mint a repository-scoped token able to read environments. GitHub's REST
/// docs list the environments, branch-policy and protection-rule GETs under
/// the App's `Actions: read` permission
/// (docs.github.com/en/rest/authentication/permissions-required-for-github-apps),
/// so the mint requests it alongside `contents: read`.
async fn environment_token(
    creds: &crate::github_app::GitHubAppCredentials,
    repository: &str,
) -> anyhow::Result<String> {
    let mut permissions = std::collections::BTreeMap::new();
    permissions.insert("contents".to_owned(), "read".to_owned());
    permissions.insert("actions".to_owned(), "read".to_owned());
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

/// Whether the credential can read this repository's Actions surface —
/// `GET /repos/{o}/{r}/actions/runs` needs the same `actions: read` grant the
/// environment GETs do.
///
/// This is the disambiguator for an environment 404: GitHub answers 404 both
/// for an environment that does not exist *and* for a resource the token
/// cannot see (a minted App token is clamped to the installation's grants, so
/// an App without `actions: read` 404s a protected environment). Treating
/// that second 404 as "auto-created unprotected" would fail open, so the
/// caller only accepts the 404 as "no protection" when this probe succeeds.
async fn actions_readable(api_base: &str, token: &str, repository: &str) -> bool {
    let path = format!("/repos/{repository}/actions/runs?per_page=1");
    match crate::shared_http::CLIENT
        .get(format!("{api_base}{path}"))
        .header("User-Agent", "preloop")
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2026-03-10")
        .send()
        .await
    {
        Ok(res) => res.status().is_success(),
        Err(error) => {
            tracing::warn!(repository, %error, "Actions-surface probe failed; keeping the gate closed");
            false
        }
    }
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
        // references it, with no protection — but the same 404 is also what a
        // credential that cannot read the repository's environment
        // configuration sees, and that must not read as "unprotected". Only
        // accept the 404 when the credential demonstrably has the
        // `actions: read` grant; otherwise fail closed (the key stays pending
        // and the gate holds).
        anyhow::ensure!(
            actions_readable(api_base, token, repository).await,
            "GET {env_path} returned 404 and the credential cannot read \
             {repository}'s Actions surface: the 404 may be an authorization \
             failure, so the environment rules stay unresolved (fail closed)"
        );
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
                    .map(|reviewers| {
                        reviewers
                            .iter()
                            .filter_map(|reviewer| parse_reviewer(reviewer, repository))
                            .collect()
                    })
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
///
/// GitHub's environments API reports team reviewers with an
/// `organization_id` and only sometimes the nested `organization` object —
/// live responses can omit it entirely. A repository's team reviewers always
/// belong to the repository's owning organization, so the org falls back to
/// the repository owner; a team whose org can be determined neither way is
/// dropped (fail closed: an unexpandable team can never approve).
fn parse_reviewer(value: &Value, repository: &str) -> Option<EnvironmentReviewer> {
    let kind = value.get("type").and_then(Value::as_str)?;
    let reviewer = value.get("reviewer")?;
    match kind {
        "User" => reviewer
            .get("login")
            .and_then(Value::as_str)
            .map(|login| EnvironmentReviewer::User(login.to_owned())),
        "Team" => {
            // Team reviewers carry `slug` plus their owning `organization`
            // when the API inlines it; otherwise the repository's owner is
            // the org — GitHub only attaches teams of the repo's org.
            let slug = reviewer.get("slug").and_then(Value::as_str)?.to_owned();
            let org = reviewer
                .pointer("/organization/login")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    repository
                        .split_once('/')
                        .map(|(owner, _)| owner.to_owned())
                })?;
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
            return Ok(policies);
        };
        let page_policies: Vec<Value> = json
            .get("branch_policies")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let reached_end = page_policies.len() < 100;
        policies.extend(page_policies);
        if reached_end {
            return Ok(policies);
        }
    }
    // The last allowed page came back full: the list is truncated, and a
    // partial policy set must not read as the environment's whole policy.
    // Error so the resolve fails closed instead of deploying on a silently
    // incomplete allowlist.
    anyhow::bail!(
        "deployment-branch-policies for {repository}/{encoded_env} exceeds \
         {MAX_POLICY_PAGES} pages; treating the list as truncated (fail closed)"
    );
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
            return Ok(names);
        };
        let Some(list) = json.as_array() else {
            return Ok(names);
        };
        names.extend(
            list.iter()
                .filter_map(|branch| branch.get("name").and_then(Value::as_str))
                .map(str::to_owned),
        );
        if list.len() < 100 {
            return Ok(names);
        }
    }
    // The last allowed page came back full: the protected-branch list is
    // truncated. A `protected_branches` policy evaluated against a partial
    // list would deny refs GitHub allows — error so the resolve fails
    // closed instead of pinning an incomplete allowlist.
    anyhow::bail!(
        "protected branches for {repository} exceed {MAX_PROTECTED_PAGES} \
         pages; treating the list as truncated (fail closed)"
    );
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
        let Some(list) = json.as_array() else {
            return Ok(logins);
        };
        logins.extend(
            list.iter()
                .filter_map(|member| member.get("login").and_then(Value::as_str))
                .map(str::to_owned),
        );
        if list.len() < 100 {
            return Ok(logins);
        }
    }
    // The last allowed page came back full: the member list is truncated.
    // A partial roster would deny members GitHub authorizes — error (fail
    // closed) rather than cache an incomplete list.
    anyhow::bail!(
        "team {org}/{slug} members exceed {MAX_MEMBER_PAGES} pages; treating \
         the list as truncated (fail closed)"
    );
}

#[cfg(test)]
#[allow(unsafe_code)] // SAFETY: env writes confined to serialized tests.
mod tests {
    use super::*;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::json;

    /// In-process GitHub API stub for the fixed repository/environment used
    /// by these tests.
    struct StubApi {
        base: String,
        /// `GET /repos/owner/repo/environments/prod` requests served.
        env_hits: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl StubApi {
        async fn serve(env_json: Value, branch_policies: Value, protection_rules: Value) -> Self {
            Self::serve_with(env_json, branch_policies, protection_rules, true).await
        }

        /// A stub whose Actions surface answers 404 — the shape a credential
        /// without `actions: read` sees, which makes an environment 404
        /// ambiguous.
        async fn serve_with_unreadable_actions(env_json: Value) -> Self {
            Self::serve_with(env_json, json!({}), json!({}), false).await
        }

        async fn serve_with(
            env_json: Value,
            branch_policies: Value,
            protection_rules: Value,
            actions_readable: bool,
        ) -> Self {
            let env_hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let hits = env_hits.clone();
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
                        hits.fetch_add(1, Ordering::SeqCst);
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
                    "/orgs/owner/teams/deployers/members",
                    get(|| async { Json(json!([{"login": "teammate"}, {"login": "octocat"}])) }),
                )
                .route(
                    "/repos/owner/repo/actions/runs",
                    get(move || async move {
                        if actions_readable {
                            Ok(Json(json!({"total_count": 0, "workflow_runs": []})))
                        } else {
                            Err(axum::http::StatusCode::NOT_FOUND)
                        }
                    }),
                );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });
            Self { base, env_hits }
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
    /// test — the vars are process-global — and restored when the guards
    /// drop (including on panic): a leaked stub base or token poisons every
    /// later test in the binary that resolves refs or reads the config PAT.
    async fn pat_env(
        api_base: &str,
    ) -> (
        tokio::sync::MutexGuard<'static, ()>,
        crate::state::TestEnvVar,
        crate::state::TestEnvVar,
    ) {
        let lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", api_base);
        (lock, token, api)
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
                            {"type": "Team", "reviewer": {"slug": "deployers"}},
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
                    org: "owner".to_owned(),
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

    /// A 404 whose credential cannot read the repository's Actions surface is
    /// ambiguous — it may be a protected environment the token cannot see —
    /// and must fail closed: the resolve errors, the key stays pending, and
    /// the gate holds instead of promoting the job unprotected.
    #[tokio::test]
    async fn unreadable_actions_surface_keeps_a_404_fail_closed() {
        let stub = StubApi::serve_with_unreadable_actions(Value::Null).await;
        let _env = pat_env(&stub.base).await;
        let shared = shared().await;
        let resolver = EnvironmentResolver::local(EnvironmentRulesMap::new());
        resolver.set_github_configured();

        let error = resolver
            .resolve_at(&stub.base, &shared, "owner/repo", "prod")
            .await
            .expect_err("an unreadable 404 must not resolve to \"no rules\"");
        assert!(
            format!("{error:#}").contains("fail closed"),
            "the error must explain the fail-closed verdict, got: {error:#}"
        );
        assert!(
            matches!(
                resolver.lookup_sync("owner/repo", "prod"),
                EnvironmentLookup::Pending
            ),
            "the unresolved key must keep the gate held"
        );
        assert_eq!(
            resolver.pending_keys(),
            vec![("owner/repo".to_owned(), "prod".to_owned())],
            "the reaper must retry the key"
        );

        // End to end: the admission gate reads that same lookup, so the
        // job's own gate check holds it (fail closed) instead of promoting
        // the deployment unprotected.
        let mut gate = None;
        let outcome = crate::runtime_scheduling::evaluate_environment_gate(
            &resolver.lookup_sync("owner/repo", "prod"),
            "refs/heads/main",
            preloop_gha_protocol::RunId::new(),
            &preloop_gha_protocol::JobId("deploy".to_owned()),
            "prod",
            &mut gate,
            1_700_000_000_000_000_000,
        );
        assert_eq!(
            outcome,
            crate::runtime_scheduling::EnvironmentGateOutcome::Wait,
            "an ambiguous 404 must hold the job, never promote it unprotected"
        );
        assert!(
            gate.is_some(),
            "the held job keeps its armed gate for the reaper's retry"
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
                        {"type": "Team", "reviewer": {"slug": "deployers"}},
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

    /// A config-file PAT (`github.pat`, no `PRELOOP_GITHUB_TOKEN`) is the
    /// credential `AppState` hands out, so resolution and reviewer-team
    /// expansion must use it: an env-var-only lookup would fall back to the
    /// (empty) TOML map for rules and fail every team check closed.
    #[tokio::test]
    async fn config_file_pat_resolves_rules_and_team_members() {
        let stub = StubApi::serve(
            json!({
                "protection_rules": [{
                    "type": "required_reviewers",
                    "reviewers": [
                        {"type": "Team", "reviewer": {"slug": "deployers"}},
                    ],
                }],
            }),
            json!({}),
            json!({}),
        )
        .await;
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _unset = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", stub.base.as_str());
        // The config-file credential shape: `AppState::new` populates
        // `github_pat` from `github.pat`, so seed the field it would hold.
        let temp = tempfile::tempdir().unwrap();
        let mut state = crate::AppState::new(temp.path().join("state"))
            .await
            .unwrap();
        state.github_pat = Some(preloop_gha_protocol::SecretString::new("ghp_config_token"));
        let shared = Arc::new(crate::state::SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        });

        let resolver = EnvironmentResolver::local(EnvironmentRulesMap::new());
        resolver.set_github_configured();
        let rules = resolver
            .resolve_at(&stub.base, &shared, "owner/repo", "prod")
            .await
            .unwrap();
        assert_eq!(
            rules.reviewers,
            vec![EnvironmentReviewer::Team {
                org: "owner".to_owned(),
                slug: "deployers".to_owned(),
            }],
            "the configured PAT fetches the environment's rules"
        );
        assert!(
            resolver
                .reviewer_authorized(&shared, "owner/repo", "prod", "teammate", "ci-bot")
                .await
                .unwrap(),
            "the configured PAT expands the reviewer team"
        );
    }
    #[tokio::test]
    async fn resolve_at_tries_later_app_when_first_mint_fails() {
        use axum::http::StatusCode;

        let app = Router::new()
            .route(
                "/app/installations/101",
                get(|| async { Json(json!({"account": {"login": "owner"}})) }),
            )
            .route(
                "/app/installations/202",
                get(|| async { Json(json!({"account": {"login": "owner"}})) }),
            )
            .route(
                "/app/installations/101/access_tokens",
                post(|| async {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"message": "first App unavailable"})),
                    )
                }),
            )
            .route(
                "/app/installations/202/access_tokens",
                post(|| async {
                    (
                        StatusCode::CREATED,
                        Json(json!({
                            "token": "installation-token",
                            "expires_at": "2030-01-01T00:00:00Z"
                        })),
                    )
                }),
            )
            .route(
                "/repos/owner/repo/environments/prod",
                get(|| async {
                    Json(json!({
                        "protection_rules": [{"type": "wait_timer", "wait_timer": 7}]
                    }))
                }),
            )
            .route(
                "/repos/owner/repo/environments/prod/deployment_protection_rules",
                get(|| async { Json(json!({"custom_deployment_protection_rules": []})) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let _env = pat_env(&base).await;

        let key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
        let mut first = crate::github_app::GitHubAppCredentials::for_tests(
            "101",
            key.clone(),
            crate::github_app::MintFailurePolicy::Error,
        );
        first.installation_id = Some(101);
        let mut second = crate::github_app::GitHubAppCredentials::for_tests(
            "202",
            key,
            crate::github_app::MintFailurePolicy::Error,
        );
        second.installation_id = Some(202);

        let temp = tempfile::tempdir().unwrap();
        let mut state = crate::AppState::new(temp.path().join("state"))
            .await
            .unwrap();
        state.github_apps = Some(crate::github_app::GitHubApps {
            apps: vec![first, second],
            default_index: 0,
        });
        let shared = Arc::new(crate::state::SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        let resolver = EnvironmentResolver::local(EnvironmentRulesMap::new());
        resolver.set_github_configured();

        let rules = resolver
            .resolve_at(&base, &shared, "owner/repo", "prod")
            .await
            .expect("the later candidate App should supply the rules");
        assert_eq!(rules.wait_timer_minutes, 7);
    }

    #[tokio::test]
    async fn pagination_truncation_fails_closed() {
        let branches: Vec<Value> = (0..100)
            .map(|index| json!({"name": format!("branch-{index}")}))
            .collect();
        let policies: Vec<Value> = (0..100)
            .map(|index| json!({"type": "branch", "name": format!("branch-{index}")}))
            .collect();
        let members: Vec<Value> = (0..100)
            .map(|index| json!({"login": format!("member-{index}")}))
            .collect();
        let app = Router::new()
            .route(
                "/repos/owner/repo/branches",
                get({
                    let branches = branches.clone();
                    move || {
                        let branches = branches.clone();
                        async move { Json(branches) }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/environments/prod/deployment-branch-policies",
                get({
                    let policies = policies.clone();
                    move || {
                        let policies = policies.clone();
                        async move { Json(json!({"branch_policies": policies})) }
                    }
                }),
            )
            .route(
                "/orgs/owner/teams/deployers/members",
                get({
                    let members = members.clone();
                    move || {
                        let members = members.clone();
                        async move { Json(members) }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let protected_error = fetch_protected_branches(&base, "token", "owner/repo")
            .await
            .expect_err("a full page at the cap must be treated as truncation");
        assert!(protected_error.to_string().contains("truncated"));

        let policy_error = fetch_branch_policies(&base, "token", "owner/repo", "prod")
            .await
            .expect_err("a full policy page at the cap must be treated as truncation");
        assert!(policy_error.to_string().contains("truncated"));

        let member_error = fetch_team_members(&base, "token", "owner", "deployers")
            .await
            .expect_err("a full member page at the cap must be treated as truncation");
        assert!(member_error.to_string().contains("truncated"));
    }

    #[test]
    fn team_reviewer_uses_repo_owner_when_organization_is_omitted() {
        let reviewer = json!({
            "type": "Team",
            "reviewer": {"slug": "deployers", "organization_id": 1234}
        });
        assert_eq!(
            parse_reviewer(&reviewer, "owner/repo"),
            Some(EnvironmentReviewer::Team {
                org: "owner".to_owned(),
                slug: "deployers".to_owned(),
            })
        );

        let nested = json!({
            "type": "Team",
            "reviewer": {
                "slug": "deployers",
                "organization": {"login": "acme"}
            }
        });
        assert_eq!(
            parse_reviewer(&nested, "owner/repo"),
            Some(EnvironmentReviewer::Team {
                org: "acme".to_owned(),
                slug: "deployers".to_owned(),
            })
        );
    }

    /// Only environments a lookup still asks for stay cached and refreshed:
    /// an expired entry nothing looks up is not re-queued, idle keys are
    /// evicted, and a key that keeps being looked up survives. A fork matrix
    /// of made-up names therefore costs one fetch per name, not a refresh
    /// loop and a permanent map entry.
    #[tokio::test]
    async fn unused_environment_keys_are_neither_refreshed_nor_kept() {
        let stub = StubApi::serve(json!({"protection_rules": []}), json!({}), json!({})).await;
        let _env = pat_env(&stub.base).await;
        let shared = shared().await;
        let resolver = EnvironmentResolver::local(EnvironmentRulesMap::new());
        resolver.set_github_configured();
        resolver
            .resolve_at(&stub.base, &shared, "owner/repo", "prod")
            .await
            .unwrap();

        // Expired but unused: the refresh pass must not fetch it again.
        let expired = Instant::now().checked_sub(RULES_TTL * 2).unwrap();
        resolver
            .entries
            .write()
            .get_mut(&("owner/repo".to_owned(), "prod".to_owned()))
            .unwrap()
            .fetched_at = expired;
        assert!(
            resolver.pending_keys().is_empty(),
            "an expired entry nothing looks up must not be queued for refresh"
        );
        // A lookup (a held job's sweep) re-queues it.
        assert!(matches!(
            resolver.lookup_sync("owner/repo", "prod"),
            EnvironmentLookup::Resolved(Some(_))
        ));
        assert_eq!(
            resolver.pending_keys(),
            vec![("owner/repo".to_owned(), "prod".to_owned())]
        );

        // Within the idle window everything survives an eviction pass.
        resolver.evict_idle(Instant::now()).await;
        assert!(matches!(
            resolver.lookup_sync("owner/repo", "prod"),
            EnvironmentLookup::Resolved(Some(_))
        ));

        // Past it, the entry and its pending refresh are gone; the next
        // lookup holds (fail closed) until the reaper resolves it again.
        resolver
            .evict_idle(Instant::now() + IDLE_EVICT + Duration::from_secs(1))
            .await;
        assert!(resolver.entries.read().is_empty());
        assert!(resolver.pending_keys().is_empty());
        assert!(matches!(
            resolver.lookup_sync("owner/repo", "prod"),
            EnvironmentLookup::Pending
        ));
    }

    /// The reaper's refresh pass must not refetch an expired environment no
    /// job is looking up: before the fix every expired key was refetched on
    /// every tick, forever.
    #[tokio::test]
    async fn refresh_pass_skips_expired_environments_nothing_uses() {
        let stub = StubApi::serve(json!({"protection_rules": []}), json!({}), json!({})).await;
        let _env = pat_env(&stub.base).await;
        let shared = shared().await;
        let resolver = EnvironmentResolver::local(EnvironmentRulesMap::new());
        resolver.set_github_configured();
        resolver
            .resolve_at(&stub.base, &shared, "owner/repo", "prod")
            .await
            .unwrap();
        assert_eq!(stub.env_hits.load(Ordering::SeqCst), 1);

        resolver
            .entries
            .write()
            .get_mut(&("owner/repo".to_owned(), "prod".to_owned()))
            .unwrap()
            .fetched_at = Instant::now().checked_sub(RULES_TTL * 2).unwrap();
        resolver.refresh_stale(&shared).await;
        assert_eq!(
            stub.env_hits.load(Ordering::SeqCst),
            1,
            "an expired environment nothing looks up must not be refetched"
        );
    }
}
