//! GitHub-side checks for action pins: commit reachability, repository identity,
//! and full-version tag targets.
//!
//! Semantics follow `gh-actions-lock`, the tool that writes and verifies
//! `actions.lock`, over the REST API the server already reaches through
//! `github_urls.api_url`:
//!
//! - Reachability (`internal/ghapi/graphql_reachability.go`,
//!   `internal/ghapi/rest_fallback.go::anonCompareCommits`): a commit is
//!   reachable when a branch head contains it. The REST check asks
//!   `compare/{sha}...{head}` and requires the merge base to be `sha`, which
//!   holds exactly when `sha` is an ancestor of `head`. Under
//!   [`RefScope::BranchesOrTags`] a tag counts when its target is the commit.
//!   Tag targets are compared for equality only, so tags never cost a compare
//!   call.
//! - Identity (`internal/ghapi/repos.go::repoMetadata`): the numeric `owner.id`
//!   and `id` of `repos/{owner}/{repo}` decide, never the name. A rename keeps
//!   both ids and passes. A transfer changes the owner id and is a mismatch
//!   here, as the `actions.lock` contract requires. gh-actions-lock blocks only
//!   when the repository id changes and merely warns on a rename or transfer
//!   that keeps it (`internal/pipeline/parity.go`).
//! - Full-version tags (`pipeline/checks.IsImmutableRef`, i.e. `ParseSemVer`
//!   plus `IsFull`; `internal/ghapi/graphql_parity.go::checkPinREST`): the tag
//!   must still peel to the locked commit.
//!
//! Verdicts. `CommitNotFound`, `NotReachable`, `IdentityMismatch` and
//! `TagMoved` are GitHub's answers. `Unproven` means a search bound stopped the
//! check before it could prove or disprove the pin, so the pin is refused
//! rather than guessed. `Invalid` means the lockfile names something GitHub
//! cannot have. Only `Unavailable` (authentication, rate limit, 5xx, transport,
//! unparseable answer) says nothing about the pin.
//!
//! Bounds. A bound that is hit yields `Unproven`, even when a transient failure
//! was also recorded during the same search:
//!
//! - 3 pages of 100 entries per branch listing and per tag listing;
//! - 100 compare calls per reachability check, spent on branches only;
//! - 16 annotated tag objects per tag chain;
//! - 8 MiB per response body. A larger body is `Unavailable`.
//!
//! Caches. Only positive verdicts are cached, per `AppState`, keyed by API
//! base, lowercased owner and repository, and the rest of the question.
//! Reachability is reused for [`ACTION_VERIFY_CACHE_TTL`]. A whole locked-pin
//! verdict from [`verify_locked_pin`] is reused for [`LOCKED_PIN_CACHE_TTL`],
//! so a moved tag or a transferred repository is noticed within that window.
//! Concurrent checks of one pin share a single verification (one gate per key),
//! so a matrix fan-out cannot multiply the requests one lock costs.
//!
//! Credentials. Each request carries an App installation token when an App
//! covers the repository, else the static PAT, else none — the same ladder the
//! workflow fetches climb. Verification therefore bills the installation's
//! budget rather than the anonymous one, and can read a private action
//! repository.
//!
//! Each map holds at most 4096 entries. No standard lock is held across an
//! await; the per-pin gate is an async mutex, held by design while the pin it
//! guards is verified.

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::github_breaker::{GithubCredentialBucket, GithubSubsystem};
use crate::state::AppState;

/// How long a positive reachability verdict is reused. A commit that stops
/// being reachable (a force-push, say) stays accepted for at most this long.
pub const ACTION_VERIFY_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
/// How long a positive whole-pin verdict from [`verify_locked_pin`] is reused.
/// Five minutes, the same window as live ref resolution, so a moved tag or a
/// transferred repository is noticed within that window.
pub const LOCKED_PIN_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
/// Positive verdicts held per cache map. The oldest is evicted beyond this.
const VERIFY_CACHE_CAPACITY: usize = 4096;
/// Entries per branch or tag listing page: GitHub's maximum.
const LISTING_PAGE_SIZE: usize = 100;
/// Listing pages read per branch or tag listing. A listing whose last page is
/// full is treated as truncated.
const LISTING_PAGES: usize = 3;
/// Compare calls per reachability check.
const MAX_REF_COMPARES: usize = 100;
/// Annotated tag objects followed down one tag chain. `gh-actions-lock`'s REST
/// peel uses the same bound (`anonPeelTagObject`).
const MAX_TAG_PEEL_DEPTH: usize = 16;
/// Largest response body read from GitHub.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Which refs may contain a pinned commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefScope {
    /// Branch heads only: the lockfile rule.
    BranchesOnly,
    /// Branch heads and tag targets: plain full-SHA pins.
    BranchesOrTags,
}

impl RefScope {
    fn describe(self) -> &'static str {
        match self {
            Self::BranchesOnly => "any branch",
            Self::BranchesOrTags => "any branch or tag",
        }
    }
}

/// Why a pin failed verification, or why it could not be verified.
#[derive(Debug)]
pub enum VerifyError {
    /// GitHub has no such commit in `owner/repo` (404 or 422 on the commit).
    CommitNotFound {
        owner: String,
        repo: String,
        sha: String,
    },
    /// Every candidate ref was checked and none contains the commit. A commit
    /// that exists only in a fork lands here.
    NotReachable {
        owner: String,
        repo: String,
        sha: String,
        scope: RefScope,
    },
    /// The repository's numeric ids differ from the locked ones: it was
    /// transferred, or another repository took its name.
    IdentityMismatch {
        owner: String,
        repo: String,
        expected_owner_id: u64,
        expected_repo_id: u64,
        actual_owner_id: u64,
        actual_repo_id: u64,
    },
    /// The tag no longer peels to the locked commit. `actual_sha` is the object
    /// the tag now names, or empty when the tag no longer exists.
    TagMoved {
        owner: String,
        repo: String,
        tag: String,
        locked_sha: String,
        actual_sha: String,
    },
    /// A search bound stopped the check before it could prove or disprove the
    /// pin. `limit` names the bound. Definitive: a retry against the same
    /// repository meets the same bound. A tag-chain overflow reports
    /// `BranchesOnly`, the rule lockfile tag pins are verified under.
    Unproven {
        owner: String,
        repo: String,
        sha: String,
        scope: RefScope,
        limit: String,
    },
    /// The lockfile names something GitHub cannot have, such as a malformed
    /// owner, repository, tag, or commit id. Definitive.
    Invalid(String),
    /// GitHub could not answer: authentication, rate limit, 5xx, transport,
    /// unparseable body, or an inconsistent answer. The reason is for humans.
    Unavailable(String),
}

impl VerifyError {
    /// `false` only for [`VerifyError::Unavailable`]. Every other variant is a
    /// verdict about the pin.
    pub fn is_definitive(&self) -> bool {
        !matches!(self, Self::Unavailable(_))
    }
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommitNotFound { owner, repo, sha } => {
                write!(f, "commit {sha} does not exist in {owner}/{repo}")
            }
            Self::NotReachable {
                owner,
                repo,
                sha,
                scope,
            } => write!(
                f,
                "commit {sha} of {owner}/{repo} is not reachable from {}",
                scope.describe()
            ),
            Self::IdentityMismatch {
                owner,
                repo,
                expected_owner_id,
                expected_repo_id,
                actual_owner_id,
                actual_repo_id,
            } => write!(
                f,
                "{owner}/{repo} is no longer the locked repository (owner id \
                 {actual_owner_id}, locked {expected_owner_id}; repository id \
                 {actual_repo_id}, locked {expected_repo_id})"
            ),
            Self::TagMoved {
                owner,
                repo,
                tag,
                locked_sha,
                actual_sha,
            } if actual_sha.is_empty() => write!(
                f,
                "tag {tag} of {owner}/{repo} no longer exists; the lock pins {locked_sha}"
            ),
            Self::TagMoved {
                owner,
                repo,
                tag,
                locked_sha,
                actual_sha,
            } => write!(
                f,
                "tag {tag} of {owner}/{repo} now points at {actual_sha}; the lock pins {locked_sha}"
            ),
            Self::Unproven {
                owner,
                repo,
                sha,
                limit,
                ..
            } => write!(
                f,
                "could not verify commit {sha} of {owner}/{repo} within the search budget \
                 ({limit}); pin a commit on the default branch or on a release tag, or set \
                 `[actions_policy] verify_commit_reachability = false`"
            ),
            Self::Invalid(reason) => write!(f, "invalid GitHub name or id: {reason}"),
            Self::Unavailable(reason) => write!(f, "could not verify against GitHub: {reason}"),
        }
    }
}

impl std::error::Error for VerifyError {}

/// One locked dependency to verify at job start.
#[derive(Clone, Copy, Debug)]
pub struct LockedPinCheck<'a> {
    /// Repository owner as the lock names it.
    pub owner: &'a str,
    /// Repository name as the lock names it.
    pub repo: &'a str,
    /// The lock's `ref`. A full-version reference is also checked against `sha`.
    pub reference: &'a str,
    /// Locked commit, as hex.
    pub sha: &'a str,
    /// Locked numeric owner id.
    pub owner_id: u64,
    /// Locked numeric repository id.
    pub repo_id: u64,
}

/// The clock a [`VerifyCache`] reads. Production reads `Instant::now`. Tests
/// substitute a clock they move by hand.
type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// `(api_base, owner, repo, sha, tags_in_scope)`, with owner, repo and sha
/// lowercased because GitHub treats them case-insensitively. `api_base` keeps
/// verdicts from two forges apart.
type ReachKey = (String, String, String, String, bool);

/// `(api_base, owner, repo, reference, sha, owner_id, repo_id)`. The reference
/// is kept as written, because tag names are case-sensitive.
type PinKey = (String, String, String, String, String, u64, u64);

/// Positive verdicts, each with the instant it was recorded and the lifetime
/// its map was built with.
struct TtlMap<K> {
    ttl: Duration,
    entries: parking_lot::Mutex<HashMap<K, Instant>>,
}

impl<K: Eq + Hash + Clone> TtlMap<K> {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    fn contains(&self, key: &K, now: Instant) -> bool {
        self.entries
            .lock()
            .get(key)
            .is_some_and(|recorded| now.saturating_duration_since(*recorded) < self.ttl)
    }

    fn insert(&self, key: K, now: Instant) {
        let mut entries = self.entries.lock();
        entries.retain(|_, recorded| now.saturating_duration_since(*recorded) < self.ttl);
        if entries.len() >= VERIFY_CACHE_CAPACITY && !entries.contains_key(&key) {
            let oldest = entries
                .iter()
                .min_by_key(|(_, recorded)| **recorded)
                .map(|(oldest, _)| oldest.clone());
            if let Some(oldest) = oldest {
                entries.remove(&oldest);
            }
        }
        entries.insert(key, now);
    }
}

/// Positive-only memo of GitHub verdicts for action pins, held in
/// [`AppState`]. Failures are never recorded. Each lock is taken only inside a
/// synchronous section, never across an await.
pub struct VerifyCache {
    clock: Clock,
    reachable: TtlMap<ReachKey>,
    pinned: TtlMap<PinKey>,
    /// One async gate per pin key, so concurrent identical verifications run
    /// one after another instead of in parallel. An entry is dropped once
    /// nobody holds it, which keeps the map bounded by the pins in flight.
    pin_gates: parking_lot::Mutex<HashMap<PinKey, Arc<tokio::sync::Mutex<()>>>>,
}

impl VerifyCache {
    fn from_clock(clock: Clock) -> Self {
        Self {
            clock,
            reachable: TtlMap::new(ACTION_VERIFY_CACHE_TTL),
            pinned: TtlMap::new(LOCKED_PIN_CACHE_TTL),
            pin_gates: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// A cache that reads `clock` instead of the system clock.
    #[cfg(test)]
    fn with_clock(clock: Clock) -> Self {
        Self::from_clock(clock)
    }

    fn now(&self) -> Instant {
        (self.clock)()
    }

    fn reachable_hit(&self, key: &ReachKey) -> bool {
        self.reachable.contains(key, self.now())
    }

    fn remember_reachable(&self, key: ReachKey) {
        self.reachable.insert(key, self.now());
    }

    fn pin_hit(&self, key: &PinKey) -> bool {
        self.pinned.contains(key, self.now())
    }

    /// The gate that serializes verification of one pin. Shared by every
    /// caller holding the same key, so the first one verifies and the rest
    /// find its verdict in the cache.
    fn pin_gate(&self, key: PinKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut gates = self.pin_gates.lock();
        if gates.len() >= VERIFY_CACHE_CAPACITY && !gates.contains_key(&key) {
            // Drop the gates no task is holding, so a long-lived server's map
            // stays bounded by the pins in flight.
            gates.retain(|_, gate| Arc::strong_count(gate) > 1);
        }
        Arc::clone(
            gates
                .entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    fn remember_pin(&self, key: PinKey) {
        self.pinned.insert(key, self.now());
    }
}

impl Default for VerifyCache {
    fn default() -> Self {
        let clock: Clock = Arc::new(Instant::now);
        Self::from_clock(clock)
    }
}

impl fmt::Debug for VerifyCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifyCache").finish_non_exhaustive()
    }
}

fn reach_key(api_base: &str, owner: &str, repo: &str, sha: &str, scope: RefScope) -> ReachKey {
    (
        api_base.to_owned(),
        owner.to_ascii_lowercase(),
        repo.to_ascii_lowercase(),
        sha.to_ascii_lowercase(),
        scope == RefScope::BranchesOrTags,
    )
}

fn pin_key(api_base: &str, pin: &LockedPinCheck<'_>) -> PinKey {
    (
        api_base.to_owned(),
        pin.owner.to_ascii_lowercase(),
        pin.repo.to_ascii_lowercase(),
        pin.reference.to_owned(),
        pin.sha.to_ascii_lowercase(),
        pin.owner_id,
        pin.repo_id,
    )
}

/// Checks that `sha` is reachable from a branch of `owner/repo`, and under
/// [`RefScope::BranchesOrTags`] also equals a tag target.
///
/// The order spends the fewest requests on a commit that is not reachable:
/// 1. the repository, then the commit (404 or 422 is `CommitNotFound`);
/// 2. the branch listing, where a branch head equal to the commit ends the
///    search for free;
/// 3. a compare against the default branch;
/// 4. under `BranchesOrTags`, the tag listing, where a tag target equal to the
///    commit ends the search for free;
/// 5. compares against the remaining branches, within the shared budget.
///
/// A positive result is cached. A failure never is.
pub async fn verify_commit_reachable(
    state: &AppState,
    owner: &str,
    repo: &str,
    sha: &str,
    scope: RefScope,
) -> Result<(), VerifyError> {
    check_repo_name(owner, repo)?;
    let sha = sha.to_ascii_lowercase();
    if !is_object_id(&sha) {
        return Err(VerifyError::Invalid(
            "the commit is not a full commit id".to_owned(),
        ));
    }
    let key = reach_key(&api_base(state), owner, repo, &sha, scope);
    if state.action_verify_cache.reachable_hit(&key) {
        return Ok(());
    }
    reachable_uncached(state, owner, repo, &sha, scope).await?;
    state.action_verify_cache.remember_reachable(key);
    Ok(())
}

async fn reachable_uncached(
    state: &AppState,
    owner: &str,
    repo: &str,
    sha: &str,
    scope: RefScope,
) -> Result<(), VerifyError> {
    let repository = match fetch_repository(state, owner, repo).await? {
        Reply::Found(repository) => repository,
        Reply::Missing => return Err(repository_missing(owner, repo)),
    };
    match github_get::<CommitSha>(state, owner, repo, &format!("repos/{owner}/{repo}/commits/{sha}"))
        .await?
    {
        Reply::Found(found) if found.sha.eq_ignore_ascii_case(sha) => {}
        Reply::Found(_) => {
            return Err(VerifyError::Unavailable(format!(
                "the commit lookup in {owner}/{repo} answered a different commit"
            )));
        }
        Reply::Missing => {
            return Err(VerifyError::CommitNotFound {
                owner: owner.to_owned(),
                repo: repo.to_owned(),
                sha: sha.to_owned(),
            });
        }
    }

    let mut scan = Scan::new();
    let branches = list_refs(state, owner, repo, RefKind::Branches).await?;
    if contains_commit(&branches.refs, sha) {
        return Ok(());
    }
    if branches.truncated {
        scan.cap(format!(
            "more than {} branches",
            LISTING_PAGES * LISTING_PAGE_SIZE
        ));
    }
    let default_branch = repository.default_branch.as_deref();
    let (defaults, others): (Vec<&RefEntry>, Vec<&RefEntry>) = branches
        .refs
        .iter()
        .partition(|branch| Some(branch.name.as_str()) == default_branch);
    if compare_candidates(state, owner, repo, sha, &defaults, &mut scan).await? {
        return Ok(());
    }

    if scope == RefScope::BranchesOrTags {
        let tags = list_refs(state, owner, repo, RefKind::Tags).await?;
        if contains_commit(&tags.refs, sha) {
            return Ok(());
        }
        if tags.truncated {
            scan.cap(format!(
                "more than {} tags",
                LISTING_PAGES * LISTING_PAGE_SIZE
            ));
        }
    }

    if compare_candidates(state, owner, repo, sha, &others, &mut scan).await? {
        return Ok(());
    }
    Err(scan.into_verdict(owner, repo, sha, scope))
}

/// What is left of one reachability search: compare calls not yet spent, the
/// first bound hit, and the first transient failure.
struct Scan {
    compares_left: usize,
    capped: Option<String>,
    unavailable: Option<String>,
}

impl Scan {
    fn new() -> Self {
        Self {
            compares_left: MAX_REF_COMPARES,
            capped: None,
            unavailable: None,
        }
    }

    fn cap(&mut self, limit: String) {
        if self.capped.is_none() {
            self.capped = Some(limit);
        }
    }

    fn unavailable(&mut self, reason: String) {
        if self.unavailable.is_none() {
            self.unavailable = Some(reason);
        }
    }

    /// The verdict once every candidate has been checked or skipped. A bound
    /// outranks a transient failure: the bound alone keeps the search from
    /// proving anything, whatever the failed request would have answered.
    fn into_verdict(self, owner: &str, repo: &str, sha: &str, scope: RefScope) -> VerifyError {
        if let Some(limit) = self.capped {
            return VerifyError::Unproven {
                owner: owner.to_owned(),
                repo: repo.to_owned(),
                sha: sha.to_owned(),
                scope,
                limit,
            };
        }
        if let Some(reason) = self.unavailable {
            return VerifyError::Unavailable(reason);
        }
        VerifyError::NotReachable {
            owner: owner.to_owned(),
            repo: repo.to_owned(),
            sha: sha.to_owned(),
            scope,
        }
    }
}

/// Compares `sha` against each candidate's head in order and returns `true` at
/// the first head that contains it. A candidate that cannot be compared is
/// recorded as unavailable. When the compare budget runs out with candidates
/// left, that is a cap.
async fn compare_candidates(
    state: &AppState,
    owner: &str,
    repo: &str,
    sha: &str,
    candidates: &[&RefEntry],
    scan: &mut Scan,
) -> Result<bool, VerifyError> {
    for candidate in candidates {
        if scan.compares_left == 0 {
            scan.cap(format!("more than {MAX_REF_COMPARES} branches to compare"));
            break;
        }
        scan.compares_left -= 1;
        match is_ancestor_of_head(state, owner, repo, sha, &candidate.commit.sha).await {
            Ok(true) => return Ok(true),
            Ok(false) => {}
            Err(VerifyError::Unavailable(reason)) => scan.unavailable(reason),
            Err(other) => return Err(other),
        }
    }
    Ok(false)
}

fn contains_commit(refs: &[RefEntry], sha: &str) -> bool {
    refs.iter()
        .any(|entry| entry.commit.sha.eq_ignore_ascii_case(sha))
}

/// Whether `sha` is an ancestor of (or equal to) `head`: the merge base of the
/// pair is `sha` exactly when it is.
async fn is_ancestor_of_head(
    state: &AppState,
    owner: &str,
    repo: &str,
    sha: &str,
    head: &str,
) -> Result<bool, VerifyError> {
    let path = format!("repos/{owner}/{repo}/compare/{sha}...{head}?per_page=1");
    let comparison: Comparison = match github_get(state, owner, repo, &path).await? {
        Reply::Found(comparison) => comparison,
        Reply::Missing => {
            return Err(VerifyError::Unavailable(format!(
                "the comparison with {head} in {owner}/{repo} was not found"
            )));
        }
    };
    let merge_base = comparison.merge_base_commit.ok_or_else(|| {
        VerifyError::Unavailable(format!(
            "the comparison with {head} in {owner}/{repo} has no merge base"
        ))
    })?;
    Ok(merge_base.sha.eq_ignore_ascii_case(sha))
}

#[derive(Clone, Copy)]
enum RefKind {
    Branches,
    Tags,
}

impl RefKind {
    fn path(self) -> &'static str {
        match self {
            Self::Branches => "branches",
            Self::Tags => "tags",
        }
    }
}

/// The entries of one listing, and whether the listing ran past
/// [`LISTING_PAGES`].
struct Listing {
    refs: Vec<RefEntry>,
    truncated: bool,
}

async fn list_refs(
    state: &AppState,
    owner: &str,
    repo: &str,
    kind: RefKind,
) -> Result<Listing, VerifyError> {
    let mut refs = Vec::new();
    for page in 1..=LISTING_PAGES {
        let path = format!(
            "repos/{owner}/{repo}/{}?per_page={LISTING_PAGE_SIZE}&page={page}",
            kind.path()
        );
        let entries: Vec<RefEntry> = match github_get(state, owner, repo, &path).await? {
            Reply::Found(entries) => entries,
            Reply::Missing => {
                return Err(VerifyError::Unavailable(format!(
                    "the {} listing of {owner}/{repo} was not found",
                    kind.path()
                )));
            }
        };
        let last_page = entries.len() < LISTING_PAGE_SIZE;
        for entry in entries {
            if !is_object_id(&entry.commit.sha) {
                return Err(VerifyError::Unavailable(format!(
                    "the {} listing of {owner}/{repo} has a malformed commit id",
                    kind.path()
                )));
            }
            refs.push(entry);
        }
        if last_page {
            return Ok(Listing {
                refs,
                truncated: false,
            });
        }
    }
    Ok(Listing {
        refs,
        truncated: true,
    })
}

/// Verifies that `owner/repo` is the repository the lock recorded, by its numeric
/// owner and repository ids. The name is not compared, so a rename that keeps
/// both ids passes.
///
/// Both ids must match. A change of owner id is an
/// [`VerifyError::IdentityMismatch`], which is stricter than gh-actions-lock, as
/// the `actions.lock` contract requires. Never cached on its own.
pub async fn verify_repo_identity(
    state: &AppState,
    owner: &str,
    repo: &str,
    owner_id: u64,
    repo_id: u64,
) -> Result<(), VerifyError> {
    check_repo_name(owner, repo)?;
    let repository = match fetch_repository(state, owner, repo).await? {
        Reply::Found(repository) => repository,
        Reply::Missing => return Err(repository_missing(owner, repo)),
    };
    if repository.owner.id == owner_id && repository.id == repo_id {
        return Ok(());
    }
    Err(VerifyError::IdentityMismatch {
        owner: owner.to_owned(),
        repo: repo.to_owned(),
        expected_owner_id: owner_id,
        expected_repo_id: repo_id,
        actual_owner_id: repository.owner.id,
        actual_repo_id: repository.id,
    })
}

/// Verifies that the tag `tag` of `owner/repo` still peels to `sha`. An annotated
/// tag is followed through its tag objects to the commit it names. Returns
/// [`VerifyError::TagMoved`] with an empty `actual_sha` when the tag no longer
/// exists in a visible repository. Never cached on its own.
pub async fn verify_tag_points_at(
    state: &AppState,
    owner: &str,
    repo: &str,
    tag: &str,
    sha: &str,
) -> Result<(), VerifyError> {
    check_repo_name(owner, repo)?;
    check_tag_name(tag)?;
    match peel_tag(state, owner, repo, tag, sha).await? {
        Some(object) if object.kind == "commit" && object.sha.eq_ignore_ascii_case(sha) => Ok(()),
        Some(object) => Err(VerifyError::TagMoved {
            owner: owner.to_owned(),
            repo: repo.to_owned(),
            tag: tag.to_owned(),
            locked_sha: sha.to_owned(),
            actual_sha: object.sha,
        }),
        None => Err(VerifyError::TagMoved {
            owner: owner.to_owned(),
            repo: repo.to_owned(),
            tag: tag.to_owned(),
            locked_sha: sha.to_owned(),
            actual_sha: String::new(),
        }),
    }
}

/// Peels `tag` in `owner/repo` through any annotated tag objects to the object it
/// names. `Ok(None)` means the tag does not exist in a repository that is
/// visible. A chain deeper than [`MAX_TAG_PEEL_DEPTH`] is `Unproven` for `sha`.
async fn peel_tag(
    state: &AppState,
    owner: &str,
    repo: &str,
    tag: &str,
    sha: &str,
) -> Result<Option<GitObject>, VerifyError> {
    let encoded: Vec<String> = tag
        .split('/')
        .map(crate::actions::percent_encode_path_segment)
        .collect();
    let path = format!("repos/{owner}/{repo}/git/ref/tags/{}", encoded.join("/"));
    let reference: GitRef = match github_get(state, owner, repo, &path).await? {
        Reply::Found(reference) => reference,
        Reply::Missing => {
            // A 404 is also what an invisible repository looks like, so the
            // repository decides which of the two it is.
            return match fetch_repository(state, owner, repo).await? {
                Reply::Found(_) => Ok(None),
                Reply::Missing => Err(repository_missing(owner, repo)),
            };
        }
    };
    let mut object = reference.object;
    let mut followed = 0;
    while object.kind == "tag" {
        if followed == MAX_TAG_PEEL_DEPTH {
            return Err(VerifyError::Unproven {
                owner: owner.to_owned(),
                repo: repo.to_owned(),
                sha: sha.to_owned(),
                scope: RefScope::BranchesOnly,
                limit: format!("annotated tag chain deeper than {MAX_TAG_PEEL_DEPTH} objects"),
            });
        }
        if !is_object_id(&object.sha) {
            return Err(VerifyError::Unavailable(format!(
                "tag {tag} of {owner}/{repo} names a malformed object id"
            )));
        }
        let tag_path = format!("repos/{owner}/{repo}/git/tags/{}", object.sha);
        object = match github_get::<GitTag>(state, owner, repo, &tag_path).await? {
            Reply::Found(annotated) => annotated.object,
            Reply::Missing => {
                return Err(VerifyError::Unavailable(format!(
                    "the annotated tag object of {tag} in {owner}/{repo} is missing"
                )));
            }
        };
        followed += 1;
    }
    Ok(Some(object))
}

/// Whether `reference` names a full version tag: `gh-actions-lock`'s
/// `IsImmutableRef`, which is its `ParseSemVer` plus `IsFull`.
///
/// The reference is an optional lowercase `v`, a decimal major, and optional
/// decimal minor and patch components, with nothing after them. Each component
/// must fit in an `i64`, as `strconv.Atoi` requires. It is not full when it is
/// spelled exactly like the canonical major (`v4`) or major.minor (`v4.2`) tag.
/// So `v4.2.1`, `4.2.0` and `v4.02` qualify, while `v4`, `v4.2`, `v4.2.1-rc.1`,
/// `v4.2.1+build` and full commit ids do not.
pub fn is_full_version_tag(reference: &str) -> bool {
    if is_object_id(reference) {
        return false;
    }
    let (prefix, body) = match reference.strip_prefix('v') {
        Some(body) => ("v", body),
        None => ("", reference),
    };
    let Some((major, rest)) = take_digits(body) else {
        return false;
    };
    let (minor, rest) = optional_component(rest);
    let (patch, rest) = optional_component(rest);
    let (Some(major_n), Some(minor_n)) = (component(Some(major)), component(minor)) else {
        return false;
    };
    if component(patch).is_none() {
        return false;
    }
    let major_tag = format!("{prefix}{major_n}");
    let minor_tag = format!("{prefix}{major_n}.{minor_n}");
    rest.is_empty() && reference != major_tag && reference != minor_tag
}

/// Verifies a locked dependency: its repository identity, then reachability
/// from a branch of the locked repository, then, for a full-version reference,
/// that the tag still peels to the locked commit.
///
/// The reachability step is the impostor-commit check, which an operator can
/// turn off with `[actions_policy] verify_commit_reachability = false` — the
/// same switch a plain SHA pin honors. Identity and moved-tag checks are never
/// optional.
///
/// A positive whole-pin verdict is cached for [`LOCKED_PIN_CACHE_TTL`]. Within
/// that window a repeated check makes no request, and concurrent checks of one
/// pin share a single verification. A failure is never cached.
pub async fn verify_locked_pin(
    state: &AppState,
    pin: &LockedPinCheck<'_>,
) -> Result<(), VerifyError> {
    let key = pin_key(&api_base(state), pin);
    if state.action_verify_cache.pin_hit(&key) {
        return Ok(());
    }
    // One verification per pin at a time: concurrent resolves of the same lock
    // (a matrix fan-out) miss the positive cache together, and N identical
    // checks would spend N times the GitHub requests of one. A waiter re-reads
    // the cache once the first verdict lands.
    let gate = state.action_verify_cache.pin_gate(key.clone());
    let _gate = gate.lock().await;
    if state.action_verify_cache.pin_hit(&key) {
        return Ok(());
    }
    verify_repo_identity(state, pin.owner, pin.repo, pin.owner_id, pin.repo_id).await?;
    if state.actions_policy().verify_commit_reachability {
        verify_commit_reachable(state, pin.owner, pin.repo, pin.sha, RefScope::BranchesOnly)
            .await?;
    }
    if is_full_version_tag(pin.reference) {
        verify_tag_points_at(state, pin.owner, pin.repo, pin.reference, pin.sha).await?;
    }
    state.action_verify_cache.remember_pin(key);
    Ok(())
}

/// A GitHub answer that is either the requested object, or a definitive "no such
/// object" for a 404 or 422.
enum Reply<T> {
    Found(T),
    Missing,
}

/// The credential every verification request for `repository` carries.
///
/// An App installation token when an App covers the repository: it is the only
/// credential that can read a private action repository, and its installation
/// budget keeps verification off the anonymous 60-requests-an-hour allowance.
/// Otherwise the static PAT, attached only when the API base may carry it, and
/// otherwise nothing.
async fn verification_credential(
    state: &AppState,
    repository: &str,
) -> (Option<String>, GithubCredentialBucket) {
    let api_base = api_base(state);
    if let Some(app) = crate::github_app::select_app_for_repo_in_state(state, repository).await {
        let permissions =
            std::collections::BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
        match crate::github_app::get_or_mint_token_at(&api_base, &app, repository, &permissions)
            .await
        {
            Ok(token) => {
                let bucket = match crate::github_app::installation_for_repo(&app, repository).await
                {
                    // The installation id is the rate-limit identity GitHub
                    // bills; never invent one.
                    Ok(installation) if installation != 0 => {
                        GithubCredentialBucket::Installation(installation)
                    }
                    _ => GithubCredentialBucket::Pat,
                };
                return (Some(token), bucket);
            }
            // The mint failure policy belongs to the job-token path, not to
            // verification: fall back to what a host without an App uses.
            Err(error) => tracing::warn!(
                %repository,
                %error,
                "GitHub App token mint failed for action verification; using the static credential"
            ),
        }
    }
    match state.static_github_pat() {
        Some(pat) if pat_may_be_sent(&api_base) => (Some(pat), GithubCredentialBucket::Pat),
        _ => (None, GithubCredentialBucket::Anonymous),
    }
}

/// `GET {api_base}/{path}` through the credential's breaker.
///
/// The credential is picked the way the workflow fetches pick theirs: an App
/// installation token when an App covers `owner/repo`, else the static PAT,
/// else nothing. Verification cannot run on the anonymous budget: 60 requests
/// an hour is one locked workflow, and a private action repository answers 404
/// to an anonymous read, which no pin can pass.
async fn github_get<T: serde::de::DeserializeOwned>(
    state: &AppState,
    owner: &str,
    repo: &str,
    path: &str,
) -> Result<Reply<T>, VerifyError> {
    let api_base = api_base(state);
    let url = format!("{api_base}/{path}");
    let mut request = crate::shared_http::CLIENT
        .get(&url)
        .header("Accept", "application/vnd.github+json");
    let (token, bucket) = verification_credential(state, &format!("{owner}/{repo}")).await;
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = crate::github_breaker::send_observed_labeled(
        &state.github_breakers,
        bucket,
        &state.github_consumption,
        GithubSubsystem::RefResolve,
        request,
    )
    .await
    .map_err(|error| VerifyError::Unavailable(format!("GitHub request refused: {error}")))?;
    state
        .github_consumption
        .record_advertised_bytes(GithubSubsystem::RefResolve, &response);

    let status = response.status().as_u16();
    if status == 404 || status == 422 {
        return Ok(Reply::Missing);
    }
    if !(200..300).contains(&status) {
        return Err(VerifyError::Unavailable(format!(
            "GitHub answered {status} for {path}"
        )));
    }
    let body = read_capped_body(response).await?;
    serde_json::from_slice::<T>(&body)
        .map(Reply::Found)
        .map_err(|error| {
            VerifyError::Unavailable(format!("unparseable GitHub response for {path}: {error}"))
        })
}

async fn read_capped_body(mut response: reqwest::Response) -> Result<Vec<u8>, VerifyError> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| VerifyError::Unavailable(format!("reading GitHub response: {error}")))?
    {
        if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(VerifyError::Unavailable(format!(
                "a GitHub response exceeds {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn fetch_repository(
    state: &AppState,
    owner: &str,
    repo: &str,
) -> Result<Reply<RepositoryInfo>, VerifyError> {
    github_get(state, owner, repo, &format!("repos/{owner}/{repo}")).await
}

/// A 404 on the repository is "missing or invisible to this credential". GitHub
/// does not tell those apart, so it can never prove a pin wrong.
fn repository_missing(owner: &str, repo: &str) -> VerifyError {
    VerifyError::Unavailable(format!(
        "repository {owner}/{repo} was not found or is not visible to this credential"
    ))
}

fn api_base(state: &AppState) -> String {
    state.github_urls.api_url.trim_end_matches('/').to_owned()
}

/// Whether `api_base` may carry the static PAT: HTTPS, or plain HTTP to a
/// loopback emulator. This mirrors `actions::url_targets_configured_github`,
/// which is private to that module.
fn pat_may_be_sent(api_base: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(api_base) else {
        return false;
    };
    match url.scheme() {
        "https" => true,
        "http" => url.host_str().is_some_and(is_loopback_host),
        _ => false,
    }
}

/// `localhost`, or a literal loopback address. Mirrors `actions::is_loopback_host`.
fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// GitHub's owner and repository name rules. These are checked before a name is
/// placed in a URL, so a lockfile cannot smuggle path segments into a request.
fn check_repo_name(owner: &str, repo: &str) -> Result<(), VerifyError> {
    let owner_ok = (1..=39).contains(&owner.len())
        && owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-');
    let repo_ok = (1..=100).contains(&repo.len())
        && repo != "."
        && repo != ".."
        && repo
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if owner_ok && repo_ok {
        Ok(())
    } else {
        Err(VerifyError::Invalid(format!(
            "{owner}/{repo} is not a GitHub owner and repository name"
        )))
    }
}

/// Tag names as they can appear in a lockfile: no empty or dot segments, so the
/// name cannot leave its `git/ref/tags/` path.
fn check_tag_name(tag: &str) -> Result<(), VerifyError> {
    let well_formed = tag.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'+' | b'/')
    }) && tag
        .split('/')
        .all(|segment| !segment.is_empty() && segment != "." && segment != "..");
    if well_formed {
        Ok(())
    } else {
        Err(VerifyError::Invalid(format!("{tag:?} is not a tag name")))
    }
}

/// A full commit id: 40 (SHA-1) or 64 (SHA-256) hex digits. Mirrors
/// `gh-actions-lock`'s `IsFullSha`.
fn is_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Splits a leading run of ASCII digits off `text`. `None` when there is none.
fn take_digits(text: &str) -> Option<(&str, &str)> {
    let end = text.bytes().take_while(u8::is_ascii_digit).count();
    (end > 0).then(|| text.split_at(end))
}

/// Reads an optional `.digits` component, returning its digits and the rest.
fn optional_component(rest: &str) -> (Option<&str>, &str) {
    match rest.strip_prefix('.').and_then(take_digits) {
        Some((digits, rest)) => (Some(digits), rest),
        None => (None, rest),
    }
}

/// `strconv.Atoi` on a digit run, which fails on overflow. An absent component
/// reads as zero, as in `gh-actions-lock`'s `ParseSemVer`.
fn component(digits: Option<&str>) -> Option<i64> {
    digits.map_or(Some(0), |digits| digits.parse().ok())
}

#[derive(serde::Deserialize)]
struct RepositoryInfo {
    id: u64,
    owner: OwnerInfo,
    default_branch: Option<String>,
}

#[derive(serde::Deserialize)]
struct OwnerInfo {
    id: u64,
}

#[derive(serde::Deserialize)]
struct CommitSha {
    sha: String,
}

#[derive(serde::Deserialize)]
struct Comparison {
    merge_base_commit: Option<CommitSha>,
}

#[derive(serde::Deserialize)]
struct RefEntry {
    name: String,
    commit: CommitSha,
}

#[derive(serde::Deserialize)]
struct GitRef {
    object: GitObject,
}

#[derive(serde::Deserialize)]
struct GitTag {
    object: GitObject,
}

#[derive(serde::Deserialize)]
struct GitObject {
    #[serde(rename = "type")]
    kind: String,
    sha: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    const LOCKED: &str = concat!("1111111111", "1111111111", "1111111111", "1111111111");
    const OTHER: &str = concat!("2222222222", "2222222222", "2222222222", "2222222222");
    const DEFAULT_HEAD: &str = concat!("3333333333", "3333333333", "3333333333", "3333333333");
    const FEATURE_HEAD: &str = concat!("4444444444", "4444444444", "4444444444", "4444444444");
    const FORK_ONLY: &str = concat!("5555555555", "5555555555", "5555555555", "5555555555");
    const TAG_OBJECT: &str = concat!("6666666666", "6666666666", "6666666666", "6666666666");

    const API: &str = "https://api.github.com";
    const REPO: &str = "/repos/o/r";
    const BRANCHES_1: &str = "/repos/o/r/branches?per_page=100&page=1";
    const BRANCHES_2: &str = "/repos/o/r/branches?per_page=100&page=2";
    const BRANCHES_3: &str = "/repos/o/r/branches?per_page=100&page=3";
    const BRANCHES_4: &str = "/repos/o/r/branches?per_page=100&page=4";
    const TAGS_1: &str = "/repos/o/r/tags?per_page=100&page=1";

    fn commit_target(sha: &str) -> String {
        format!("/repos/o/r/commits/{sha}")
    }

    fn compare_target(base: &str, head: &str) -> String {
        format!("/repos/o/r/compare/{base}...{head}?per_page=1")
    }

    fn tag_ref_target(tag: &str) -> String {
        format!("/repos/o/r/git/ref/tags/{tag}")
    }

    fn git_tag_target(sha: &str) -> String {
        format!("/repos/o/r/git/tags/{sha}")
    }

    /// The id of the `index`-th annotated tag object in a synthetic chain.
    fn tag_object_id(index: usize) -> String {
        format!("{:040x}", 0x1000 + index)
    }

    fn repo_json(repo_id: u64, owner_id: u64, default_branch: &str) -> String {
        serde_json::json!({
            "id": repo_id,
            "owner": { "id": owner_id },
            "default_branch": default_branch,
        })
        .to_string()
    }

    fn sha_json(sha: &str) -> String {
        serde_json::json!({ "sha": sha }).to_string()
    }

    fn refs_json(refs: &[(&str, &str)]) -> String {
        let entries: Vec<serde_json::Value> = refs
            .iter()
            .map(|(name, sha)| serde_json::json!({ "name": name, "commit": { "sha": sha } }))
            .collect();
        serde_json::Value::Array(entries).to_string()
    }

    /// `count` branch entries, the first named `b{first}`, with distinct commit ids
    /// that never equal `LOCKED`, `OTHER`, or `FORK_ONLY`.
    fn listing_page(first: usize, count: usize) -> String {
        let entries: Vec<serde_json::Value> = (first..first + count)
            .map(|index| {
                serde_json::json!({
                    "name": format!("b{index}"),
                    "commit": { "sha": format!("{index:040x}") },
                })
            })
            .collect();
        serde_json::Value::Array(entries).to_string()
    }

    fn compare_json(merge_base: &str) -> String {
        serde_json::json!({
            "status": "ahead",
            "merge_base_commit": { "sha": merge_base },
        })
        .to_string()
    }

    fn git_object_json(kind: &str, sha: &str) -> String {
        serde_json::json!({ "object": { "type": kind, "sha": sha } }).to_string()
    }

    /// The routes for a tag `v9.9.9` whose annotated chain has `links` tag objects,
    /// the last of which names `LOCKED`.
    fn tag_chain(links: usize) -> Vec<(String, u16, String)> {
        let mut routes = vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(
                tag_ref_target("v9.9.9"),
                200,
                git_object_json("tag", &tag_object_id(0)),
            ),
        ];
        for index in 0..links {
            let (kind, sha) = if index + 1 == links {
                ("commit", LOCKED.to_owned())
            } else {
                ("tag", tag_object_id(index + 1))
            };
            routes.push(route(
                git_tag_target(&tag_object_id(index)),
                200,
                git_object_json(kind, &sha),
            ));
        }
        routes
    }

    /// Answers from `routes` by exact request target (path and query). Anything
    /// else is a 404.
    fn table(
        routes: Vec<(String, u16, String)>,
    ) -> impl Fn(&str) -> (u16, String) + Send + Sync + 'static {
        move |target: &str| {
            routes
                .iter()
                .find(|(route, _, _)| route.as_str() == target)
                .map_or((404, String::new()), |(_, status, body)| {
                    (*status, body.clone())
                })
        }
    }

    fn route(
        target: impl Into<String>,
        status: u16,
        body: impl Into<String>,
    ) -> (String, u16, String) {
        (target.into(), status, body.into())
    }

    /// A GitHub stand-in. `respond` maps each request target (path and query) to
    /// a status and body, and every target received is recorded.
    struct Api {
        base: String,
        requests: Arc<parking_lot::Mutex<Vec<String>>>,
    }
    type Responder = Arc<dyn Fn(&str) -> (u16, String) + Send + Sync>;

    impl Api {
        async fn serve(respond: impl Fn(&str) -> (u16, String) + Send + Sync + 'static) -> Self {
            let respond: Responder = Arc::new(respond);
            let requests = Arc::new(parking_lot::Mutex::new(Vec::new()));
            let recorded = requests.clone();
            let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
                let respond = respond.clone();
                let recorded = recorded.clone();
                async move {
                    let target = request
                        .uri()
                        .path_and_query()
                        .map_or_else(String::new, |path| path.as_str().to_owned());
                    let (status, body) = respond(target.as_str());
                    recorded.lock().push(target);
                    axum::http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body))
                        .unwrap()
                }
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self { base, requests }
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().clone()
        }

        fn count(&self, target: &str) -> usize {
            self.requests
                .lock()
                .iter()
                .filter(|seen| seen.as_str() == target)
                .count()
        }

        fn count_prefix(&self, prefix: &str) -> usize {
            self.requests
                .lock()
                .iter()
                .filter(|seen| seen.starts_with(prefix))
                .count()
        }
    }

    async fn state_for(api: &Api) -> (AppState, tempfile::TempDir) {
        let temp = tempfile::tempdir().unwrap();
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.github_urls = crate::state::GitHubUrls {
            server_url: api.base.clone(),
            api_url: api.base.clone(),
            graphql_url: api.base.clone(),
        };
        (state, temp)
    }

    /// A clock the test moves forward by hand.
    struct ManualClock(Arc<parking_lot::Mutex<Instant>>);

    impl ManualClock {
        fn new() -> Self {
            Self(Arc::new(parking_lot::Mutex::new(Instant::now())))
        }

        fn advance(&self, by: Duration) {
            *self.0.lock() += by;
        }

        /// A cache that reads this clock.
        fn cache(&self) -> VerifyCache {
            let source = self.0.clone();
            let clock: Clock = Arc::new(move || *source.lock());
            VerifyCache::with_clock(clock)
        }
    }

    #[tokio::test]
    async fn default_branch_is_checked_first() {
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, sha_json(LOCKED)),
            route(
                BRANCHES_1,
                200,
                refs_json(&[("feature", FEATURE_HEAD), ("main", DEFAULT_HEAD)]),
            ),
            route(
                compare_target(LOCKED, DEFAULT_HEAD),
                200,
                compare_json(LOCKED),
            ),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;

        verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap();
        assert_eq!(
            api.requests().len(),
            4,
            "repo, commit, listing, one compare"
        );
        assert_eq!(api.count_prefix("/repos/o/r/compare/"), 1);
        assert_eq!(api.count(&compare_target(LOCKED, DEFAULT_HEAD)), 1);
    }

    #[tokio::test]
    async fn a_positive_verdict_is_served_from_the_cache() {
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, sha_json(LOCKED)),
            route(BRANCHES_1, 200, refs_json(&[("main", DEFAULT_HEAD)])),
            route(
                compare_target(LOCKED, DEFAULT_HEAD),
                200,
                compare_json(LOCKED),
            ),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;

        verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap();
        let before = api.requests().len();

        // Owner and repo are case-insensitive, so this names the same entry.
        verify_commit_reachable(&state, "O", "R", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap();
        assert_eq!(
            api.requests().len(),
            before,
            "a cached positive makes no request"
        );
    }

    #[tokio::test]
    async fn commit_on_a_non_default_branch_is_reachable() {
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, sha_json(LOCKED)),
            route(
                BRANCHES_1,
                200,
                refs_json(&[("main", DEFAULT_HEAD), ("feature", FEATURE_HEAD)]),
            ),
            route(
                compare_target(LOCKED, DEFAULT_HEAD),
                200,
                compare_json(OTHER),
            ),
            route(
                compare_target(LOCKED, FEATURE_HEAD),
                200,
                compare_json(LOCKED),
            ),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;

        verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap();
        assert_eq!(api.count(&compare_target(LOCKED, FEATURE_HEAD)), 1);
    }

    #[tokio::test]
    async fn fork_only_commit_is_not_reachable_from_any_ref() {
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            // Fork networks share objects, so the lookup finds the commit.
            route(commit_target(FORK_ONLY), 200, sha_json(FORK_ONLY)),
            route(
                BRANCHES_1,
                200,
                refs_json(&[("main", DEFAULT_HEAD), ("feature", FEATURE_HEAD)]),
            ),
            route(
                compare_target(FORK_ONLY, DEFAULT_HEAD),
                200,
                compare_json(DEFAULT_HEAD),
            ),
            route(
                compare_target(FORK_ONLY, FEATURE_HEAD),
                200,
                compare_json(OTHER),
            ),
            route(TAGS_1, 200, refs_json(&[])),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;

        let error = verify_commit_reachable(&state, "o", "r", FORK_ONLY, RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                VerifyError::NotReachable {
                    scope: RefScope::BranchesOnly,
                    ..
                }
            ),
            "{error:?}"
        );
        assert!(error.is_definitive());

        let error = verify_commit_reachable(&state, "o", "r", FORK_ONLY, RefScope::BranchesOrTags)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                VerifyError::NotReachable {
                    scope: RefScope::BranchesOrTags,
                    ..
                }
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn tag_target_is_accepted_only_when_tags_are_in_scope() {
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, sha_json(LOCKED)),
            route(BRANCHES_1, 200, refs_json(&[("main", DEFAULT_HEAD)])),
            route(
                compare_target(LOCKED, DEFAULT_HEAD),
                200,
                compare_json(OTHER),
            ),
            route(TAGS_1, 200, refs_json(&[("v1.0.0", LOCKED)])),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;

        let error = verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                VerifyError::NotReachable {
                    scope: RefScope::BranchesOnly,
                    ..
                }
            ),
            "{error:?}"
        );
        assert_eq!(
            api.count(TAGS_1),
            0,
            "tags are not listed under BranchesOnly"
        );

        verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOrTags)
            .await
            .unwrap();
        assert_eq!(
            api.count(&compare_target(LOCKED, LOCKED)),
            0,
            "a tag target matches by equality, without a compare"
        );
    }

    #[tokio::test]
    async fn identity_is_judged_by_numeric_ids_not_names() {
        // Both ids match, so this is the locked repository.
        let api = Api::serve(table(vec![route(REPO, 200, repo_json(2, 1, "main"))])).await;
        let (state, _temp) = state_for(&api).await;
        verify_repo_identity(&state, "o", "r", 1, 2).await.unwrap();

        // Another repository answers under the name: its repository id differs.
        let api = Api::serve(table(vec![route(REPO, 200, repo_json(9, 1, "main"))])).await;
        let (state, _temp) = state_for(&api).await;
        let error = verify_repo_identity(&state, "o", "r", 1, 2)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                VerifyError::IdentityMismatch {
                    expected_repo_id: 2,
                    actual_repo_id: 9,
                    ..
                }
            ),
            "{error:?}"
        );

        // A transfer changes the owner id, and that blocks as well.
        let api = Api::serve(table(vec![route(REPO, 200, repo_json(2, 77, "main"))])).await;
        let (state, _temp) = state_for(&api).await;
        let error = verify_repo_identity(&state, "o", "r", 1, 2)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                VerifyError::IdentityMismatch {
                    expected_owner_id: 1,
                    actual_owner_id: 77,
                    ..
                }
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn annotated_tag_peels_to_its_commit_and_a_moved_tag_is_reported() {
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(
                tag_ref_target("v4.2.1"),
                200,
                git_object_json("tag", TAG_OBJECT),
            ),
            route(
                git_tag_target(TAG_OBJECT),
                200,
                git_object_json("commit", LOCKED),
            ),
            route(
                tag_ref_target("v4.2.2"),
                200,
                git_object_json("commit", LOCKED),
            ),
            route(
                tag_ref_target("v4.2.3"),
                200,
                git_object_json("commit", OTHER),
            ),
            route(
                tag_ref_target("v4.2.4"),
                200,
                git_object_json("tree", LOCKED),
            ),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;

        verify_tag_points_at(&state, "o", "r", "v4.2.1", LOCKED)
            .await
            .unwrap();
        verify_tag_points_at(&state, "o", "r", "v4.2.2", LOCKED)
            .await
            .unwrap();
        let error = verify_tag_points_at(&state, "o", "r", "v4.2.3", LOCKED)
            .await
            .unwrap_err();
        assert!(
            matches!(error, VerifyError::TagMoved { ref actual_sha, .. } if actual_sha == OTHER),
            "{error:?}"
        );
        let error = verify_tag_points_at(&state, "o", "r", "v4.2.4", LOCKED)
            .await
            .unwrap_err();
        assert!(
            matches!(error, VerifyError::TagMoved { ref actual_sha, .. } if actual_sha == LOCKED),
            "a tag that names a tree is not the commit: {error:?}"
        );
    }

    #[tokio::test]
    async fn annotated_tag_chains_are_followed_up_to_the_cap() {
        let at_cap = Api::serve(table(tag_chain(MAX_TAG_PEEL_DEPTH))).await;
        let (state, _temp) = state_for(&at_cap).await;
        verify_tag_points_at(&state, "o", "r", "v9.9.9", LOCKED)
            .await
            .unwrap();
        assert_eq!(
            at_cap.count_prefix("/repos/o/r/git/tags/"),
            MAX_TAG_PEEL_DEPTH
        );

        let over_cap = Api::serve(table(tag_chain(MAX_TAG_PEEL_DEPTH + 1))).await;
        let (state, _temp) = state_for(&over_cap).await;
        let error = verify_tag_points_at(&state, "o", "r", "v9.9.9", LOCKED)
            .await
            .unwrap_err();
        assert!(
            matches!(error, VerifyError::Unproven { .. }),
            "a chain past the cap is refused, not passed: {error:?}"
        );
        assert!(error.is_definitive());
        assert_eq!(
            over_cap.count_prefix("/repos/o/r/git/tags/"),
            MAX_TAG_PEEL_DEPTH
        );
    }

    #[tokio::test]
    async fn locked_pin_checks_the_tag_only_for_full_version_references() {
        // The tag now points at OTHER.
        let moved = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, sha_json(LOCKED)),
            route(BRANCHES_1, 200, refs_json(&[("main", DEFAULT_HEAD)])),
            route(
                compare_target(LOCKED, DEFAULT_HEAD),
                200,
                compare_json(LOCKED),
            ),
            route(
                tag_ref_target("v4.2.1"),
                200,
                git_object_json("commit", OTHER),
            ),
        ]))
        .await;
        let (state, _temp) = state_for(&moved).await;
        let pin = LockedPinCheck {
            owner: "o",
            repo: "r",
            reference: "v4.2.1",
            sha: LOCKED,
            owner_id: 1,
            repo_id: 2,
        };
        let error = verify_locked_pin(&state, &pin).await.unwrap_err();
        assert!(
            matches!(error, VerifyError::TagMoved { ref actual_sha, .. } if actual_sha == OTHER),
            "{error:?}"
        );

        // A partial reference such as `v4` is not checked, so no tag request is made.
        let partial = LockedPinCheck {
            reference: "v4",
            ..pin
        };
        verify_locked_pin(&state, &partial).await.unwrap();
        assert_eq!(moved.count(&tag_ref_target("v4")), 0);

        // The tag was deleted: a visible repository without the tag.
        let deleted = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, sha_json(LOCKED)),
            route(BRANCHES_1, 200, refs_json(&[("main", DEFAULT_HEAD)])),
            route(
                compare_target(LOCKED, DEFAULT_HEAD),
                200,
                compare_json(LOCKED),
            ),
        ]))
        .await;
        let (state, _temp) = state_for(&deleted).await;
        let error = verify_locked_pin(&state, &pin).await.unwrap_err();
        assert!(
            matches!(error, VerifyError::TagMoved { ref actual_sha, .. } if actual_sha.is_empty()),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn locked_pin_on_a_matching_tag_is_verified() {
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, sha_json(LOCKED)),
            route(BRANCHES_1, 200, refs_json(&[("main", DEFAULT_HEAD)])),
            route(
                compare_target(LOCKED, DEFAULT_HEAD),
                200,
                compare_json(LOCKED),
            ),
            route(
                tag_ref_target("v4.2.1"),
                200,
                git_object_json("commit", LOCKED),
            ),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;
        let pin = LockedPinCheck {
            owner: "o",
            repo: "r",
            reference: "v4.2.1",
            sha: LOCKED,
            owner_id: 1,
            repo_id: 2,
        };
        verify_locked_pin(&state, &pin).await.unwrap();
    }

    #[tokio::test]
    async fn a_locked_pin_verdict_is_reused_for_five_minutes_then_reverified() {
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, sha_json(LOCKED)),
            route(BRANCHES_1, 200, refs_json(&[("main", DEFAULT_HEAD)])),
            route(
                compare_target(LOCKED, DEFAULT_HEAD),
                200,
                compare_json(LOCKED),
            ),
        ]))
        .await;
        let (mut state, _temp) = state_for(&api).await;
        let clock = ManualClock::new();
        state.action_verify_cache = Arc::new(clock.cache());
        let pin = LockedPinCheck {
            owner: "o",
            repo: "r",
            reference: "v4",
            sha: LOCKED,
            owner_id: 1,
            repo_id: 2,
        };

        verify_locked_pin(&state, &pin).await.unwrap();
        let after_first = api.requests().len();
        assert_eq!(
            after_first, 5,
            "identity (repo), reachability (repo, commit, branches, compare)"
        );

        verify_locked_pin(&state, &pin).await.unwrap();
        assert_eq!(
            api.requests().len(),
            after_first,
            "a cached pin verdict makes no request"
        );

        clock.advance(LOCKED_PIN_CACHE_TTL + Duration::from_secs(1));
        verify_locked_pin(&state, &pin).await.unwrap();
        assert_eq!(
            api.count(REPO),
            3,
            "re-verified: the identity is fetched again"
        );
        assert_eq!(
            api.count_prefix("/repos/o/r/compare/"),
            1,
            "the reachability verdict is still inside its own hour"
        );
    }

    #[tokio::test]
    async fn a_failed_locked_pin_is_not_cached() {
        let fixed = Arc::new(AtomicBool::new(false));
        let flag = fixed.clone();
        let api = Api::serve(move |target: &str| {
            let tag_commit = if flag.load(Ordering::SeqCst) {
                LOCKED
            } else {
                OTHER
            };
            table(vec![
                route(REPO, 200, repo_json(2, 1, "main")),
                route(commit_target(LOCKED), 200, sha_json(LOCKED)),
                route(BRANCHES_1, 200, refs_json(&[("main", DEFAULT_HEAD)])),
                route(
                    compare_target(LOCKED, DEFAULT_HEAD),
                    200,
                    compare_json(LOCKED),
                ),
                route(
                    tag_ref_target("v4.2.1"),
                    200,
                    git_object_json("commit", tag_commit),
                ),
            ])(target)
        })
        .await;
        let (state, _temp) = state_for(&api).await;
        let pin = LockedPinCheck {
            owner: "o",
            repo: "r",
            reference: "v4.2.1",
            sha: LOCKED,
            owner_id: 1,
            repo_id: 2,
        };

        let error = verify_locked_pin(&state, &pin).await.unwrap_err();
        assert!(matches!(error, VerifyError::TagMoved { .. }), "{error:?}");

        fixed.store(true, Ordering::SeqCst);
        verify_locked_pin(&state, &pin).await.unwrap();
    }

    #[tokio::test]
    async fn server_errors_and_rate_limits_are_unavailable_never_reachable() {
        // The commit lookup fails: the commit is unknown, not absent.
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 500, String::new()),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;
        let error = verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(!error.is_definitive(), "{error:?}");

        // Rate limited while comparing against the only branch.
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, sha_json(LOCKED)),
            route(BRANCHES_1, 200, refs_json(&[("main", DEFAULT_HEAD)])),
            route(compare_target(LOCKED, DEFAULT_HEAD), 429, String::new()),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;
        let error = verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(
            matches!(error, VerifyError::Unavailable(_)),
            "a 429 is not NotReachable: {error:?}"
        );

        // The branch listing is down.
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, sha_json(LOCKED)),
            route(BRANCHES_1, 503, String::new()),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;
        let error = verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(!error.is_definitive(), "{error:?}");

        // The identity lookup is rate limited.
        let api = Api::serve(table(vec![route(REPO, 429, String::new())])).await;
        let (state, _temp) = state_for(&api).await;
        let error = verify_repo_identity(&state, "o", "r", 1, 2)
            .await
            .unwrap_err();
        assert!(!error.is_definitive(), "{error:?}");
    }

    #[tokio::test]
    async fn an_absent_commit_is_commit_not_found() {
        for status in [404, 422] {
            let api = Api::serve(table(vec![
                route(REPO, 200, repo_json(2, 1, "main")),
                route(commit_target(LOCKED), status, String::new()),
            ]))
            .await;
            let (state, _temp) = state_for(&api).await;
            let error = verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOrTags)
                .await
                .unwrap_err();
            assert!(
                matches!(error, VerifyError::CommitNotFound { .. }),
                "{status}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_missing_repository_is_unavailable_not_commit_not_found() {
        let api = Api::serve(table(vec![])).await;
        let (state, _temp) = state_for(&api).await;
        let error = verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(!error.is_definitive(), "{error:?}");
    }

    #[tokio::test]
    async fn an_unparseable_answer_is_unavailable() {
        let api = Api::serve(table(vec![
            route(REPO, 200, repo_json(2, 1, "main")),
            route(commit_target(LOCKED), 200, "<html>not json</html>"),
        ]))
        .await;
        let (state, _temp) = state_for(&api).await;
        let error = verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(!error.is_definitive(), "{error:?}");
    }

    #[tokio::test]
    async fn negative_verdicts_are_never_cached() {
        let fixed = Arc::new(AtomicBool::new(false));
        let flag = fixed.clone();
        let api = Api::serve(move |target: &str| {
            // The only branch contains the commit once `fixed` flips.
            let merge_base = if flag.load(Ordering::SeqCst) {
                LOCKED
            } else {
                OTHER
            };
            table(vec![
                route(REPO, 200, repo_json(2, 1, "main")),
                route(commit_target(LOCKED), 200, sha_json(LOCKED)),
                route(BRANCHES_1, 200, refs_json(&[("main", DEFAULT_HEAD)])),
                route(
                    compare_target(LOCKED, DEFAULT_HEAD),
                    200,
                    compare_json(merge_base),
                ),
            ])(target)
        })
        .await;
        let (state, _temp) = state_for(&api).await;

        let error = verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(
            matches!(error, VerifyError::NotReachable { .. }),
            "{error:?}"
        );

        fixed.store(true, Ordering::SeqCst);
        verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn listings_and_compares_are_bounded_and_fail_closed() {
        let api = Api::serve(move |target: &str| match target {
            REPO => (200, repo_json(2, 1, "main")),
            BRANCHES_1 => (200, listing_page(1, LISTING_PAGE_SIZE)),
            BRANCHES_2 => (200, listing_page(1 + LISTING_PAGE_SIZE, LISTING_PAGE_SIZE)),
            BRANCHES_3 => (
                200,
                listing_page(1 + 2 * LISTING_PAGE_SIZE, LISTING_PAGE_SIZE),
            ),
            _ if target == commit_target(LOCKED) => (200, sha_json(LOCKED)),
            _ if target.starts_with("/repos/o/r/compare/") => (200, compare_json(OTHER)),
            _ => (404, String::new()),
        })
        .await;
        let (state, _temp) = state_for(&api).await;

        let error = verify_commit_reachable(&state, "o", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(matches!(error, VerifyError::Unproven { .. }), "{error:?}");
        assert!(error.is_definitive());
        assert_eq!(
            api.count(BRANCHES_4),
            0,
            "listings stop after {LISTING_PAGES} pages"
        );
        assert_eq!(api.count_prefix("/repos/o/r/compare/"), MAX_REF_COMPARES);
    }

    #[tokio::test]
    async fn fork_only_sha_in_a_repo_beyond_the_budget_is_refused_not_passed() {
        // 150 branches, more than the compare budget, and none contains the commit.
        let api = Api::serve(move |target: &str| match target {
            REPO => (200, repo_json(2, 1, "main")),
            _ if target == commit_target(FORK_ONLY) => (200, sha_json(FORK_ONLY)),
            BRANCHES_1 => (200, listing_page(1, LISTING_PAGE_SIZE)),
            BRANCHES_2 => (200, listing_page(1 + LISTING_PAGE_SIZE, 50)),
            TAGS_1 => (200, refs_json(&[])),
            _ if target.starts_with("/repos/o/r/compare/") => (200, compare_json(OTHER)),
            _ => (404, String::new()),
        })
        .await;
        let (state, _temp) = state_for(&api).await;

        for scope in [RefScope::BranchesOnly, RefScope::BranchesOrTags] {
            let error = verify_commit_reachable(&state, "o", "r", FORK_ONLY, scope)
                .await
                .unwrap_err();
            assert!(
                matches!(error, VerifyError::Unproven { .. }),
                "{scope:?}: the impostor must not pass: {error:?}"
            );
            assert!(error.is_definitive(), "{scope:?}: {error:?}");
        }
        assert_eq!(
            api.count_prefix("/repos/o/r/compare/"),
            2 * MAX_REF_COMPARES,
            "each scope spends exactly its budget"
        );
    }

    #[tokio::test]
    async fn malformed_names_and_ids_fail_without_a_request() {
        let api = Api::serve(table(vec![])).await;
        let (state, _temp) = state_for(&api).await;

        let error = verify_commit_reachable(&state, "o", "r", "main", RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(matches!(error, VerifyError::Invalid(_)), "{error:?}");
        assert!(error.is_definitive());
        let error = verify_commit_reachable(&state, "o/../x", "r", LOCKED, RefScope::BranchesOnly)
            .await
            .unwrap_err();
        assert!(matches!(error, VerifyError::Invalid(_)), "{error:?}");
        let error = verify_tag_points_at(&state, "o", "r", "../../user", LOCKED)
            .await
            .unwrap_err();
        assert!(matches!(error, VerifyError::Invalid(_)), "{error:?}");
        assert!(error.is_definitive());
        assert!(api.requests().is_empty(), "{:?}", api.requests());
    }

    #[test]
    fn full_version_tags_follow_the_gh_actions_lock_rule() {
        for reference in ["v4.2.1", "4.2.1", "v10.0.12", "v4.2.0"] {
            assert!(is_full_version_tag(reference), "{reference}");
        }
        for reference in [
            "v4",
            "v4.2",
            "4",
            "v4.2.1-beta.1",
            "v4.2.1+build",
            "v4.2.1.1",
            "v4.",
            "V4.2.1",
            "main",
            "releases/v4",
            LOCKED,
        ] {
            assert!(!is_full_version_tag(reference), "{reference}");
        }
    }

    #[test]
    fn positive_reachability_expires_after_its_ttl() {
        let clock = ManualClock::new();
        let cache = clock.cache();
        let key = reach_key(API, "o", "r", LOCKED, RefScope::BranchesOnly);
        cache.remember_reachable(key.clone());
        clock.advance(ACTION_VERIFY_CACHE_TTL - Duration::from_secs(1));
        assert!(cache.reachable_hit(&key));
        clock.advance(Duration::from_secs(1));
        assert!(!cache.reachable_hit(&key));
    }

    #[test]
    fn positive_cache_evicts_its_oldest_entry_past_the_cap() {
        let clock = ManualClock::new();
        let cache = clock.cache();
        let oldest = reach_key(API, "o", "r0", LOCKED, RefScope::BranchesOnly);
        cache.remember_reachable(oldest.clone());
        for index in 1..=VERIFY_CACHE_CAPACITY {
            clock.advance(Duration::from_millis(1));
            let repo = format!("r{index}");
            cache.remember_reachable(reach_key(API, "o", &repo, LOCKED, RefScope::BranchesOnly));
        }
        assert_eq!(cache.reachable.entries.lock().len(), VERIFY_CACHE_CAPACITY);
        assert!(!cache.reachable_hit(&oldest));
    }
}
