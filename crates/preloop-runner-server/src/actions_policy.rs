//! Operator policy over the `uses:` references a run may execute, configured
//! in `[actions_policy]`. It mirrors GitHub's "Require actions to be pinned to
//! a full-length commit SHA" and "Allow or block specified actions and
//! reusable workflows" settings, plus the `actions.lock` requirement.
//!
//! # Allow and block entries
//!
//! `allowed_actions` holds one entry per array element. An entry is
//! `[!]PATTERN`; a leading `!` blocks instead of allowing. GitHub documents an
//! action entry as `OWNER/REPOSITORY@TAG-OR-SHA` and a reusable-workflow entry
//! as `OWNER/REPOSITORY/PATH/FILENAME@TAG-OR-SHA`.
//!
//! - `*` (or `**`) alone matches every remote action and reusable workflow.
//! - `OWNER/REPO` matches that repository. An optional `@REF` narrows it to one
//!   tag, branch, or commit: the ref is everything after the first `@`, as in
//!   `uses:`. Without `@REF` every ref matches.
//! - `OWNER/REPO/PATH` names one reusable workflow file or directory, and
//!   matches only reusable-workflow calls. An entry without a path matches every
//!   path of its repository, reusable workflows included. That is Preloop's
//!   reading, not a GitHub-documented rule: GitHub's docs give the
//!   reusable-workflow syntax with a path, but do not say whether a path-less
//!   repository entry covers its reusable workflows. Read this way, an allow list
//!   admits more than an exact-path entry would, and a block list blocks more.
//!   That suits an operator who allows or blocks a whole repository.
//! - A step's action is matched by its repository and ref, and its subpath takes
//!   no part: `github/codeql-action/init@v3` is matched as
//!   `github/codeql-action@v3`. So an entry with a path never names a step.
//! - `*` matches any run of characters inside one `/`-separated segment, never
//!   `/`. `**` matches any run, `/` included, so `actions/checkout@**` matches
//!   `actions/checkout@release/1` and `actions/checkout@*` does not. This is the
//!   filter-pattern rule GitHub documents for `*` in its workflow syntax
//!   reference; the allowed-actions page does not restate it. `*` in the owner
//!   or repo works the same way: `space-org*/*`, `*/octocat*@*`.
//! - Owner and repo compare ASCII case-insensitively, as GitHub owner and
//!   repository names do. Path and ref compare case-sensitively, with one
//!   exception: a full-length commit SHA (40 or 64 hex digits, in any case) names
//!   one commit however it is spelled, so both sides are lowercased before the
//!   comparison. Tags and branches, including hex names that are not full
//!   length, stay case-sensitive.
//! - Owner and repo accept letters, digits, `-`, `_`, `.`, and `*`. A path adds
//!   `/` between segments. A ref may hold any character except whitespace,
//!   control characters, and `\`, since Git allows `!`, `#`, `%`, and more.
//!   `*` is the only wildcard. An entry may not contain `,`, `?`, `[`, or `]`:
//!   `,` separates entries in GitHub's settings, and the others are not
//!   wildcards here, so such an entry would silently match nothing. Entries are
//!   at most 1024 bytes, and at most 1000 entries are accepted, GitHub's own
//!   limit.
//! - `docker://` images cannot be named by an entry, and an entry that starts
//!   with `docker://` is rejected at load.
//!
//! Evaluation:
//!
//! - A matching `!` entry blocks the reference, whatever the entry order and
//!   whatever allows match.
//! - With no allow entry, every reference that no block matches is allowed.
//! - With at least one allow entry, a reference must also match one of them.
//! - Local references are always allowed: `./` and `$/`, which GitHub documents
//!   as local actions, and `../`, which this policy treats alike. A `$/`
//!   reference with a ref does not parse, so an allow list refuses it.
//! - A reference that does not parse is refused whenever `allowed_actions` is
//!   non-empty, block-only lists included: a block entry cannot be checked
//!   against a reference it cannot read.
//! - GitHub does not document how `docker://` images are matched. Preloop fails
//!   closed: while an allow entry is set, a `docker://` reference matches nothing
//!   and is blocked. Block-only lists leave it alone.
//! - A dependency covered by its workflow's `actions.lock` is matched by its
//!   locked commit SHA, not by the tag or branch written in the workflow, as
//!   GitHub documents. `actions/checkout@v4` does not match a locked
//!   `actions/checkout@v4`. Use `actions/checkout@*`, `actions/*`, or the full
//!   commit SHA instead.
//!
//! # Pinning and lockfile rules
//!
//! - `require_sha_pinning`: every remote `uses:` must be a full-length commit SHA
//!   other than all zeros, or be covered by the actions.lock entry of the
//!   workflow that contains it. The check covers step actions and job-level
//!   reusable workflow references, including references inside the reusable
//!   workflows the run calls. `./`, `../`, `$/`, and `docker://` are exempt.
//!   Dependencies of composite actions are not walked here: the lock pins them
//!   when the workflow is onboarded. A reusable workflow call is never covered
//!   by the lock, so it needs a full SHA: `resolve_remote_workflows` resolves a
//!   remote reference live through the commits API, so no lock entry can vouch
//!   for the commit that runs. GitHub's lockfile docs say the lockfile pins
//!   reusable workflows too, and gh-actions-lock does not, so this is stricter
//!   than GitHub for onboarded workflows; that is an open product decision.
//!   GitHub's own SHA setting exempts reusable workflows, which may be referenced
//!   by tag; this knob does not.
//! - `require_lockfile`: the run's top-level workflow and every reusable workflow
//!   it calls, transitively, must be onboarded in the `actions.lock` of the
//!   repository that defines it. A `./` or `$/` call inherits the lock of the
//!   workflow that makes it. A remote call takes the lock stored under its
//!   `uses:` string in `remote_locks`.
//!
//! # Reusable workflows
//!
//! A callee is located as the reusable-workflow expander locates it:
//! `callee_reference` gives its key and `normalize_reusable_path` its lookup. A
//! call from the run's own workflow keys by its written reference. A `./` or
//! `$/` call inside a remote callee names that callee's repository at the commit
//! the callee resolved to, so it is never the run's file at the same path. The
//! walk follows at most `DECLARED_ACTION_REUSABLE_DEPTH` levels. A callee that
//! is deeper, cannot be located, or does not parse is a violation under every
//! enabled rule.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use preloop_gha_parser::lockfile::{ActionsLock, LockedDependency, pin_key_for_uses};
use preloop_gha_parser::{callee_reference, is_local_workflow_ref, normalize_reusable_path};
use preloop_gha_protocol::git_ref::is_commit_sha_not_zero;
use serde::{Deserialize, Serialize};

use crate::actions::DECLARED_ACTION_REUSABLE_DEPTH;

/// Longest entry or `uses:` reference the policy examines. Longer input is
/// rejected rather than matched, which bounds the matcher's work.
const MAX_ENTRY_LEN: usize = 1024;
/// Most `allowed_actions` entries accepted: GitHub's own limit.
const MAX_ALLOWED_ACTIONS: usize = 1000;
/// Violations named in a rejection before the rest are only counted.
const MAX_LISTED_VIOLATIONS: usize = 20;

/// `[actions_policy]` keys. Each also names the rule of the violations it
/// produces, so a rejection points the operator at the setting.
const ALLOWED_ACTIONS: &str = "allowed_actions";
const REQUIRE_SHA_PINNING: &str = "require_sha_pinning";
const REQUIRE_LOCKFILE: &str = "require_lockfile";

const SHA_MESSAGE: &str = "not pinned to a full-length commit SHA and not covered by actions.lock; \
     pin the commit SHA, or onboard the workflow with `gh actions-lock`";
const MALFORMED_SHA_MESSAGE: &str =
    "not a recognized owner/repo@ref reference, so it cannot be verified as pinned";
const NOT_A_REPOSITORY_WORKFLOW: &str =
    "not a workflow stored in a repository, so it cannot be onboarded in actions.lock";
const DOCKER_ENTRY: &str = "docker:// images cannot be named in allowed_actions; a docker:// \
     reference is blocked while an allow entry is set";
const DOCKER_NOT_NAMEABLE: &str = "docker:// images cannot be matched by allowed_actions entries, \
     so the reference is blocked while an allow entry is set";
const MALFORMED_NOT_NAMEABLE: &str = "not a recognized owner/repo@ref reference, so it cannot be \
     checked against allowed_actions";

/// Operator policy over the `uses:` references a run may execute
/// (`[actions_policy]`). Every control is off by default, except the
/// impostor-commit check, which is on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ActionsPolicy {
    /// Require every remote `uses:` to be a full-length commit SHA, or to be
    /// covered by the actions.lock entry of the workflow that contains it. The
    /// check covers step actions and job-level reusable workflow references,
    /// including those inside the reusable workflows a run calls. `./`, `../`,
    /// `$/`, and `docker://` references are exempt. A reusable workflow call
    /// is never covered by the lock, so it needs a full SHA: the commit that
    /// runs is resolved live, not from the lock. GitHub's SHA setting exempts
    /// reusable workflows; this knob does not.
    pub require_sha_pinning: bool,
    /// Require every workflow in the run, the top-level one and each reusable
    /// workflow it calls transitively, to be onboarded in the `actions.lock` of
    /// the repository that defines it.
    pub require_lockfile: bool,
    /// Check that full-SHA pins are reachable from a branch or tag of the named
    /// repository (the impostor-commit check). This module only carries the
    /// switch; the resolve path applies it.
    pub verify_commit_reachability: bool,
    /// Allow and block list for `uses:` references, one `[!]PATTERN` per entry.
    /// The module docs define the grammar. A step's action is matched by its
    /// repository and ref, so an entry's `/PATH` names a reusable workflow only:
    /// GitHub documents an action entry as `OWNER/REPOSITORY@TAG-OR-SHA`. A
    /// path-less entry also covers that repository's reusable workflows, which is
    /// Preloop's reading. Empty means no allow list. Entries are validated when
    /// the config loads.
    #[serde(deserialize_with = "de_allowed_actions")]
    pub allowed_actions: Vec<String>,
}

impl Default for ActionsPolicy {
    fn default() -> Self {
        Self {
            require_sha_pinning: false,
            require_lockfile: false,
            verify_commit_reachability: true,
            allowed_actions: Vec::new(),
        }
    }
}

/// Everything [`ActionsPolicy::evaluate`] reads about one run.
#[derive(Debug, Clone, Copy)]
pub struct PolicyInputs<'a> {
    /// Repository-relative path of the run's top-level workflow.
    pub workflow_path: &'a str,
    /// The parsed top-level workflow at `workflow_path`.
    pub workflow: &'a preloop_gha_parser::Workflow,
    /// Reusable-workflow YAML keyed the way `uses:` strings key it: the full
    /// reference for remote callees, the repository-relative path for `./` and
    /// `$/` callees.
    pub reusable_workflows: &'a BTreeMap<String, String>,
    /// The commit each remote reusable workflow reference resolved to, keyed like
    /// `reusable_workflows`. The expander locates a remote callee's `./` and `$/`
    /// calls through it, so the policy does too.
    pub reusable_workflow_shas: &'a BTreeMap<String, String>,
    /// The run repository's `actions.lock`, when it has one.
    pub lock: Option<&'a ActionsLock>,
    /// The `actions.lock` of each remote reusable workflow's repository, keyed
    /// by the remote `uses:` string.
    pub remote_locks: &'a BTreeMap<String, ActionsLock>,
}

/// One offending reference.
#[derive(Debug)]
pub struct Violation {
    /// The workflow file containing the reference. A remote workflow is named
    /// `owner/repo/path`. Empty when the check has no workflow context.
    pub workflow_path: String,
    /// The reference as written. Empty for a violation of the whole workflow.
    pub uses: String,
    /// The `[actions_policy]` key that rejected the reference.
    pub rule: &'static str,
    /// Why, with the remedy.
    pub message: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.workflow_path.is_empty() {
            write!(f, "{}: ", self.workflow_path)?;
        }
        if !self.uses.is_empty() {
            write!(f, "{}: ", self.uses)?;
        }
        write!(
            f,
            "{} (config: [actions_policy] {})",
            self.message, self.rule
        )
    }
}

/// Every violation found in one run. Names each offender, up to a cap, so the
/// operator can fix them all in one pass.
#[derive(Debug)]
pub struct PolicyViolation {
    /// The violations, in the order the walk met them.
    pub violations: Vec<Violation>,
}

impl fmt::Display for PolicyViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total = self.violations.len();
        let noun = if total == 1 {
            "violation"
        } else {
            "violations"
        };
        write!(f, "actions policy rejected this run ({total} {noun})")?;
        for violation in self.violations.iter().take(MAX_LISTED_VIOLATIONS) {
            write!(f, "\n  - {violation}")?;
        }
        let hidden = total.saturating_sub(MAX_LISTED_VIOLATIONS);
        if hidden > 0 {
            write!(f, "\n  ... and {hidden} more")?;
        }
        Ok(())
    }
}

impl std::error::Error for PolicyViolation {}

/// Why an `allowed_actions` entry was rejected.
#[derive(Debug)]
struct PatternError {
    /// The entry as configured.
    entry: String,
    /// What is wrong with it.
    reason: String,
}

impl fmt::Display for PatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid allowed_actions entry {:?}: {}",
            self.entry, self.reason
        )
    }
}

impl std::error::Error for PatternError {}

impl ActionsPolicy {
    /// Check a run's workflows against the policy.
    ///
    /// Walks every `uses:` of the top-level workflow and of each reusable
    /// workflow it calls, locating each callee as the expander does and
    /// following the calls transitively within the depth bound. Returns every
    /// violation found, never the first only. A default policy returns `Ok`
    /// without inspecting anything.
    pub fn evaluate(&self, inputs: &PolicyInputs<'_>) -> Result<(), PolicyViolation> {
        if !self.any_rule_enabled() {
            return Ok(());
        }
        let patterns =
            compile_patterns(&self.allowed_actions).map_err(|error| PolicyViolation {
                violations: vec![Violation {
                    workflow_path: inputs.workflow_path.to_owned(),
                    uses: error.entry,
                    rule: ALLOWED_ACTIONS,
                    message: error.reason,
                }],
            })?;
        let root = Frame {
            origin: None,
            path: repo_relative(inputs.workflow_path),
            display: inputs.workflow_path.to_owned(),
            lock: inputs.lock,
            depth: 0,
        };
        let mut evaluation = Evaluation {
            policy: self,
            patterns: &patterns,
            reusable_workflows: inputs.reusable_workflows,
            shas: inputs.reusable_workflow_shas,
            remote_locks: inputs.remote_locks,
            walked: BTreeSet::new(),
            reported: BTreeSet::new(),
            violations: Vec::new(),
        };
        if self.require_lockfile && !is_onboarded(inputs.lock, &root.path) {
            evaluation.report(
                &root,
                "",
                REQUIRE_LOCKFILE,
                lockfile_message(inputs.lock.is_some(), false),
            );
        }
        evaluation.walk(&root, inputs.workflow);
        if evaluation.violations.is_empty() {
            Ok(())
        } else {
            Err(PolicyViolation {
                violations: evaluation.violations,
            })
        }
    }

    /// Check one `uses:` reference against the allow and block lists only.
    ///
    /// Used at resolve time for nested composite dependencies, which the
    /// submission-time walk does not see. Pass the form GitHub matches:
    /// `owner/repo@<locked sha>` for a dependency covered by `actions.lock`,
    /// since a locked dependency is matched by its commit, and the written
    /// `uses:` string otherwise. A step's subpath takes no part in matching.
    /// The violation's `workflow_path` is empty, since this check has no
    /// workflow context.
    pub fn check_uses(&self, uses: &str) -> Result<(), Violation> {
        if self.allowed_actions.is_empty() {
            return Ok(());
        }
        let patterns = compile_patterns(&self.allowed_actions).map_err(|error| Violation {
            workflow_path: String::new(),
            uses: error.entry,
            rule: ALLOWED_ACTIONS,
            message: error.reason,
        })?;
        allow_list_verdict(&patterns, &classify(uses).for_action(), None).map_err(|message| {
            Violation {
                workflow_path: String::new(),
                uses: uses.to_owned(),
                rule: ALLOWED_ACTIONS,
                message,
            }
        })
    }

    fn any_rule_enabled(&self) -> bool {
        self.require_sha_pinning || self.require_lockfile || !self.allowed_actions.is_empty()
    }
}

/// Deserialize `allowed_actions`, rejecting malformed entries at config load
/// rather than when a run is submitted.
fn de_allowed_actions<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let entries = Vec::<String>::deserialize(deserializer)?;
    if entries.len() > MAX_ALLOWED_ACTIONS {
        return Err(<D::Error as serde::de::Error>::custom(format!(
            "allowed_actions has {} entries; at most {MAX_ALLOWED_ACTIONS} are supported",
            entries.len()
        )));
    }
    for entry in &entries {
        parse_pattern(entry).map_err(<D::Error as serde::de::Error>::custom)?;
    }
    Ok(entries)
}

#[derive(Debug)]
struct Pattern {
    /// The entry as configured, `!` included, for messages.
    entry: String,
    negated: bool,
    matcher: Matcher,
}

#[derive(Debug)]
enum Matcher {
    /// `*` or `**`: every remote reference.
    Any,
    /// `owner/repo[/path][@ref]`. An absent path or ref matches any.
    Repository {
        owner: Vec<Token>,
        repo: Vec<Token>,
        path: Option<Vec<Token>>,
        git_ref: Option<Vec<Token>>,
    },
}

/// Compiled wildcard text. `*` is a `Star`, `**` is a `DoubleStar`, and every
/// other byte is a `Literal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    Literal(u8),
    /// Any run of bytes except `/`.
    Star,
    /// Any run of bytes, `/` included.
    DoubleStar,
}

fn compile_patterns(entries: &[String]) -> Result<Vec<Pattern>, PatternError> {
    entries.iter().map(|entry| parse_pattern(entry)).collect()
}

fn parse_pattern(entry: &str) -> Result<Pattern, PatternError> {
    let reject = |reason: String| PatternError {
        entry: entry.to_owned(),
        reason,
    };
    if entry.is_empty() {
        return Err(reject("the entry is empty".to_owned()));
    }
    if entry.len() > MAX_ENTRY_LEN {
        return Err(reject(format!(
            "the entry is longer than {MAX_ENTRY_LEN} bytes"
        )));
    }
    let (negated, body) = match entry.strip_prefix('!') {
        Some(body) => (true, body),
        None => (false, entry),
    };
    if body.is_empty() {
        return Err(reject("`!` must be followed by a pattern".to_owned()));
    }
    if body.starts_with('!') {
        return Err(reject("`!` may appear only once, at the start".to_owned()));
    }
    if body.starts_with("docker://") {
        return Err(reject(DOCKER_ENTRY.to_owned()));
    }
    let matcher = if body == "*" || body == "**" {
        Matcher::Any
    } else {
        parse_repository_pattern(body).map_err(reject)?
    };
    Ok(Pattern {
        entry: entry.to_owned(),
        negated,
        matcher,
    })
}

fn parse_repository_pattern(body: &str) -> Result<Matcher, String> {
    let (location, git_ref) = match body.split_once('@') {
        Some((location, git_ref)) => (location, Some(git_ref)),
        None => (body, None),
    };
    let mut parts = location.splitn(3, '/');
    let (Some(owner), Some(repo)) = (parts.next(), parts.next()) else {
        return Err(
            "expected `owner/repo`, then an optional `/path` and `@ref`, or `*` for every action"
                .to_owned(),
        );
    };
    let path = parts.next();
    for (segment, what) in [(owner, "owner"), (repo, "repo")] {
        check_pattern_segment(segment, what)?;
    }
    if let Some(path) = path {
        for segment in path.split('/') {
            check_pattern_segment(segment, "path segment")?;
        }
    }
    if let Some(git_ref) = git_ref {
        if git_ref.is_empty() {
            return Err("nothing follows `@`".to_owned());
        }
        if let Some(c) = git_ref.chars().find(|&c| !is_pattern_ref_char(c)) {
            return Err(unsupported(
                c,
                "a ref may not contain whitespace, control characters, or `\\`",
            ));
        }
    }
    Ok(Matcher::Repository {
        owner: compile_glob(owner),
        repo: compile_glob(repo),
        path: path.map(compile_glob),
        git_ref: git_ref.map(|git_ref| compile_glob(fold_full_sha(git_ref).as_ref())),
    })
}

fn check_pattern_segment(segment: &str, what: &str) -> Result<(), String> {
    if segment.is_empty() {
        return Err(format!("the {what} is empty"));
    }
    if segment == "." || segment == ".." {
        return Err(format!("`{segment}` cannot be a {what}"));
    }
    match segment.chars().find(|&c| !(is_name_char(c) || c == '*')) {
        Some(c) => Err(unsupported(
            c,
            "allowed are letters, digits, `-`, `_`, `.`, and `*`",
        )),
        None => Ok(()),
    }
}

/// Why `c` may not appear in an entry, where `allowed` says what may. `,` is
/// refused as a separator, and `?`, `[`, and `]` as wildcards this matcher lacks.
fn unsupported(c: char, allowed: &str) -> String {
    match c {
        ',' => "`,` does not separate entries; give each pattern its own array element".to_owned(),
        '?' | '[' | ']' => format!("{c:?} is not supported; the only wildcard is `*`"),
        _ => format!("unsupported character {c:?}; {allowed}"),
    }
}

fn is_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')
}

/// Whether `c` may appear in a `uses:` ref. Git allows `!`, `#`, `%`, `,`, and
/// more, so a ref is refused only for whitespace, control characters, and `\`.
/// Refusing a character a ref can carry would let a block entry be bypassed.
fn is_ref_char(c: char) -> bool {
    !c.is_whitespace() && !c.is_control() && c != '\\'
}

/// Whether `c` may appear in an entry's ref. `,`, `?`, `[`, and `]` are refused:
/// `,` separates entries in GitHub's settings, and the other three are not
/// wildcards here, so an entry carrying one would silently match nothing.
fn is_pattern_ref_char(c: char) -> bool {
    is_ref_char(c) && !matches!(c, ',' | '?' | '[' | ']')
}

/// Whether `git_ref` is a full-length commit SHA: 40 hex digits (SHA-1) or 64
/// (SHA-256), in any case.
fn is_full_hex_sha(git_ref: &str) -> bool {
    matches!(git_ref.len(), 40 | 64) && git_ref.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// `git_ref` with a full-length commit SHA lowercased, so every spelling of one
/// commit compares equal. Any other ref is returned as written: a tag or branch
/// is case-sensitive, and hex that is not full length names no commit. A SHA
/// already in lowercase, and every other ref, is borrowed, so the common path
/// does not allocate.
fn fold_full_sha(git_ref: &str) -> Cow<'_, str> {
    if is_full_hex_sha(git_ref) && git_ref.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Cow::Owned(git_ref.to_ascii_lowercase())
    } else {
        Cow::Borrowed(git_ref)
    }
}

/// Compile a wildcard pattern into tokens. A run of `*` of length one is a
/// `Star`; a longer run is a `DoubleStar`.
fn compile_glob(text: &str) -> Vec<Token> {
    let mut tokens = Vec::with_capacity(text.len());
    let mut bytes = text.bytes().peekable();
    while let Some(byte) = bytes.next() {
        if byte != b'*' {
            tokens.push(Token::Literal(byte));
            continue;
        }
        let mut stars = 1usize;
        while bytes.next_if_eq(&b'*').is_some() {
            stars += 1;
        }
        tokens.push(if stars == 1 {
            Token::Star
        } else {
            Token::DoubleStar
        });
    }
    tokens
}

/// Whether the whole of `text` matches `tokens`, by simulating the pattern's
/// automaton one byte at a time. Linear in `tokens * text`, with no
/// backtracking blow-up.
fn glob_matches(tokens: &[Token], text: &str, fold_case: bool) -> bool {
    // `states[i]`: the first `i` tokens can match the bytes consumed so far.
    let mut states = vec![false; tokens.len() + 1];
    states[0] = true;
    settle(tokens, &mut states);
    let mut next = vec![false; tokens.len() + 1];
    for byte in text.bytes() {
        next.fill(false);
        for (index, token) in tokens.iter().enumerate() {
            if !states[index] {
                continue;
            }
            match token {
                Token::Literal(literal) => {
                    if literal_matches(*literal, byte, fold_case) {
                        next[index + 1] = true;
                    }
                }
                Token::Star => {
                    if byte != b'/' {
                        next[index] = true;
                    }
                }
                Token::DoubleStar => next[index] = true,
            }
        }
        settle(tokens, &mut next);
        std::mem::swap(&mut states, &mut next);
        if !states.iter().any(|&reachable| reachable) {
            return false;
        }
    }
    states[tokens.len()]
}

/// A wildcard may match the empty run, so a state before it also reaches the
/// state after it.
fn settle(tokens: &[Token], states: &mut [bool]) {
    for (index, token) in tokens.iter().enumerate() {
        if states[index] && matches!(token, Token::Star | Token::DoubleStar) {
            states[index + 1] = true;
        }
    }
}

fn literal_matches(literal: u8, byte: u8, fold_case: bool) -> bool {
    if fold_case {
        literal.eq_ignore_ascii_case(&byte)
    } else {
        literal == byte
    }
}

impl Pattern {
    fn matches(&self, subject: &Subject<'_>) -> bool {
        match &self.matcher {
            Matcher::Any => true,
            Matcher::Repository {
                owner,
                repo,
                path,
                git_ref,
            } => {
                glob_matches(owner, subject.owner, true)
                    && glob_matches(repo, subject.repo, true)
                    && match (path, subject.path) {
                        (None, _) => true,
                        (Some(pattern), Some(actual)) => glob_matches(pattern, actual, false),
                        (Some(_), None) => false,
                    }
                    && git_ref
                        .as_ref()
                        .is_none_or(|pattern| glob_matches(pattern, subject.git_ref, false))
            }
        }
    }
}

fn has_allow_entries(patterns: &[Pattern]) -> bool {
    patterns.iter().any(|pattern| !pattern.negated)
}

/// The owner/repo[/path]@ref of a remote `uses:` reference.
#[derive(Debug, Clone, Copy)]
struct Subject<'a> {
    owner: &'a str,
    repo: &'a str,
    path: Option<&'a str>,
    git_ref: &'a str,
}

impl<'a> Subject<'a> {
    /// The subject as a step's action. GitHub documents an action entry as
    /// `OWNER/REPOSITORY@TAG-OR-SHA`, so a step's subpath takes no part in
    /// matching: `owner/repo/sub@v1` is the action `owner/repo@v1`. Only a
    /// reusable-workflow call carries a path that an entry can name.
    fn as_action(self) -> Self {
        Self { path: None, ..self }
    }
}

/// Where a `uses:` reference appears. Only a step's action may be lock-covered.
/// `resolve_remote_workflows` resolves a job-level reusable call live, so no
/// lock entry can vouch for the commit that runs.
#[derive(Clone, Copy)]
enum UseSite {
    /// A step's `uses:`.
    Step,
    /// A job-level `uses:`, which calls a reusable workflow.
    ReusableCall,
}

/// How a `uses:` reference is classified.
enum UsesKind<'a> {
    /// `./`, `../`, or `$/`: the workflow's own tree.
    Local,
    /// `docker://`: a container image, which no entry can name.
    Docker,
    /// `owner/repo[/path]@ref`.
    Remote(Subject<'a>),
    /// Anything else, including expressions and refs without `@`.
    Malformed,
}

impl<'a> UsesKind<'a> {
    /// The reference as a step's action, its subpath dropped.
    fn for_action(self) -> Self {
        match self {
            UsesKind::Remote(subject) => UsesKind::Remote(subject.as_action()),
            other => other,
        }
    }
}

fn classify(uses: &str) -> UsesKind<'_> {
    if uses.len() > MAX_ENTRY_LEN {
        return UsesKind::Malformed;
    }
    if uses.starts_with("./") || uses.starts_with("../") {
        return UsesKind::Local;
    }
    if let Some(rest) = uses.strip_prefix("$/") {
        // GitHub rejects a ref on a `$/` reference: it always resolves to the
        // running commit.
        return if rest.is_empty() || rest.contains('@') {
            UsesKind::Malformed
        } else {
            UsesKind::Local
        };
    }
    if uses.starts_with("docker://") {
        return UsesKind::Docker;
    }
    parse_remote(uses).map_or(UsesKind::Malformed, UsesKind::Remote)
}

/// Split at the first `@`, as GitHub's `uses:` parser does, so a ref may itself
/// contain `@`.
fn parse_remote(uses: &str) -> Option<Subject<'_>> {
    let (location, git_ref) = uses.split_once('@')?;
    if git_ref.is_empty() || !git_ref.chars().all(is_ref_char) {
        return None;
    }
    let mut parts = location.splitn(3, '/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    let path = parts.next();
    if !is_segment(owner) || !is_segment(repo) {
        return None;
    }
    if let Some(path) = path
        && !path.split('/').all(is_segment)
    {
        return None;
    }
    Some(Subject {
        owner,
        repo,
        path,
        git_ref,
    })
}

fn is_segment(segment: &str) -> bool {
    !segment.is_empty() && segment != "." && segment != ".." && segment.chars().all(is_name_char)
}

/// Whether `uses` may run under `patterns`. A locked dependency passes its
/// commit SHA as `locked_sha`, which GitHub matches instead of the written ref.
fn allow_list_verdict(
    patterns: &[Pattern],
    kind: &UsesKind<'_>,
    locked_sha: Option<&str>,
) -> Result<(), String> {
    match kind {
        UsesKind::Local => Ok(()),
        UsesKind::Docker if has_allow_entries(patterns) => Err(DOCKER_NOT_NAMEABLE.to_owned()),
        UsesKind::Docker => Ok(()),
        // A block entry cannot be checked against a reference it cannot read, so
        // any allow list, block-only lists included, refuses one.
        UsesKind::Malformed if !patterns.is_empty() => Err(MALFORMED_NOT_NAMEABLE.to_owned()),
        UsesKind::Malformed => Ok(()),
        UsesKind::Remote(subject) => {
            let effective = Subject {
                git_ref: locked_sha.unwrap_or(subject.git_ref),
                ..*subject
            };
            decide(patterns, &effective)
        }
    }
}

/// Apply the block and allow rules to one remote reference. Blocks win over
/// allows whatever the order. With no allow entry, anything unblocked passes. A
/// full-length commit SHA is compared in lowercase, so every spelling of one
/// commit meets the same entries.
fn decide(patterns: &[Pattern], subject: &Subject<'_>) -> Result<(), String> {
    let folded = fold_full_sha(subject.git_ref);
    let folded_subject = Subject {
        git_ref: folded.as_ref(),
        ..*subject
    };
    let mut allowed = !has_allow_entries(patterns);
    for pattern in patterns
        .iter()
        .filter(|pattern| pattern.matches(&folded_subject))
    {
        if pattern.negated {
            return Err(format!(
                "blocked by allowed_actions entry {:?}",
                pattern.entry
            ));
        }
        allowed = true;
    }
    if allowed {
        Ok(())
    } else {
        Err("not listed in allowed_actions".to_owned())
    }
}

/// One workflow file under evaluation.
struct Frame<'a> {
    /// The `reusable_workflows` key this file was loaded under, or `None` for the
    /// run's own workflow. The expander locates a remote callee's `./` and `$/`
    /// calls against it, so the policy does too.
    origin: Option<String>,
    /// Repository-relative path of the file, used for lock lookups.
    path: String,
    /// The name violations report for this file.
    display: String,
    /// The lock of the repository that defines this file.
    lock: Option<&'a ActionsLock>,
    /// Reusable-workflow nesting depth. The run's own workflow is 0.
    depth: usize,
}

/// State for one evaluation. Violations accumulate, so the operator sees every
/// offender at once.
struct Evaluation<'a> {
    policy: &'a ActionsPolicy,
    patterns: &'a [Pattern],
    reusable_workflows: &'a BTreeMap<String, String>,
    shas: &'a BTreeMap<String, String>,
    remote_locks: &'a BTreeMap<String, ActionsLock>,
    /// Callee keys already walked. A callee reached twice is checked once, which
    /// also bounds the work when many jobs call one workflow.
    walked: BTreeSet<String>,
    /// Violations already recorded, so a reference reached twice is reported
    /// once.
    reported: BTreeSet<(String, String, &'static str, String)>,
    violations: Vec<Violation>,
}

impl<'a> Evaluation<'a> {
    fn walk(&mut self, frame: &Frame<'a>, workflow: &preloop_gha_parser::Workflow) {
        for job in workflow.jobs.values() {
            if let Some(uses) = job.uses.as_deref() {
                self.check_reusable(frame, uses);
            }
            for step in &job.steps {
                if let Some(uses) = step.uses.as_deref() {
                    self.check_reference(frame, uses, UseSite::Step);
                }
            }
        }
    }

    /// The allow-list and SHA checks for one `uses:` reference in `frame`. A
    /// step's action is matched without its subpath; a reusable-workflow call
    /// keeps its path, which an entry may name.
    fn check_reference(&mut self, frame: &Frame<'a>, uses: &str, site: UseSite) {
        let kind = match site {
            UseSite::Step => classify(uses).for_action(),
            UseSite::ReusableCall => classify(uses),
        };
        let covered = match (&kind, site) {
            (UsesKind::Remote(_), UseSite::Step) => {
                covered_dependency(frame.lock, &frame.path, uses)
            }
            _ => None,
        };
        if self.policy.require_sha_pinning {
            match &kind {
                UsesKind::Local | UsesKind::Docker => {}
                UsesKind::Remote(subject) => {
                    if covered.is_none() && !is_commit_sha_not_zero(subject.git_ref) {
                        self.report(frame, uses, REQUIRE_SHA_PINNING, SHA_MESSAGE);
                    }
                }
                UsesKind::Malformed => {
                    self.report(frame, uses, REQUIRE_SHA_PINNING, MALFORMED_SHA_MESSAGE);
                }
            }
        }
        if !self.patterns.is_empty() {
            let locked = covered.map(|dependency| dependency.commit.as_git_sha());
            if let Err(reason) = allow_list_verdict(self.patterns, &kind, locked) {
                let message = if locked.is_some() {
                    format!("{reason} (checked by its locked commit SHA)")
                } else {
                    reason
                };
                self.report(frame, uses, ALLOWED_ACTIONS, message);
            }
        }
    }

    /// A job-level `uses:`: the reference itself, the callee the expander runs
    /// for it, and that callee's onboarding and contents.
    fn check_reusable(&mut self, frame: &Frame<'a>, uses: &str) {
        self.check_reference(frame, uses, UseSite::ReusableCall);
        if matches!(classify(uses), UsesKind::Docker | UsesKind::Malformed) {
            if self.policy.require_lockfile {
                self.report(frame, uses, REQUIRE_LOCKFILE, NOT_A_REPOSITORY_WORKFLOW);
            }
            return;
        }
        let Ok(key) = callee_reference(uses, frame.origin.as_deref(), self.shas) else {
            self.unverifiable(
                frame,
                uses,
                "its location cannot be derived from the calling remote workflow",
            );
            return;
        };
        // A `./` or `$/` call inherits the lock of the workflow that makes it,
        // which is the same repository at the same commit. A remote reference
        // takes the lock of the repository it names.
        let lock = if is_local_workflow_ref(uses) {
            frame.lock
        } else {
            let remote_locks: &'a BTreeMap<String, ActionsLock> = self.remote_locks;
            remote_locks.get(&key)
        };
        let (path, display) = callee_location(&key);
        let child = Frame {
            origin: (!is_local_workflow_ref(&key)).then(|| key.clone()),
            path,
            display,
            lock,
            depth: frame.depth + 1,
        };
        if self.policy.require_lockfile && !is_onboarded(child.lock, &child.path) {
            self.report(
                frame,
                uses,
                REQUIRE_LOCKFILE,
                lockfile_message(child.lock.is_some(), true),
            );
        }
        self.descend(frame, uses, &key, child);
    }

    /// Walk the callee `key` names, unless it was walked already or lies beyond
    /// the depth bound. Content that cannot be read is a violation.
    fn descend(&mut self, frame: &Frame<'a>, uses: &str, key: &str, child: Frame<'a>) {
        if child.depth > DECLARED_ACTION_REUSABLE_DEPTH {
            self.unverifiable(frame, uses, "nesting is deeper than the supported limit");
            return;
        }
        if !self.walked.insert(key.to_owned()) {
            return;
        }
        let reusable_workflows: &'a BTreeMap<String, String> = self.reusable_workflows;
        let Some(yaml) = callee_yaml(key, reusable_workflows) else {
            self.unverifiable(
                frame,
                uses,
                "no workflow content was submitted for this reference",
            );
            return;
        };
        let Ok(callee) = preloop_gha_parser::parse_workflow(yaml) else {
            self.unverifiable(frame, uses, "the submitted workflow content does not parse");
            return;
        };
        self.walk(&child, &callee);
    }

    /// A callee whose contents cannot be checked fails every rule that needs
    /// them.
    fn unverifiable(&mut self, frame: &Frame<'a>, uses: &str, reason: &str) {
        for rule in self.enabled_rules() {
            self.report(
                frame,
                uses,
                rule,
                format!("cannot check the workflow behind this reference: {reason}"),
            );
        }
    }

    fn enabled_rules(&self) -> Vec<&'static str> {
        let mut rules = Vec::with_capacity(3);
        if !self.patterns.is_empty() {
            rules.push(ALLOWED_ACTIONS);
        }
        if self.policy.require_sha_pinning {
            rules.push(REQUIRE_SHA_PINNING);
        }
        if self.policy.require_lockfile {
            rules.push(REQUIRE_LOCKFILE);
        }
        rules
    }

    fn report(
        &mut self,
        frame: &Frame<'a>,
        uses: &str,
        rule: &'static str,
        message: impl Into<String>,
    ) {
        let message: String = message.into();
        let key = (
            frame.display.clone(),
            uses.to_owned(),
            rule,
            message.clone(),
        );
        if self.reported.insert(key) {
            self.violations.push(Violation {
                workflow_path: frame.display.clone(),
                uses: uses.to_owned(),
                rule,
                message,
            });
        }
    }
}

/// The locked dependency that covers `uses` in the workflow at `path`: the
/// workflow must be onboarded, and its `workflows` entry must list the pin.
fn covered_dependency<'l>(
    lock: Option<&'l ActionsLock>,
    path: &str,
    uses: &str,
) -> Option<&'l LockedDependency> {
    let lock = lock?;
    if !lock.is_onboarded(path) {
        return None;
    }
    let key = pin_key_for_uses(uses)?;
    if !lock.workflow_pins(path)?.contains(&key) {
        return None;
    }
    lock.dependency(&key)
}

fn is_onboarded(lock: Option<&ActionsLock>, path: &str) -> bool {
    lock.is_some_and(|lock| lock.is_onboarded(path))
}

fn lockfile_message(lock_present: bool, callee: bool) -> String {
    match (lock_present, callee) {
        (false, false) => {
            "this repository has no .github/workflows/actions.lock; onboard the workflow with \
             `gh actions-lock`"
                .to_owned()
        }
        (true, false) => {
            "the workflow is not onboarded in .github/workflows/actions.lock; onboard it with \
             `gh actions-lock`"
                .to_owned()
        }
        (false, true) => {
            "the repository that defines this reusable workflow has no actions.lock; onboard it \
             with `gh actions-lock`"
                .to_owned()
        }
        (true, true) => {
            "this reusable workflow is not onboarded in its repository's actions.lock; onboard it \
             with `gh actions-lock`"
                .to_owned()
        }
    }
}

/// The repository-relative path a reusable key names, and the name violations
/// report for it: `owner/repo/path` for a remote key, the path itself otherwise.
fn callee_location(key: &str) -> (String, String) {
    let normalized = normalize_reusable_path(key);
    if is_local_workflow_ref(key) {
        return (normalized.clone(), normalized);
    }
    let path = normalized
        .splitn(3, '/')
        .nth(2)
        .unwrap_or_default()
        .to_owned();
    (path, normalized)
}

/// The callee YAML the expander runs for `key`: the key itself, else its
/// normalized repository path.
fn callee_yaml<'m>(
    key: &str,
    reusable_workflows: &'m BTreeMap<String, String>,
) -> Option<&'m String> {
    reusable_workflows
        .get(key)
        .or_else(|| reusable_workflows.get(&normalize_reusable_path(key)))
}

/// `path` without a leading `./` or `$/`, with `.` components collapsed: the
/// run's own workflow path, as lock lookups key it.
fn repo_relative(path: &str) -> String {
    let relative = path
        .strip_prefix("./")
        .or_else(|| path.strip_prefix("$/"))
        .unwrap_or(path);
    Path::new(relative)
        .components()
        .collect::<PathBuf>()
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const CI: &str = ".github/workflows/ci.yml";
    const SHA_A: &str = concat!("1111111111", "1111111111", "1111111111", "1111111111");
    const SHA_B: &str = concat!("2222222222", "2222222222", "2222222222", "2222222222");
    const ZERO_SHA: &str = concat!("0000000000", "0000000000", "0000000000", "0000000000");
    const REMOTE: &str = "octo/shared/.github/workflows/build.yml";
    const BUILD_V1: &str = "owner/shared/.github/workflows/build.yml@v1";
    const SHA_C: &str = concat!("abcdef0123456789", "abcdef0123456789", "abcdef01");
    const SHA_D: &str = concat!("0f1e2d3c4b5a6978", "0f1e2d3c4b5a6978", "0f1e2d3c");
    const SHA256_C: &str = concat!(
        "abcdef0123456789",
        "abcdef0123456789",
        "abcdef0123456789",
        "abcdef0123456789",
    );

    /// One job whose steps use each reference in `uses`, in order.
    fn steps_workflow(uses: &[&str]) -> String {
        let mut yaml =
            String::from("on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n");
        for reference in uses {
            yaml.push_str(&format!("      - uses: \"{reference}\"\n"));
        }
        yaml
    }

    /// A reusable workflow whose steps use each reference in `uses`.
    fn callee_workflow(uses: &[&str]) -> String {
        steps_workflow(uses).replacen("on: push", "on: workflow_call", 1)
    }

    /// A caller workflow with one job that calls the reusable workflow `uses`.
    fn caller_workflow(uses: &str) -> String {
        format!("on: push\njobs:\n  call:\n    uses: \"{uses}\"\n")
    }

    /// An `actions.lock` built from `(workflow path, pin keys)` and
    /// `(pin key, commit SHA)` pairs.
    fn lock_file(workflows: &[(&str, &[&str])], dependencies: &[(&str, &str)]) -> ActionsLock {
        let mut yaml = String::from("version: v0.0.3\n");
        if workflows.is_empty() {
            yaml.push_str("workflows: {}\n");
        } else {
            yaml.push_str("workflows:\n");
        }
        for (path, pins) in workflows {
            if pins.is_empty() {
                yaml.push_str(&format!("  {path}: []\n"));
            } else {
                yaml.push_str(&format!("  {path}:\n"));
                for pin in *pins {
                    yaml.push_str(&format!("    - {pin}\n"));
                }
            }
        }
        if dependencies.is_empty() {
            yaml.push_str("dependencies: {}\n");
        } else {
            yaml.push_str("dependencies:\n");
        }
        for (key, sha) in dependencies {
            let reference = key.split_once('@').map_or("", |(_, reference)| reference);
            let entry = format!("  {key}:\n    ref: {reference}\n    commit: sha1-{sha}\n");
            yaml.push_str(&entry);
            yaml.push_str("    owner_id: 1\n    repo_id: 2\n");
        }
        ActionsLock::parse(&yaml).expect("fixture actions.lock parses")
    }

    struct Fixture {
        path: &'static str,
        workflow: preloop_gha_parser::Workflow,
        reusable: BTreeMap<String, String>,
        shas: BTreeMap<String, String>,
        lock: Option<ActionsLock>,
        remote: BTreeMap<String, ActionsLock>,
    }

    impl Fixture {
        fn new(workflow_yaml: &str) -> Self {
            Self {
                path: CI,
                workflow: preloop_gha_parser::parse_workflow(workflow_yaml)
                    .expect("fixture workflow parses"),
                reusable: BTreeMap::new(),
                shas: BTreeMap::new(),
                lock: None,
                remote: BTreeMap::new(),
            }
        }

        fn evaluate(&self, policy: &ActionsPolicy) -> Result<(), PolicyViolation> {
            policy.evaluate(&PolicyInputs {
                workflow_path: self.path,
                workflow: &self.workflow,
                reusable_workflows: &self.reusable,
                reusable_workflow_shas: &self.shas,
                lock: self.lock.as_ref(),
                remote_locks: &self.remote,
            })
        }
    }

    /// `(workflow_path, uses, rule)` for each violation, in report order.
    fn offenders(result: Result<(), PolicyViolation>) -> Vec<(String, String, &'static str)> {
        result.err().map_or_else(Vec::new, |error| {
            error
                .violations
                .into_iter()
                .map(|violation| (violation.workflow_path, violation.uses, violation.rule))
                .collect()
        })
    }

    fn with_allow_list(entries: &[&str]) -> ActionsPolicy {
        ActionsPolicy {
            allowed_actions: entries.iter().map(|entry| (*entry).to_owned()).collect(),
            ..ActionsPolicy::default()
        }
    }

    fn sha_policy() -> ActionsPolicy {
        ActionsPolicy {
            require_sha_pinning: true,
            ..ActionsPolicy::default()
        }
    }

    fn lockfile_policy() -> ActionsPolicy {
        ActionsPolicy {
            require_lockfile: true,
            ..ActionsPolicy::default()
        }
    }

    #[test]
    fn defaults_are_inert_except_for_commit_reachability() {
        let policy = ActionsPolicy::default();
        assert!(!policy.require_sha_pinning);
        assert!(!policy.require_lockfile);
        assert!(policy.verify_commit_reachability);
        assert!(policy.allowed_actions.is_empty());

        // With every rule off, even malformed references pass unexamined.
        let fixture = Fixture::new(&steps_workflow(&[
            "${{ inputs.action }}",
            "docker://alpine:3",
        ]));
        assert!(offenders(fixture.evaluate(&policy)).is_empty());
        assert!(policy.check_uses("${{ inputs.action }}").is_ok());
    }

    #[test]
    fn entry_grammar_accepts_the_documented_forms() {
        for entry in [
            "*",
            "**",
            "!*",
            "*/*",
            "owner/*",
            "owner/repo",
            "owner/repo@v1",
            "owner/repo@*",
            "owner/repo@**",
            "owner*/*",
            "*/octocat*@*",
            "!owner/repo@feature/x",
            "owner/repo@release/*",
            "owner/repo@release@2024",
            "owner/repo/.github/workflows/build.yml@v1",
            "owner/repo/dir/action@v1.2.3",
            "owner/repo@v1!x",
            "!owner/repo@v1#x",
            "owner/repo@v1%2",
            "owner/repo@a=b",
        ] {
            let result = parse_pattern(entry);
            assert!(result.is_ok(), "{entry:?} should be accepted: {result:?}");
        }
    }

    #[test]
    fn entry_grammar_rejects_malformed_entries() {
        for entry in [
            "",
            "!",
            "!!owner/repo",
            "owner",
            "owner/",
            "/repo",
            "owner//repo",
            "owner/repo/",
            "owner/repo//x",
            "owner/repo@",
            "owner/re po",
            "owner/repo?",
            "owner/[ab]",
            "owner/repo, other/repo",
            "owner/./x",
            "../repo",
            "docker://alpine:3.19",
            "!docker://alpine",
            "owner/repo@v1,x",
            "owner/repo@v?",
            "owner/repo@v[1]",
            "owner/repo@v1 x",
            "owner/repo@v\\1",
        ] {
            let result = parse_pattern(entry);
            assert!(result.is_err(), "{entry:?} should be rejected");
        }
    }

    /// `(entry, uses, allowed)`: one entry, one step's `uses:`, and the verdict.
    /// A step's subpath takes no part: the action is its repository and ref.
    const ACTION_ENTRY_CASES: &[(&str, &str, bool)] = &[
        ("actions/checkout@v4", "actions/checkout@v4", true),
        ("actions/checkout@v4", "actions/checkout@v3", false),
        // owner and repo are case-insensitive, the ref is not.
        ("actions/checkout@v4", "ACTIONS/Checkout@v4", true),
        ("actions/checkout@v4", "actions/checkout@V4", false),
        // no ref in the entry: every ref matches.
        ("actions/checkout", "actions/checkout@v3", true),
        ("actions/*", "actions/setup-node@v4", true),
        ("actions/*", "other/checkout@v4", false),
        ("actions/checkout@*", "actions/checkout@v9", true),
        // `*` does not cross `/`; `**` does.
        ("actions/checkout@*", "actions/checkout@release/1", false),
        ("actions/checkout@**", "actions/checkout@release/1", true),
        ("*", "any/where/deep@x", true),
        // a step's subpath takes no part: `init` is the same action as its repository.
        (
            "github/codeql-action@*",
            "github/codeql-action/init@v3",
            true,
        ),
        ("actions/*", "actions/checkout/sub@v1", true),
        // an entry with a path names a reusable workflow, so it never names a step.
        (
            "github/codeql-action/init@*",
            "github/codeql-action/init@v3",
            false,
        ),
        (
            "owner/*/.github/workflows/*.yml@v1",
            "owner/repo/.github/workflows/build.yml@v1",
            false,
        ),
        ("space-org*/*", "space-org-dev/tool@v1", true),
        ("*/octocat*@*", "someone/octocat-fork@v1", true),
        ("*/octocat*@*", "someone/other@v1", false),
    ];

    #[test]
    fn allow_entries_match_a_step_by_its_repository_and_ref_as_github_does() {
        for &(entry, uses, expected) in ACTION_ENTRY_CASES {
            let verdict = with_allow_list(&[entry]).check_uses(uses).is_ok();
            assert_eq!(verdict, expected, "entry {entry:?} against {uses:?}");
        }
    }

    /// `(entries, reusable reference, allowed)`: a job-level call to a remote
    /// reusable workflow, the only place an entry's path applies.
    const REUSABLE_CASES: &[(&[&str], &str, bool)] = &[
        (
            &["owner/shared/.github/workflows/build.yml@v1"],
            BUILD_V1,
            true,
        ),
        // an entry without a path covers every path of its repository (Preloop's reading).
        (&["owner/shared"], BUILD_V1, true),
        (&["owner/*"], BUILD_V1, true),
        (&["owner/shared/.github/workflows/*.yml@v1"], BUILD_V1, true),
        (
            &["owner/shared/.github/workflows/*.yml@v2"],
            BUILD_V1,
            false,
        ),
        // without a ref in the entry, every ref matches.
        (&["owner/shared/.github/workflows/*.yml"], BUILD_V1, true),
        (&["owner/shared/build.yml@v1"], BUILD_V1, false),
        (&["owner/shared@v2"], BUILD_V1, false),
        // blocks override allows, and a block with a path names the call.
        (&["*", "!owner/shared"], BUILD_V1, false),
        (&["!owner/shared/.github/workflows/*.yml"], BUILD_V1, false),
    ];

    #[test]
    fn a_reusable_call_is_matched_by_its_path_as_well_as_its_repository() {
        for &(entries, uses, expected) in REUSABLE_CASES {
            let mut fixture = Fixture::new(&caller_workflow(uses));
            fixture.reusable.insert(
                uses.to_owned(),
                callee_workflow(&["./.github/actions/local"]),
            );
            let verdict = offenders(fixture.evaluate(&with_allow_list(entries))).is_empty();
            assert_eq!(verdict, expected, "entries {entries:?} against {uses:?}");
        }
    }

    #[test]
    fn a_path_entry_never_names_a_step() {
        let uses = "owner/shared/.github/workflows/build.yml@v1";
        let fixture = Fixture::new(&steps_workflow(&[uses]));
        let path_entry = with_allow_list(&["owner/shared/.github/workflows/*.yml@v1"]);
        assert_eq!(
            offenders(fixture.evaluate(&path_entry)),
            vec![(CI.to_owned(), uses.to_owned(), ALLOWED_ACTIONS)],
        );
        assert!(offenders(fixture.evaluate(&with_allow_list(&["owner/shared@v1"]))).is_empty());
    }

    /// `(entries, uses, allowed)`: several entries, where blocks override allows.
    const BLOCK_CASES: &[(&[&str], &str, bool)] = &[
        (
            &["actions/*", "!actions/checkout@v4"],
            "actions/checkout@v4",
            false,
        ),
        (
            &["actions/*", "!actions/checkout@v4"],
            "actions/checkout@v3",
            true,
        ),
        // order does not matter: a block still wins over an earlier allow.
        (
            &["!actions/checkout@v4", "actions/*"],
            "actions/checkout@v4",
            false,
        ),
        (
            &["actions/checkout@v4", "!actions/checkout@v4"],
            "actions/checkout@v4",
            false,
        ),
        (&["*", "!space-org/action@*"], "space-org/action@v1", false),
        (&["*", "!space-org/action@*"], "space-org/other@v1", true),
        // a block-only list allows everything it does not block.
        (&["!evil/*"], "evil/tool@v1", false),
        (&["!evil/*"], "good/tool@v1", true),
        (&["!evil/*"], "./local", true),
        // docker images cannot be named, so a block-only list leaves them be.
        (&["!evil/*"], "docker://alpine:3.19", true),
        // local references are always allowed.
        (&["actions/*"], "./.github/actions/local", true),
        (&["actions/*"], "$/.github/actions/local", true),
        (&["actions/*"], "../shared", true),
        // an allow list blocks docker images, which no entry can name.
        (&["*"], "docker://alpine:3.19", false),
        // references that do not parse match nothing under an allow list.
        (&["actions/*"], "${{ inputs.action }}", false),
        (&["actions/*"], "actions/checkout", false),
        // a block entry names the repository, so its subpath blocks a step too.
        (&["!evil/action"], "evil/action/sub@v1", false),
        // a ref containing `!` is a ref, and the block matches it.
        (&["!evil/action"], "evil/action@release!1", false),
        // an unparseable reference is refused by a block-only list.
        (&["!evil/action"], "evil/action@release\\1", false),
        (&["!evil/action"], "evil/action", false),
    ];

    #[test]
    fn block_entries_override_allows_whatever_the_order() {
        for &(entries, uses, expected) in BLOCK_CASES {
            let verdict = with_allow_list(entries).check_uses(uses).is_ok();
            assert_eq!(verdict, expected, "entries {entries:?} against {uses:?}");
        }
    }

    #[test]
    fn a_block_entry_matches_a_ref_that_contains_a_bang() {
        // `!` is a legal ref character, so the reference parses and the block applies.
        let policy = with_allow_list(&["!evil/action"]);
        assert!(policy.check_uses("evil/action@release!1").is_err());
        let fixture = Fixture::new(&steps_workflow(&["evil/action@release!1"]));
        assert_eq!(
            offenders(fixture.evaluate(&policy)),
            vec![(
                CI.to_owned(),
                "evil/action@release!1".to_owned(),
                ALLOWED_ACTIONS
            )],
        );
    }

    #[test]
    fn a_block_only_list_refuses_a_reference_it_cannot_parse() {
        // A backslash makes the ref unparseable. A block entry cannot be checked
        // against it, so a block-only list refuses it rather than let it through.
        let policy = with_allow_list(&["!evil/action"]);
        let error = policy
            .check_uses("evil/action@release\\1")
            .expect_err("an unparseable reference is refused");
        assert!(
            error
                .message
                .contains("cannot be checked against allowed_actions"),
            "{}",
            error.message
        );
        // The scalar is single-quoted, so the backslash reaches the parser literally.
        let fixture = Fixture::new(concat!(
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n",
            "    steps:\n      - uses: 'evil/action@release\\1'\n",
        ));
        assert_eq!(
            offenders(fixture.evaluate(&policy)),
            vec![(
                CI.to_owned(),
                "evil/action@release\\1".to_owned(),
                ALLOWED_ACTIONS
            )],
        );
        // Docker images are refused only while an allow entry exists.
        assert!(policy.check_uses("docker://alpine:3.19").is_ok());
    }

    #[test]
    fn an_empty_allow_list_checks_nothing_at_resolve_time() {
        assert!(ActionsPolicy::default().check_uses("evil/tool@v1").is_ok());
        assert!(
            with_allow_list(&["actions/*"])
                .check_uses("./local")
                .is_ok()
        );
    }

    #[test]
    fn check_uses_matches_the_form_it_is_given() {
        // A lock-covered dependency is passed as `owner/repo@<locked sha>`: a tag
        // entry does not match its commit, while a SHA entry and a wildcard do.
        let locked = format!("actions/checkout@{SHA_A}");
        assert!(
            with_allow_list(&["actions/checkout@v4"])
                .check_uses(&locked)
                .is_err()
        );
        assert!(
            with_allow_list(&[locked.as_str()])
                .check_uses(&locked)
                .is_ok()
        );
        assert!(with_allow_list(&["actions/*"]).check_uses(&locked).is_ok());
    }

    #[test]
    fn a_block_entry_for_a_full_sha_blocks_every_spelling_of_that_commit() {
        let upper = SHA_C.to_ascii_uppercase();
        let entry = format!("!evil/action@{SHA_C}");
        let policy = with_allow_list(&[entry.as_str()]);
        assert!(policy.check_uses(&format!("evil/action@{SHA_C}")).is_err());
        assert!(policy.check_uses(&format!("evil/action@{upper}")).is_err());
        let uses = format!("evil/action@{upper}");
        let fixture = Fixture::new(&steps_workflow(&[uses.as_str()]));
        assert_eq!(
            offenders(fixture.evaluate(&policy)),
            vec![(CI.to_owned(), uses, ALLOWED_ACTIONS)],
        );
    }

    #[test]
    fn a_block_entry_spelled_in_uppercase_blocks_the_lowercase_commit() {
        let upper = SHA_C.to_ascii_uppercase();
        let entry = format!("!evil/action@{upper}");
        let policy = with_allow_list(&[entry.as_str()]);
        assert!(policy.check_uses(&format!("evil/action@{SHA_C}")).is_err());
        let uses = format!("evil/action@{SHA_C}");
        let fixture = Fixture::new(&steps_workflow(&[uses.as_str()]));
        assert_eq!(
            offenders(fixture.evaluate(&policy)),
            vec![(CI.to_owned(), uses, ALLOWED_ACTIONS)],
        );
    }

    #[test]
    fn an_allow_entry_for_a_full_sha_admits_every_spelling_of_that_commit() {
        let upper = SHA_C.to_ascii_uppercase();
        for entry in [format!("owner/repo@{SHA_C}"), format!("owner/repo@{upper}")] {
            let policy = with_allow_list(&[entry.as_str()]);
            assert!(
                policy.check_uses(&format!("owner/repo@{SHA_C}")).is_ok(),
                "{entry}"
            );
            assert!(
                policy.check_uses(&format!("owner/repo@{upper}")).is_ok(),
                "{entry}"
            );
            assert!(
                policy.check_uses(&format!("owner/repo@{SHA_D}")).is_err(),
                "{entry}"
            );
        }
        let entry = format!("owner/repo@{SHA_C}");
        let uses = format!("owner/repo@{upper}");
        let fixture = Fixture::new(&steps_workflow(&[uses.as_str()]));
        assert!(offenders(fixture.evaluate(&with_allow_list(&[entry.as_str()]))).is_empty());
    }

    #[test]
    fn a_sha256_commit_is_folded_the_same_way() {
        let upper = SHA256_C.to_ascii_uppercase();
        let block = format!("!evil/action@{upper}");
        let blocking = with_allow_list(&[block.as_str()]);
        assert!(
            blocking
                .check_uses(&format!("evil/action@{SHA256_C}"))
                .is_err()
        );
        let uses = format!("evil/action@{SHA256_C}");
        let fixture = Fixture::new(&steps_workflow(&[uses.as_str()]));
        assert_eq!(
            offenders(fixture.evaluate(&blocking)),
            vec![(CI.to_owned(), uses, ALLOWED_ACTIONS)],
        );
        let allow = format!("owner/repo@{SHA256_C}");
        let allowing = with_allow_list(&[allow.as_str()]);
        assert!(allowing.check_uses(&format!("owner/repo@{upper}")).is_ok());
        assert!(allowing.check_uses(&format!("owner/repo@{SHA_C}")).is_err());
    }

    #[test]
    fn tags_and_branches_stay_case_sensitive_even_when_they_look_like_hex() {
        assert!(
            with_allow_list(&["!owner/repo@V4"])
                .check_uses("owner/repo@v4")
                .is_ok()
        );
        assert!(
            with_allow_list(&["owner/repo@V4"])
                .check_uses("owner/repo@v4")
                .is_err()
        );
        // Hex that is not full length names no commit, so its case is kept.
        assert!(
            with_allow_list(&["owner/repo@ABC123"])
                .check_uses("owner/repo@abc123")
                .is_err()
        );
        assert!(
            with_allow_list(&["owner/repo@ABC123"])
                .check_uses("owner/repo@ABC123")
                .is_ok()
        );
    }
    #[test]
    fn the_sha_rule_without_a_lock_needs_full_commit_shas_except_exempt_refs() {
        let policy = sha_policy();
        let pinned = format!("actions/checkout@{SHA_A}");
        let zero = format!("actions/checkout@{ZERO_SHA}");
        let cases = [
            ("actions/checkout@v4", false),
            (pinned.as_str(), true),
            (zero.as_str(), false),
            ("actions/checkout", false),
            ("${{ inputs.action }}", false),
            ("./.github/actions/local", true),
            ("../shared/action", true),
            ("$/.github/actions/self", true),
            ("docker://alpine@sha256:abc", true),
        ];
        for (uses, expected_ok) in cases {
            let fixture = Fixture::new(&steps_workflow(&[uses]));
            assert_eq!(
                offenders(fixture.evaluate(&policy)).is_empty(),
                expected_ok,
                "{uses:?}"
            );
        }
    }

    #[test]
    fn the_sha_rule_with_a_lock_accepts_locked_pins_in_onboarded_workflows_only() {
        let policy = sha_policy();
        let mut fixture = Fixture::new(&steps_workflow(&[
            "actions/checkout@v4",
            "actions/setup-node@v4",
        ]));
        fixture.lock = Some(lock_file(
            &[(CI, &["actions/checkout@v4"])],
            &[("actions/checkout@v4", SHA_A)],
        ));
        assert_eq!(
            offenders(fixture.evaluate(&policy)),
            vec![(
                CI.to_owned(),
                "actions/setup-node@v4".to_owned(),
                REQUIRE_SHA_PINNING
            )],
            "the locked pin passes; the unlocked tag does not"
        );

        // The same pin in a workflow the lock does not onboard is not covered.
        fixture.path = ".github/workflows/other.yml";
        assert_eq!(offenders(fixture.evaluate(&policy)).len(), 2);

        // A `./` prefix names the same workflow.
        fixture.path = "./.github/workflows/ci.yml";
        assert_eq!(offenders(fixture.evaluate(&policy)).len(), 1);
    }

    #[test]
    fn job_level_reusable_references_need_a_full_sha_even_when_locked() {
        let policy = sha_policy();
        let action = format!("actions/checkout@{SHA_A}");

        let pinned = format!("owner/shared/.github/workflows/build.yml@{SHA_A}");
        let mut fixture = Fixture::new(&caller_workflow(&pinned));
        fixture
            .reusable
            .insert(pinned.clone(), callee_workflow(&[action.as_str()]));
        assert!(
            offenders(fixture.evaluate(&policy)).is_empty(),
            "a SHA-pinned call with a SHA-pinned callee passes"
        );

        let tag = "owner/shared/.github/workflows/build.yml@v1";
        let mut tagged = Fixture::new(&caller_workflow(tag));
        tagged
            .reusable
            .insert(tag.to_owned(), callee_workflow(&[action.as_str()]));
        assert_eq!(
            offenders(tagged.evaluate(&policy)),
            vec![(CI.to_owned(), tag.to_owned(), REQUIRE_SHA_PINNING)]
        );

        // Remote reusable calls resolve live, so the caller's lock cannot cover one,
        // even when it lists the repository.
        tagged.lock = Some(lock_file(
            &[(CI, &["owner/shared@v1"])],
            &[("owner/shared@v1", SHA_B)],
        ));
        assert_eq!(
            offenders(tagged.evaluate(&policy)),
            vec![(CI.to_owned(), tag.to_owned(), REQUIRE_SHA_PINNING)],
            "a reusable workflow reference is never covered by a lock entry"
        );
    }

    #[test]
    fn a_job_level_call_is_never_lock_covered_even_when_its_pin_key_is_locked() {
        // `pin_key_for_uses` maps this call to a locked key, but a job-level call
        // resolves live, so no lock entry may cover it.
        let action = format!("actions/checkout@{SHA_A}");
        let mut fixture = Fixture::new(&caller_workflow("owner/shared@v1"));
        fixture.reusable.insert(
            "owner/shared@v1".to_owned(),
            callee_workflow(&[action.as_str()]),
        );
        fixture.lock = Some(lock_file(
            &[(CI, &["owner/shared@v1"])],
            &[("owner/shared@v1", SHA_B)],
        ));
        assert_eq!(
            offenders(fixture.evaluate(&sha_policy())),
            vec![(
                CI.to_owned(),
                "owner/shared@v1".to_owned(),
                REQUIRE_SHA_PINNING
            )],
        );
    }

    #[test]
    fn require_lockfile_onboards_the_run_and_every_reusable_workflow_it_calls() {
        let policy = lockfile_policy();
        let mut fixture = Fixture::new(&steps_workflow(&["actions/checkout@v4"]));
        assert_eq!(
            offenders(fixture.evaluate(&policy)),
            vec![(CI.to_owned(), String::new(), REQUIRE_LOCKFILE)],
            "no actions.lock at all"
        );
        fixture.lock = Some(lock_file(&[(CI, &[])], &[]));
        assert!(offenders(fixture.evaluate(&policy)).is_empty());

        // A local callee must be onboarded in the run repository's lock.
        let mut local = Fixture::new(&caller_workflow("./.github/workflows/callee.yml"));
        local.reusable.insert(
            ".github/workflows/callee.yml".to_owned(),
            callee_workflow(&["actions/checkout@v4"]),
        );
        local.lock = Some(lock_file(&[(CI, &[])], &[]));
        assert_eq!(
            offenders(local.evaluate(&policy)),
            vec![(
                CI.to_owned(),
                "./.github/workflows/callee.yml".to_owned(),
                REQUIRE_LOCKFILE
            )]
        );
        local.lock = Some(lock_file(
            &[(CI, &[]), (".github/workflows/callee.yml", &[])],
            &[],
        ));
        assert!(offenders(local.evaluate(&policy)).is_empty());

        // A remote callee must be onboarded in its own repository's lock.
        let uses = "owner/shared/.github/workflows/build.yml@v1";
        let mut remote = Fixture::new(&caller_workflow(uses));
        remote
            .reusable
            .insert(uses.to_owned(), callee_workflow(&["actions/checkout@v4"]));
        remote.lock = Some(lock_file(&[(CI, &[])], &[]));
        assert_eq!(
            offenders(remote.evaluate(&policy)),
            vec![(CI.to_owned(), uses.to_owned(), REQUIRE_LOCKFILE)]
        );
        remote.remote.insert(
            uses.to_owned(),
            lock_file(&[(".github/workflows/build.yml", &[])], &[]),
        );
        assert!(offenders(remote.evaluate(&policy)).is_empty());
    }

    #[test]
    fn a_local_callee_is_checked_under_the_callers_rules() {
        let policy = with_allow_list(&["actions/*"]);
        let mut fixture = Fixture::new(&caller_workflow("./.github/workflows/callee.yml"));
        fixture.reusable.insert(
            ".github/workflows/callee.yml".to_owned(),
            callee_workflow(&["actions/checkout@v4", "evil/tool@v1"]),
        );
        assert_eq!(
            offenders(fixture.evaluate(&policy)),
            vec![(
                ".github/workflows/callee.yml".to_owned(),
                "evil/tool@v1".to_owned(),
                ALLOWED_ACTIONS
            )],
            "the offender is named by the file that contains it"
        );
    }

    #[test]
    fn a_remote_callee_is_checked_against_its_own_repository_lock() {
        let uses = format!("owner/shared/.github/workflows/build.yml@{SHA_A}");
        let mut fixture = Fixture::new(&caller_workflow(&uses));
        fixture
            .reusable
            .insert(uses.clone(), callee_workflow(&["actions/checkout@v4"]));
        let policy = sha_policy();
        assert_eq!(
            offenders(fixture.evaluate(&policy)),
            vec![(
                "owner/shared/.github/workflows/build.yml".to_owned(),
                "actions/checkout@v4".to_owned(),
                REQUIRE_SHA_PINNING
            )],
            "no lock for the callee's repository: its tag is unpinned"
        );
        fixture.remote.insert(
            uses.clone(),
            lock_file(
                &[(".github/workflows/build.yml", &["actions/checkout@v4"])],
                &[("actions/checkout@v4", SHA_B)],
            ),
        );
        assert!(offenders(fixture.evaluate(&policy)).is_empty());
    }

    #[test]
    fn a_remote_callees_local_call_is_checked_in_its_repository_at_its_pin() {
        // The run repository has a benign file at the same path. The callee's `./`
        // call must reach the remote's own inner workflow, which carries the
        // offender, and never the run's file.
        let callee = format!("{REMOTE}@{SHA_A}");
        let benign = format!("actions/checkout@{SHA_B}");
        let mut fixture = Fixture::new(&caller_workflow(&callee));
        fixture.reusable.insert(
            callee.clone(),
            caller_workflow("./.github/workflows/inner.yml"),
        );
        fixture.reusable.insert(
            format!("octo/shared/.github/workflows/inner.yml@{SHA_A}"),
            callee_workflow(&["evil/x@v1"]),
        );
        fixture.reusable.insert(
            ".github/workflows/inner.yml".to_owned(),
            callee_workflow(&[benign.as_str()]),
        );
        fixture.shas.insert(callee, SHA_A.to_owned());
        assert_eq!(
            offenders(fixture.evaluate(&sha_policy())),
            vec![(
                "octo/shared/.github/workflows/inner.yml".to_owned(),
                "evil/x@v1".to_owned(),
                REQUIRE_SHA_PINNING,
            )],
        );
    }

    #[test]
    fn a_remote_callees_local_call_fails_closed_without_the_callee_it_names() {
        // The run's own file at the same path must not stand in for the remote's.
        let callee = format!("{REMOTE}@{SHA_A}");
        let benign = format!("actions/checkout@{SHA_B}");
        let mut fixture = Fixture::new(&caller_workflow(&callee));
        fixture.reusable.insert(
            callee.clone(),
            caller_workflow("./.github/workflows/inner.yml"),
        );
        fixture.reusable.insert(
            ".github/workflows/inner.yml".to_owned(),
            callee_workflow(&[benign.as_str()]),
        );
        fixture.shas.insert(callee, SHA_A.to_owned());
        assert_eq!(
            offenders(fixture.evaluate(&sha_policy())),
            vec![(
                REMOTE.to_owned(),
                "./.github/workflows/inner.yml".to_owned(),
                REQUIRE_SHA_PINNING,
            )],
        );
    }

    #[test]
    fn a_remote_callees_local_call_is_keyed_by_the_commit_it_resolved_to() {
        // The run names a tag that the callee resolved to SHA_A. The inner workflow
        // at that commit carries the offender; a decoy under the written tag must
        // not be used.
        let written = format!("{REMOTE}@v1");
        let mut fixture = Fixture::new(&caller_workflow(&written));
        fixture.reusable.insert(
            written.clone(),
            caller_workflow("./.github/workflows/inner.yml"),
        );
        fixture.reusable.insert(
            format!("octo/shared/.github/workflows/inner.yml@{SHA_A}"),
            callee_workflow(&["evil/x@v1"]),
        );
        fixture.reusable.insert(
            "octo/shared/.github/workflows/inner.yml@v1".to_owned(),
            callee_workflow(&["octo/tool@v1"]),
        );
        fixture.shas.insert(written, SHA_A.to_owned());
        assert_eq!(
            offenders(fixture.evaluate(&with_allow_list(&["octo/*"]))),
            vec![(
                "octo/shared/.github/workflows/inner.yml".to_owned(),
                "evil/x@v1".to_owned(),
                ALLOWED_ACTIONS,
            )],
        );
    }

    #[test]
    fn a_local_call_two_levels_down_stays_in_the_remote_repository_at_its_pin() {
        let callee = format!("{REMOTE}@{SHA_A}");
        let mut fixture = Fixture::new(&caller_workflow(&callee));
        fixture.reusable.insert(
            callee.clone(),
            caller_workflow("./.github/workflows/inner.yml"),
        );
        fixture.reusable.insert(
            format!("octo/shared/.github/workflows/inner.yml@{SHA_A}"),
            caller_workflow("./.github/workflows/deeper.yml"),
        );
        fixture.reusable.insert(
            format!("octo/shared/.github/workflows/deeper.yml@{SHA_A}"),
            callee_workflow(&["evil/x@v1"]),
        );
        fixture.shas.insert(callee, SHA_A.to_owned());
        assert_eq!(
            offenders(fixture.evaluate(&sha_policy())),
            vec![(
                "octo/shared/.github/workflows/deeper.yml".to_owned(),
                "evil/x@v1".to_owned(),
                REQUIRE_SHA_PINNING,
            )],
        );
    }

    #[test]
    fn without_a_recorded_commit_the_written_ref_names_the_callees_commit() {
        // The expander falls back to the ref a remote callee was written with.
        let written = format!("{REMOTE}@v1");
        let mut fixture = Fixture::new(&caller_workflow(&written));
        fixture
            .reusable
            .insert(written, caller_workflow("./.github/workflows/inner.yml"));
        fixture.reusable.insert(
            "octo/shared/.github/workflows/inner.yml@v1".to_owned(),
            callee_workflow(&["evil/x@v1"]),
        );
        assert_eq!(
            offenders(fixture.evaluate(&with_allow_list(&["octo/*"]))),
            vec![(
                "octo/shared/.github/workflows/inner.yml".to_owned(),
                "evil/x@v1".to_owned(),
                ALLOWED_ACTIONS,
            )],
        );
    }

    #[test]
    fn a_remote_callees_local_call_inherits_the_callees_lock() {
        let callee = format!("{REMOTE}@{SHA_A}");
        let mut fixture = Fixture::new(&caller_workflow(&callee));
        fixture.lock = Some(lock_file(&[(CI, &[])], &[]));
        fixture.reusable.insert(
            callee.clone(),
            caller_workflow("./.github/workflows/inner.yml"),
        );
        fixture.reusable.insert(
            format!("octo/shared/.github/workflows/inner.yml@{SHA_A}"),
            callee_workflow(&["actions/checkout@v4"]),
        );
        fixture.shas.insert(callee.clone(), SHA_A.to_owned());
        // The callee's lock onboards the callee, but not the workflow it calls.
        fixture.remote.insert(
            callee.clone(),
            lock_file(&[(".github/workflows/build.yml", &[])], &[]),
        );
        assert_eq!(
            offenders(fixture.evaluate(&lockfile_policy())),
            vec![(
                REMOTE.to_owned(),
                "./.github/workflows/inner.yml".to_owned(),
                REQUIRE_LOCKFILE,
            )],
        );
        // Onboarding the called workflow in the callee's own lock satisfies it.
        fixture.remote.insert(
            callee,
            lock_file(
                &[
                    (".github/workflows/build.yml", &[]),
                    (".github/workflows/inner.yml", &[]),
                ],
                &[],
            ),
        );
        assert!(offenders(fixture.evaluate(&lockfile_policy())).is_empty());
    }

    #[test]
    fn a_remote_callees_local_call_outside_the_reusable_paths_fails_closed() {
        // `./scripts/x.yml` names no reusable workflow path, so the expander cannot
        // locate it from the callee; the run's own file at that path is no stand-in.
        let callee = format!("{REMOTE}@{SHA_A}");
        let mut fixture = Fixture::new(&caller_workflow(&callee));
        fixture
            .reusable
            .insert(callee.clone(), caller_workflow("./scripts/x.yml"));
        fixture.reusable.insert(
            "scripts/x.yml".to_owned(),
            callee_workflow(&["./.github/actions/local"]),
        );
        fixture.shas.insert(callee, SHA_A.to_owned());
        assert_eq!(
            offenders(fixture.evaluate(&sha_policy())),
            vec![(
                REMOTE.to_owned(),
                "./scripts/x.yml".to_owned(),
                REQUIRE_SHA_PINNING
            )],
        );
    }

    /// A run whose top-level workflow calls `c1`, which calls `c2`, and so on up
    /// to `c{links}`, which uses a pinned action. Each file is a reusable
    /// workflow at its own depth.
    fn chained_run(links: usize) -> Fixture {
        let mut fixture = Fixture::new(&caller_workflow("./.github/workflows/c1.yml"));
        let pinned = format!("actions/checkout@{SHA_A}");
        for index in 1..=links {
            let content = if index < links {
                format!(
                    "on: workflow_call\njobs:\n  next:\n    uses: \
                     \"./.github/workflows/c{}.yml\"\n",
                    index + 1
                )
            } else {
                callee_workflow(&[pinned.as_str()])
            };
            fixture
                .reusable
                .insert(format!(".github/workflows/c{index}.yml"), content);
        }
        fixture
    }

    #[test]
    fn nesting_beyond_the_reusable_depth_bound_fails_closed() {
        let policy = sha_policy();
        // The bound is the parser's own reusable depth, so this check follows
        // exactly the tree the expander materializes: one callee inside it is
        // judged, one past it is refused.
        let depth = crate::actions::DECLARED_ACTION_REUSABLE_DEPTH;
        assert!(offenders(chained_run(depth).evaluate(&policy)).is_empty());
        assert_eq!(
            offenders(chained_run(depth + 1).evaluate(&policy)),
            vec![(
                format!(".github/workflows/c{depth}.yml"),
                format!("./.github/workflows/c{}.yml", depth + 1),
                REQUIRE_SHA_PINNING
            )]
        );
    }

    #[test]
    fn callees_that_cannot_be_read_fail_closed() {
        let policy = sha_policy();
        let mut fixture = Fixture::new(&caller_workflow("./.github/workflows/missing.yml"));
        let error = fixture
            .evaluate(&policy)
            .expect_err("a missing callee cannot be checked");
        assert!(
            error.violations[0]
                .message
                .contains("no workflow content was submitted"),
            "{error}"
        );

        fixture.reusable.insert(
            ".github/workflows/missing.yml".to_owned(),
            "jobs: [".to_owned(),
        );
        let error = fixture
            .evaluate(&policy)
            .expect_err("an unparseable callee cannot be checked");
        assert!(
            error.violations[0].message.contains("does not parse"),
            "{error}"
        );
    }

    #[test]
    fn locked_dependencies_are_matched_by_their_commit_sha() {
        let mut fixture = Fixture::new(&steps_workflow(&["actions/checkout@v4"]));
        fixture.lock = Some(lock_file(
            &[(CI, &["actions/checkout@v4"])],
            &[("actions/checkout@v4", SHA_A)],
        ));
        let by_tag = with_allow_list(&["actions/checkout@v4"]);
        assert_eq!(
            offenders(fixture.evaluate(&by_tag)),
            vec![(
                CI.to_owned(),
                "actions/checkout@v4".to_owned(),
                ALLOWED_ACTIONS
            )],
            "a tag entry does not match the locked commit"
        );

        let sha_entry = format!("actions/checkout@{SHA_A}");
        assert!(offenders(fixture.evaluate(&with_allow_list(&[sha_entry.as_str()]))).is_empty());
        assert!(offenders(fixture.evaluate(&with_allow_list(&["actions/*"]))).is_empty());

        // Not onboarded: the reference as written is matched, so the tag passes.
        fixture.lock = None;
        assert!(offenders(fixture.evaluate(&by_tag)).is_empty());
    }

    #[test]
    fn a_rejection_names_every_offender_up_to_the_cap() {
        let error = PolicyViolation {
            violations: (0..25)
                .map(|index| Violation {
                    workflow_path: CI.to_owned(),
                    uses: format!("actions/checkout@v{index}"),
                    rule: REQUIRE_SHA_PINNING,
                    message: "not pinned".to_owned(),
                })
                .collect(),
        };
        let text = error.to_string();
        assert!(text.starts_with("actions policy rejected this run (25 violations)"));
        assert!(text.contains("actions/checkout@v0"));
        assert!(text.contains("actions/checkout@v19"));
        assert!(!text.contains("actions/checkout@v20"));
        assert!(text.contains("... and 5 more"));
        assert!(text.contains("(config: [actions_policy] require_sha_pinning)"));
    }

    #[test]
    fn the_actions_policy_table_loads_with_defaults_for_omitted_keys() {
        let absent = toml::from_str::<crate::config::ConfigFile>("").unwrap();
        assert_eq!(absent.actions_policy, ActionsPolicy::default());

        let partial = toml::from_str::<crate::config::ConfigFile>(
            "[actions_policy]\nrequire_sha_pinning = true\n",
        )
        .unwrap();
        assert!(partial.actions_policy.require_sha_pinning);
        assert!(
            partial.actions_policy.verify_commit_reachability,
            "an omitted key keeps its default"
        );
        assert!(!partial.actions_policy.require_lockfile);
    }

    #[test]
    fn unknown_actions_policy_keys_are_rejected() {
        assert!(
            toml::from_str::<crate::config::ConfigFile>(
                "[actions_policy]\nrequire_sha_pining = true\n"
            )
            .is_err()
        );
    }

    #[test]
    fn a_malformed_allowed_actions_entry_fails_config_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "[actions_policy]\nallowed_actions = [\"actions/*\", \"owner/repo, other/repo\"]\n",
        )
        .unwrap();
        let error = crate::config::load_config_from(&path).unwrap_err();
        assert!(
            format!("{error:?}").contains("`,` does not separate entries"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn valid_allowed_actions_entries_load_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "[actions_policy]\nallowed_actions = [\"actions/*\", \"!actions/cache@*\"]\n",
        )
        .unwrap();
        let config = crate::config::load_config_from(&path).unwrap();
        assert_eq!(
            config.actions_policy.allowed_actions,
            ["actions/*", "!actions/cache@*"]
        );
    }

    #[test]
    fn more_allowed_actions_entries_than_the_limit_fail_config_load() {
        let entries = vec!["\"actions/checkout@v4\""; MAX_ALLOWED_ACTIONS + 1].join(", ");
        let text = format!("[actions_policy]\nallowed_actions = [{entries}]\n");
        let error = toml::from_str::<crate::config::ConfigFile>(&text).unwrap_err();
        assert!(error.to_string().contains("at most 1000"), "{error}");
    }
}
