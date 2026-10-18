//! The workflow pass of `preloop init`: the secrets the repositories' workflows
//! read, and the `runs-on` labels they request, each checked against the rule
//! the engine applies to it. Read-only: nothing here stores a secret or edits a
//! workflow. `init` decides what to offer.

use preloop_gha_parser::secrets::ENGINE_PROVIDED_SECRETS;
use preloop_gha_parser::{RunsOn, parse_workflow};
use preloop_orchestrator::environment::{EnvironmentSpec, OFFICIAL_GOLDEN};
use preloop_runner_server::config::ConfigFile;
use preloop_runner_server::github_app;
use preloop_runner_server::runtime_scheduling::{job_matches_runner, unhostable_platform};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The largest workflow or action file the scan reads.
const MAX_FILE_BYTES: u64 = 1024 * 1024;
/// How deep `.github/actions` is searched for `action.yml` files.
const ACTION_DEPTH_LIMIT: usize = 8;
/// Accept header that returns a file's bytes instead of its JSON envelope.
const RAW_ACCEPT: &str = "application/vnd.github.raw";

// -- targets ------------------------------------------------------------------

/// A repository the scan reads: its checkout when the working directory is
/// that repository, otherwise GitHub, through the configured credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Target {
    /// `owner/name`, when GitHub knows the repository.
    pub repo: Option<String>,
    /// The working tree to read instead of the API.
    pub checkout: Option<PathBuf>,
}

/// The repositories in scope: each `--repo` (read from the working tree when
/// that is the checkout in use), else the checkout the working directory is in.
pub(super) fn targets(
    requested: &[String],
    checkout: Option<&Path>,
    origin: Option<&str>,
) -> Vec<Target> {
    if requested.is_empty() {
        return checkout
            .map(|root| Target {
                repo: origin.map(str::to_owned),
                checkout: Some(root.to_path_buf()),
            })
            .into_iter()
            .collect();
    }
    let mut seen = BTreeSet::new();
    requested
        .iter()
        .filter(|repo| seen.insert(repo.to_ascii_lowercase()))
        .map(|repo| Target {
            repo: Some(repo.clone()),
            checkout: match (checkout, origin) {
                (Some(root), Some(origin)) if origin.eq_ignore_ascii_case(repo) => {
                    Some(root.to_path_buf())
                }
                _ => None,
            },
        })
        .collect()
}

/// The root of the git checkout the working directory is in, if there is one.
pub(super) fn checkout_root() -> Option<PathBuf> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    let root = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (output.status.success() && !root.is_empty()).then(|| PathBuf::from(root))
}

/// A workflow or composite-action file, `path` relative to the repository root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WorkflowFile {
    pub path: String,
    pub text: String,
}

/// What reading one target produced, and what was left out.
#[derive(Debug)]
pub(super) struct Scan {
    pub target: Target,
    /// `checkout`, `github`, or `none` (nothing could be read).
    pub source: &'static str,
    pub files: Vec<WorkflowFile>,
    /// Why nothing was read, when nothing was.
    pub skipped: Option<String>,
    /// Files or listings that could not be read in full.
    pub notes: Vec<String>,
}

/// Read one target: its checkout when it has one, else GitHub with the
/// configured credential. A repository that cannot be read is skipped with a
/// reason; it never fails the run.
pub(super) async fn read_target(target: Target, config: &ConfigFile) -> Scan {
    if let Some(root) = target.checkout.clone() {
        let mut notes = Vec::new();
        let files = scan_checkout(&root, &mut notes);
        return Scan {
            target,
            source: "checkout",
            files,
            skipped: None,
            notes,
        };
    }
    let Some(repo) = target.repo.clone() else {
        return Scan {
            target,
            source: "none",
            files: Vec::new(),
            skipped: Some("it has no repository name".to_owned()),
            notes: Vec::new(),
        };
    };
    match fetch_repo(config, &repo).await {
        Ok((files, notes)) => Scan {
            target,
            source: "github",
            files,
            skipped: None,
            notes,
        },
        Err(reason) => Scan {
            target,
            source: "none",
            files: Vec::new(),
            skipped: Some(reason),
            notes: Vec::new(),
        },
    }
}

/// One line for the report: what was read, or why the repository was skipped.
pub(super) fn describe(scan: &Scan) -> String {
    let name = scan.target.repo.as_deref().unwrap_or("this checkout");
    if let Some(reason) = &scan.skipped {
        return format!("{name}: skipped, {reason}");
    }
    let origin = if scan.source == "checkout" {
        "checkout"
    } else {
        "GitHub"
    };
    let (workflows, actions) = (workflow_count(scan), action_count(scan));
    format!(
        "{name} ({origin}): {workflows} workflow file{}, {actions} composite action{}",
        plural(workflows),
        plural(actions)
    )
}

/// The name used in prose for one scan target.
pub(super) fn target_name(scan: &Scan) -> &str {
    scan.target.repo.as_deref().unwrap_or("this checkout")
}

fn workflow_count(scan: &Scan) -> usize {
    scan.files
        .iter()
        .filter(|file| is_workflow(&file.path))
        .count()
}

fn action_count(scan: &Scan) -> usize {
    scan.files.len() - workflow_count(scan)
}

fn plural(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn is_workflow(path: &str) -> bool {
    path.starts_with(".github/workflows/")
}

// -- reading a checkout -------------------------------------------------------

/// The workflow files and composite actions in a checkout's working tree.
fn scan_checkout(root: &Path, notes: &mut Vec<String>) -> Vec<WorkflowFile> {
    let mut paths = Vec::new();
    // GitHub reads only the top level of `.github/workflows`.
    if let Ok(entries) = std::fs::read_dir(root.join(".github/workflows")) {
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().is_ok_and(|kind| kind.is_file()) && is_yaml(&path) {
                paths.push(path);
            }
        }
    }
    collect_actions(&root.join(".github/actions"), 0, &mut paths);
    paths.sort();
    paths
        .into_iter()
        .filter_map(|path| {
            let display = relative_path(root, &path);
            match read_small(&path) {
                Ok(text) => Some(WorkflowFile {
                    path: display,
                    text,
                }),
                Err(reason) => {
                    notes.push(format!("{display}: {reason}"));
                    None
                }
            }
        })
        .collect()
}

fn is_yaml(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("yml" | "yaml")
    )
}

fn is_action_file_name(name: &str) -> bool {
    matches!(name, "action.yml" | "action.yaml")
}

fn collect_actions(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
    if depth > ACTION_DEPTH_LIMIT {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if kind.is_dir() {
            collect_actions(&path, depth + 1, out);
        } else if kind.is_file() && is_action_file_name(&entry.file_name().to_string_lossy()) {
            out.push(path);
        }
    }
}

fn read_small(path: &Path) -> Result<String, String> {
    let size = std::fs::metadata(path)
        .map_err(|error| error.to_string())?
        .len();
    if size > MAX_FILE_BYTES {
        return Err(format!("{size} bytes is over the 1 MiB scan limit"));
    }
    std::fs::read_to_string(path).map_err(|error| format!("not readable as UTF-8 text ({error})"))
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

// -- reading a GitHub repository ----------------------------------------------

/// The workflow files and composite actions of `repo` on its default branch,
/// read through the GitHub API with the configured credential. The error is
/// the reason the repository was skipped.
async fn fetch_repo(
    config: &ConfigFile,
    repo: &str,
) -> Result<(Vec<WorkflowFile>, Vec<String>), String> {
    let token = github_token(config, repo).await?;
    let client = preloop_runner_server::shared_http::github_client_builder()
        .user_agent("preloop-init")
        .build()
        .map_err(|error| format!("could not build the GitHub client ({error})"))?;
    let base = format!("{}/repos/{repo}", github_api_base());
    let info = get_json(&client, &token, &base).await?;
    let branch = info["default_branch"]
        .as_str()
        .ok_or_else(|| format!("GitHub named no default branch for {repo}"))?;
    let tree = get_json(
        &client,
        &token,
        &format!("{base}/git/trees/{}?recursive=1", encode_segment(branch)),
    )
    .await?;
    let mut notes = Vec::new();
    if tree["truncated"].as_bool() == Some(true) {
        notes
            .push("GitHub truncated the file tree; some workflow files were not listed".to_owned());
    }
    let mut files = Vec::new();
    for path in workflow_paths(&tree) {
        let url = format!("{base}/contents/{}", encode_path(&path));
        match get_raw(&client, &token, &url, branch).await {
            Ok(text) => files.push(WorkflowFile { path, text }),
            Err(reason) => notes.push(format!("{path}: {reason}")),
        }
    }
    Ok((files, notes))
}

/// A token that can read `repo`'s files: the first configured App that can
/// mint one for it, else the PAT. The token itself is never reported.
async fn github_token(config: &ConfigFile, repo: &str) -> Result<String, String> {
    let read_only = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
    let mut app_failure = None;
    match github_app::load_from(config) {
        Ok(Some(apps)) => {
            for app in &apps.apps {
                match github_app::get_or_mint_token(app, repo, &read_only).await {
                    Ok(token) => return Ok(token),
                    Err(error) => app_failure = Some(format!("{error:#}")),
                }
            }
        }
        Ok(None) => {}
        Err(error) => app_failure = Some(format!("{error:#}")),
    }
    if let Some(pat) = std::env::var("PRELOOP_GITHUB_PAT")
        .ok()
        .filter(|pat| !pat.trim().is_empty())
    {
        return Ok(pat);
    }
    if let Some(pat) = config.github.pat() {
        return Ok(pat.to_owned());
    }
    Err(match app_failure {
        Some(error) => format!("no GitHub App token for {repo} ({error})"),
        None => "no GitHub credential is configured".to_owned(),
    })
}

/// The GitHub API base, as the engine reads `PRELOOP_GITHUB_API_URL`.
fn github_api_base() -> String {
    std::env::var("PRELOOP_GITHUB_API_URL")
        .ok()
        .map(|base| base.trim().trim_end_matches('/').to_owned())
        .filter(|base| !base.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_owned())
}

async fn get_json(client: &reqwest::Client, token: &str, url: &str) -> Result<Value, String> {
    let response = client
        .get(url)
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .send()
        .await
        .map_err(|error| format!("could not reach GitHub ({error})"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("GitHub answered {status}"));
    }
    response
        .json()
        .await
        .map_err(|error| format!("unreadable GitHub response ({error})"))
}

async fn get_raw(
    client: &reqwest::Client,
    token: &str,
    url: &str,
    branch: &str,
) -> Result<String, String> {
    let response = client
        .get(url)
        .query(&[("ref", branch)])
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, RAW_ACCEPT)
        .send()
        .await
        .map_err(|error| format!("could not reach GitHub ({error})"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("GitHub answered {status}"));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_FILE_BYTES)
    {
        return Err("over the 1 MiB scan limit".to_owned());
    }
    response
        .text()
        .await
        .map_err(|error| format!("unreadable GitHub response ({error})"))
}

/// The workflow files and composite actions named by a git tree listing.
fn workflow_paths(tree: &Value) -> Vec<String> {
    let mut paths: Vec<String> = tree["tree"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| entry["type"].as_str() == Some("blob"))
        .filter_map(|entry| entry["path"].as_str())
        .filter(|path| is_workflow_path(path) || is_action_path(path))
        .map(str::to_owned)
        .collect();
    paths.sort();
    paths
}

fn is_workflow_path(path: &str) -> bool {
    path.strip_prefix(".github/workflows/").is_some_and(|name| {
        !name.contains('/') && (name.ends_with(".yml") || name.ends_with(".yaml"))
    })
}

fn is_action_path(path: &str) -> bool {
    path.strip_prefix(".github/actions/")
        .is_some_and(|rest| is_action_file_name(rest.rsplit('/').next().unwrap_or(rest)))
}

/// Percent-encode one URL path segment (a branch name may contain `/`).
fn encode_segment(segment: &str) -> String {
    segment
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn encode_path(path: &str) -> String {
    path.split('/')
        .map(encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

// -- secrets ------------------------------------------------------------------

/// One `secrets.NAME` or `secrets['NAME']` read, and where it sits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SecretRead {
    pub name: String,
    pub path: String,
    /// 1-based.
    pub line: usize,
}

/// Every secret the text reads, except the tokens the engine mints itself
/// (`GITHUB_TOKEN` and friends are never stored, so never missing).
pub(super) fn secret_reads(path: &str, text: &str) -> Vec<SecretRead> {
    let mut reads = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim_start().starts_with('#') {
            continue;
        }
        for name in names_read_on(line) {
            if ENGINE_PROVIDED_SECRETS
                .iter()
                .any(|token| token.eq_ignore_ascii_case(name))
            {
                continue;
            }
            reads.push(SecretRead {
                name: name.to_owned(),
                path: path.to_owned(),
                line: index + 1,
            });
        }
    }
    reads
}

/// The names read on one line, in order: `secrets.NAME` and `secrets['NAME']`.
/// `secrets` must start a word, so `mysecrets.X` and `my-secrets.X` do not match.
fn names_read_on(line: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut from = 0;
    while let Some(offset) = line[from..].find("secrets") {
        let start = from + offset;
        let end = start + "secrets".len();
        from = end;
        let continues_a_name = line[..start]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
        if continues_a_name {
            continue;
        }
        let rest = &line[end..];
        let name = match rest.strip_prefix('.') {
            Some(dotted) => identifier(dotted),
            None => bracketed(rest),
        };
        names.extend(name);
    }
    names
}

/// A secret name at the start of `text`: letters, digits, underscores, not
/// starting with a digit.
fn identifier(text: &str) -> Option<&str> {
    let first = text.chars().next()?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return None;
    }
    let end = text
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(text.len());
    Some(&text[..end])
}

/// The `['NAME']` / `["NAME"]` form, starting at the text after `secrets`.
fn bracketed(rest: &str) -> Option<&str> {
    let inner = rest.trim_start().strip_prefix('[')?.trim_start();
    let quote = inner.chars().next().filter(|c| matches!(c, '\'' | '"'))?;
    let inner = &inner[1..];
    let name = identifier(inner)?;
    let after = inner[name.len()..].strip_prefix(quote)?.trim_start();
    after.starts_with(']').then_some(name)
}

/// The secret names the engine holds, by scope: names only, never values.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct Stored {
    global: BTreeSet<String>,
    /// repository -> names
    repo: BTreeMap<String, BTreeSet<String>>,
    /// repository -> environment -> names
    env: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
}

impl Stored {
    /// The names in the config file.
    pub(super) fn from_config(config: &ConfigFile) -> Self {
        let mut stored = Self::default();
        stored.global.extend(config.secrets.keys().cloned());
        for (repo, names) in &config.repo_secrets {
            stored
                .repo
                .entry(repo.clone())
                .or_default()
                .extend(names.keys().cloned());
        }
        for (repo, envs) in &config.env_secrets {
            for (env, names) in envs {
                stored
                    .env
                    .entry(repo.clone())
                    .or_default()
                    .entry(env.clone())
                    .or_default()
                    .extend(names.keys().cloned());
            }
        }
        stored
    }

    /// The running engine's listing (`GET /api/v1/secrets`), as
    /// `(name, repository, environment)` entries.
    pub(super) fn from_listing(entries: &[(String, Option<String>, Option<String>)]) -> Self {
        let mut stored = Self::default();
        for (name, repo, env) in entries {
            match (repo, env) {
                (Some(repo), Some(env)) => {
                    stored
                        .env
                        .entry(repo.clone())
                        .or_default()
                        .entry(env.clone())
                        .or_default()
                        .insert(name.clone());
                }
                (Some(repo), None) => {
                    stored
                        .repo
                        .entry(repo.clone())
                        .or_default()
                        .insert(name.clone());
                }
                _ => {
                    stored.global.insert(name.clone());
                }
            }
        }
        stored
    }

    /// Where `name` is stored for a job in `repo`: its environment (the most
    /// specific tier), then repository, then global. Exact spelling, as the
    /// engine matches it.
    pub(super) fn scope_of(&self, name: &str, repo: Option<&str>) -> Option<String> {
        if let Some(repo) = repo {
            let environment = self
                .env
                .get(repo)
                .and_then(|envs| envs.iter().find(|(_, names)| names.contains(name)));
            if let Some((env, _)) = environment {
                return Some(format!("environment {env} of {repo}"));
            }
            if self
                .repo
                .get(repo)
                .is_some_and(|names| names.contains(name))
            {
                return Some(format!("repository {repo}"));
            }
        }
        self.global.contains(name).then(|| "global".to_owned())
    }

    /// A stored name that differs from `name` only in letter case.
    pub(super) fn spelled_differently(&self, name: &str) -> Option<String> {
        self.names()
            .find(|stored| stored.as_str() != name && stored.eq_ignore_ascii_case(name))
            .cloned()
    }

    fn names(&self) -> impl Iterator<Item = &String> + '_ {
        self.global
            .iter()
            .chain(self.repo.values().flatten())
            .chain(self.env.values().flat_map(|envs| envs.values().flatten()))
    }
}

/// One secret a target reads, and whether the engine holds it for that target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SecretRow {
    pub name: String,
    /// `path:line` of every read, sorted.
    pub reads: Vec<String>,
    /// Where it is stored for this target; `None` when missing.
    pub scope: Option<String>,
    /// The spelling the engine does hold, when only a case variant is stored.
    pub stored_as: Option<String>,
}

impl SecretRow {
    /// The configured column: the scope, or why it is missing.
    pub(super) fn status(&self) -> String {
        if let Some(scope) = &self.scope {
            return scope.clone();
        }
        if let Some(stored) = &self.stored_as {
            return format!("MISSING (stored as {stored}; names match exactly)");
        }
        if !is_upper_snake(&self.name) {
            return "MISSING (stored names are UPPER_SNAKE; rename the reference)".to_owned();
        }
        "MISSING".to_owned()
    }
}

/// The secrets one target reads, with where the engine stores each.
pub(super) fn secret_rows(scan: &Scan, stored: &Stored) -> Vec<SecretRow> {
    let repo = scan.target.repo.as_deref();
    let mut reads: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for file in &scan.files {
        for read in secret_reads(&file.path, &file.text) {
            reads
                .entry(read.name)
                .or_default()
                .insert(format!("{}:{}", read.path, read.line));
        }
    }
    reads
        .into_iter()
        .map(|(name, places)| {
            let scope = stored.scope_of(&name, repo);
            let stored_as = match scope {
                Some(_) => None,
                None => stored.spelled_differently(&name),
            };
            SecretRow {
                name,
                reads: places.into_iter().collect(),
                scope,
                stored_as,
            }
        })
        .collect()
}

fn is_upper_snake(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// The command that stores `name` for `repo` (global when `None`). `None` when
/// `preloop secret set` would refuse the name.
pub(super) fn set_command(name: &str, repo: Option<&str>) -> Option<String> {
    if !is_upper_snake(name) {
        return None;
    }
    Some(match repo {
        Some(repo) => format!("preloop secret set {name} --repo {repo}"),
        None => format!("preloop secret set {name}"),
    })
}

/// `ok` when every secret read is stored, `missing` when one is not, `none`
/// when nothing is read.
pub(super) fn secrets_decision(rows: &[SecretRow]) -> &'static str {
    if rows.is_empty() {
        "none"
    } else if rows.iter().any(|row| row.scope.is_none()) {
        "missing"
    } else {
        "ok"
    }
}

// -- runner labels ------------------------------------------------------------

/// What the pool serves: the labels every runner declares, the labels
/// `PRELOOP_RUNNER_LABELS` adds, and the golden jobs fork from.
#[derive(Debug, Clone)]
pub(super) struct Pool {
    /// Every label the pool's runners declare, as the engine advertises them.
    labels: Vec<String>,
    /// The default labels: `self-hosted`, `Linux`, and the host architecture.
    base: Vec<String>,
    /// The host architecture as the engine names it (`x86_64`, `aarch64`).
    host_arch: String,
    /// The golden a job without a GitHub-hosted label forks from.
    golden: String,
}

impl Pool {
    /// `labels` is the engine's advertised set, `golden` the configured base
    /// image (or [`OFFICIAL_GOLDEN`] for the packed official one).
    pub(super) fn new(labels: Vec<String>, host_arch: &str, golden: &str) -> Self {
        Self {
            labels,
            base: vec![
                "self-hosted".to_owned(),
                "Linux".to_owned(),
                host_arch.to_owned(),
            ],
            host_arch: host_arch.to_owned(),
            golden: golden.to_owned(),
        }
    }

    /// The labels added on top of the base set, i.e. `PRELOOP_RUNNER_LABELS`.
    fn declared(&self) -> Vec<String> {
        self.labels
            .iter()
            .filter(|label| !self.is_base(label))
            .cloned()
            .collect()
    }

    fn is_base(&self, label: &str) -> bool {
        self.base
            .iter()
            .any(|base| base.eq_ignore_ascii_case(label))
    }
}
/// The non-secret pool facts included in `--json`.
pub(super) fn pool_json(pool: &Pool) -> Value {
    json!({
        "labels": pool.labels,
        "base_labels": pool.base,
        "host_arch": pool.host_arch,
        "golden": pool.golden,
    })
}

/// How the engine treats one `runs-on` label on this pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Verdict {
    /// Declared by every pool runner.
    Default,
    /// Declared through `PRELOOP_RUNNER_LABELS`.
    Declared,
    /// A GitHub-hosted Linux image the pool serves. `official` says the job
    /// forks from the official golden whatever the operator configured.
    HostedLinux { official: bool },
    /// Only a runner of this OS can serve it, and none is registered here.
    ExternalOs(&'static str),
    /// Declared nowhere on this pool.
    Undeclared,
    /// An expression the engine resolves only when the job is promoted.
    Dynamic,
    /// A runner group other than `Default`.
    RunnerGroup,
}

impl Verdict {
    /// The machine name `--json` reports.
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Declared => "declared",
            Self::HostedLinux { .. } => "hosted_linux",
            Self::ExternalOs(_) => "external_os",
            Self::Undeclared => "undeclared",
            Self::Dynamic => "dynamic",
            Self::RunnerGroup => "runner_group",
        }
    }

    /// `ok`, `warn`, or `action`.
    pub(super) fn severity(self) -> &'static str {
        match self {
            Self::Default | Self::Declared | Self::HostedLinux { .. } => "ok",
            Self::ExternalOs(_) | Self::Dynamic | Self::RunnerGroup => "warn",
            Self::Undeclared => "action",
        }
    }
}

/// Classify one label against the engine's own matching and scheduling
/// functions, in the order the scheduler applies them.
pub(super) fn classify(label: &str, pool: &Pool) -> Verdict {
    if label.contains("${{") {
        return Verdict::Dynamic;
    }
    // A macOS or Windows label fails at submit unless that OS has a runner
    // registered; the Linux pool never counts as one.
    if let Some(os) = unhostable_platform(&[label.to_owned()], ["linux"]) {
        return Verdict::ExternalOs(os);
    }
    if !job_matches_runner(&[label.to_owned()], &pool.labels) {
        return Verdict::Undeclared;
    }
    if pool.is_base(label) {
        return Verdict::Default;
    }
    if label.to_ascii_lowercase().starts_with("ubuntu") {
        let official =
            EnvironmentSpec::base_for_labels(&[label.to_owned()], &pool.golden) == OFFICIAL_GOLDEN;
        return Verdict::HostedLinux { official };
    }
    Verdict::Declared
}

/// The human explanation for a verdict.
pub(super) fn verdict_message(verdict: Verdict, label: &str, pool: &Pool) -> String {
    match verdict {
        Verdict::Default => "served by every pool runner (base label)".to_owned(),
        Verdict::Declared => "served: declared in PRELOOP_RUNNER_LABELS".to_owned(),
        Verdict::HostedLinux { official: true } if pool.golden == OFFICIAL_GOLDEN => {
            "served by the Linux pool on the official golden".to_owned()
        }
        Verdict::HostedLinux { official: true } => {
            "served by the Linux pool on the official golden, not your configured golden \
             (ubuntu-latest and ubuntu-24.04 always use the official one)"
                .to_owned()
        }
        Verdict::HostedLinux { official: false } => format!(
            "served by the Linux pool on your configured golden, not the Ubuntu version {label} names"
        ),
        Verdict::ExternalOs(os) => {
            format!("fails at submit unless a {os} runner is registered with this server")
        }
        Verdict::Undeclared => {
            let mut message = "not declared: fails at submit unless it is declared on this host \
                               or a registered runner has the label"
                .to_owned();
            if let Some(note) = arch_note(label, &pool.host_arch) {
                message.push_str("; ");
                message.push_str(&note);
            }
            message
        }
        Verdict::Dynamic => "resolved when the job is promoted: check by hand".to_owned(),
        Verdict::RunnerGroup => {
            "preloop's runners are in the Default group, so this group never matches them"
                .to_owned()
        }
    }
}

/// GitHub names architectures `X64`/`ARM64`; the engine declares
/// `x86_64`/`aarch64` and nothing else, so say which one this host is.
fn arch_note(label: &str, host_arch: &str) -> Option<String> {
    let engine_arch = match label.to_ascii_uppercase().as_str() {
        "X64" => "x86_64",
        "ARM64" => "aarch64",
        _ => return None,
    };
    Some(if engine_arch == host_arch {
        format!("{label} is GitHub's name for this host's {host_arch}")
    } else {
        format!("{label} is GitHub's name for {engine_arch}, not this host's {host_arch}")
    })
}

/// One distinct `runs-on` label, with where it is used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LabelRow {
    pub label: String,
    /// `path (job id)` for each use.
    pub uses: Vec<String>,
    pub verdict: Verdict,
}

/// Every `runs-on` label across the scanned workflows.
#[derive(Debug, Default)]
pub(super) struct LabelReport {
    pub rows: Vec<LabelRow>,
    /// Workflow files that did not parse, with the parser's reason.
    pub unparsed: Vec<(String, String)>,
}

impl LabelReport {
    /// `ok`, `warning`, `action_required`, or `none`.
    pub(super) fn decision(&self) -> &'static str {
        if self.rows.is_empty() && self.unparsed.is_empty() {
            "none"
        } else if self
            .rows
            .iter()
            .any(|row| row.verdict == Verdict::Undeclared)
        {
            "action_required"
        } else if !self.unparsed.is_empty()
            || self.rows.iter().any(|row| row.verdict.severity() == "warn")
        {
            "warning"
        } else {
            "ok"
        }
    }
}

/// Collect and classify the `runs-on` labels of every workflow in `scans`.
pub(super) fn label_report(scans: &[Scan], pool: &Pool) -> LabelReport {
    let mut report = LabelReport::default();
    let mut rows: BTreeMap<String, LabelRow> = BTreeMap::new();
    for scan in scans {
        for file in scan.files.iter().filter(|file| is_workflow(&file.path)) {
            let requests = match runs_on_requests(&file.text) {
                Ok(requests) => requests,
                Err(error) => {
                    report.unparsed.push((file.path.clone(), error));
                    continue;
                }
            };
            for request in requests {
                let place = format!("{} (job {})", file.path, request.job);
                for label in &request.labels {
                    add_use(&mut rows, label, &place, classify(label, pool));
                }
                if let Some(group) = request.group.as_deref() {
                    let label = format!("group: {group}");
                    let verdict = if group.eq_ignore_ascii_case("default") {
                        Verdict::Default
                    } else {
                        Verdict::RunnerGroup
                    };
                    add_use(&mut rows, &label, &place, verdict);
                }
                for expression in &request.dynamic {
                    add_use(&mut rows, expression, &place, Verdict::Dynamic);
                }
            }
        }
    }
    report.rows = rows.into_values().collect();
    report
}

fn add_use(rows: &mut BTreeMap<String, LabelRow>, label: &str, place: &str, verdict: Verdict) {
    let row = rows
        .entry(label.to_ascii_lowercase())
        .or_insert_with(|| LabelRow {
            label: label.to_owned(),
            uses: Vec::new(),
            verdict,
        });
    if !row.uses.iter().any(|existing| existing == place) {
        row.uses.push(place.to_owned());
    }
}

/// The `runs-on` of one job, split into what can be checked statically.
#[derive(Debug, Default, PartialEq, Eq)]
struct RunsOnRequest {
    job: String,
    labels: Vec<String>,
    /// The `group:` of an object-valued `runs-on`, when literal.
    group: Option<String>,
    /// Values with an expression, which only resolve at promotion.
    dynamic: Vec<String>,
}

fn runs_on_requests(text: &str) -> Result<Vec<RunsOnRequest>, String> {
    let workflow = parse_workflow(text).map_err(|error| error.to_string())?;
    Ok(workflow
        .jobs
        .iter()
        .map(|(job, body)| request_of(job, &body.runs_on))
        .collect())
}

fn request_of(job: &str, runs_on: &RunsOn) -> RunsOnRequest {
    let mut request = RunsOnRequest {
        job: job.to_owned(),
        ..RunsOnRequest::default()
    };
    match runs_on {
        RunsOn::Single(label) => sort_label(label, &mut request),
        RunsOn::Many(labels) => {
            for label in labels {
                sort_label(label, &mut request);
            }
        }
        RunsOn::Dynamic(Value::String(text)) => sort_label(text, &mut request),
        RunsOn::Dynamic(Value::Object(map)) => {
            if let Some(group) = map.get("group").and_then(Value::as_str) {
                if group.contains("${{") {
                    request.dynamic.push(group.to_owned());
                } else {
                    request.group = Some(group.to_owned());
                }
            }
            match map.get("labels") {
                Some(Value::String(label)) => sort_label(label, &mut request),
                Some(Value::Array(items)) => {
                    for label in items.iter().filter_map(Value::as_str) {
                        sort_label(label, &mut request);
                    }
                }
                _ => {}
            }
        }
        RunsOn::Dynamic(Value::Null) => {}
        RunsOn::Dynamic(other) => request.dynamic.push(other.to_string()),
    }
    request
}

fn sort_label(label: &str, request: &mut RunsOnRequest) {
    let label = label.trim();
    if label.is_empty() {
        return;
    }
    if label.contains("${{") {
        request.dynamic.push(label.to_owned());
    } else {
        request.labels.push(label.to_owned());
    }
}

/// The `PRELOOP_RUNNER_LABELS` line that makes the undeclared labels served on
/// this host, keeping what is already declared. `None` when nothing is missing.
pub(super) fn declare_line(pool: &Pool, report: &LabelReport) -> Option<String> {
    let mut declared = pool.declared();
    let mut added = false;
    for row in report
        .rows
        .iter()
        .filter(|row| row.verdict == Verdict::Undeclared)
    {
        let plain = !row.label.contains([',', ' ', '\t']);
        let known = declared
            .iter()
            .any(|label| label.eq_ignore_ascii_case(&row.label));
        if plain && !known {
            declared.push(row.label.clone());
            added = true;
        }
    }
    added.then(|| format!("PRELOOP_RUNNER_LABELS={}", declared.join(",")))
}

// -- report rendering ---------------------------------------------------------

/// Lines of a plain-text table: the headers, then the rows, two spaces between
/// columns, the last column unpadded.
pub(super) fn table(headers: &[&str], rows: &[Vec<String>]) -> Vec<String> {
    let mut widths: Vec<usize> = headers
        .iter()
        .map(|header| header.chars().count())
        .collect();
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let render = |cells: Vec<&str>| -> String {
        let last = cells.len().saturating_sub(1);
        cells
            .iter()
            .enumerate()
            .map(|(index, cell)| {
                if index == last {
                    (*cell).to_owned()
                } else {
                    format!("{cell:<width$}", width = widths[index])
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_owned()
    };
    let mut lines = vec![render(headers.to_vec())];
    for row in rows {
        lines.push(render(row.iter().map(String::as_str).collect()));
    }
    lines
}

/// A short list of places: the first three, then how many more.
pub(super) fn places(places: &[String]) -> String {
    let mut shown = places
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if places.len() > 3 {
        shown.push_str(&format!(" (+{} more)", places.len() - 3));
    }
    shown
}

/// The `targets` array both steps report: where each repository's files came from.
pub(super) fn targets_json(scans: &[Scan]) -> Value {
    Value::Array(
        scans
            .iter()
            .map(|scan| {
                json!({
                    "repo": scan.target.repo,
                    "source": scan.source,
                    "workflow_files": workflow_count(scan),
                    "action_files": action_count(scan),
                    "skipped": scan.skipped,
                    "notes": scan.notes,
                })
            })
            .collect(),
    )
}

/// The `secrets` array of the secrets step.
pub(super) fn secret_rows_json(scan: &Scan, rows: &[SecretRow]) -> Vec<Value> {
    let repo = scan.target.repo.as_deref();
    rows.iter()
        .map(|row| {
            let command = if row.scope.is_none() {
                set_command(&row.name, repo)
            } else {
                None
            };
            json!({
                "name": row.name,
                "repo": repo,
                "reads": row.reads,
                "configured": row.scope,
                "stored_as": row.stored_as,
                "command": command,
            })
        })
        .collect()
}

/// The `labels` array of the runner-labels step.
pub(super) fn label_rows_json(report: &LabelReport, pool: &Pool) -> Vec<Value> {
    report
        .rows
        .iter()
        .map(|row| {
            json!({
                "label": row.label,
                "uses": row.uses,
                "verdict": row.verdict.name(),
                "severity": row.verdict.severity(),
                "message": verdict_message(row.verdict, &row.label, pool),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_with(repo: Option<&str>, files: &[(&str, &str)]) -> Scan {
        Scan {
            target: Target {
                repo: repo.map(str::to_owned),
                checkout: None,
            },
            source: "checkout",
            files: files
                .iter()
                .map(|(path, text)| WorkflowFile {
                    path: (*path).to_owned(),
                    text: (*text).to_owned(),
                })
                .collect(),
            skipped: None,
            notes: Vec::new(),
        }
    }

    fn names(text: &str) -> Vec<(String, usize)> {
        secret_reads("ci.yml", text)
            .into_iter()
            .map(|read| (read.name, read.line))
            .collect()
    }

    #[test]
    fn secret_reads_cover_dotted_and_bracketed_forms_with_lines() {
        let text = "env:\n  A: ${{ secrets.OP_SERVICE_ACCOUNT_TOKEN }}\n  \
                    B: ${{ secrets['NPM_TOKEN'] }}\n  C: ${{ secrets[ \"QUOTED\" ] }}\n";
        assert_eq!(
            names(text),
            vec![
                ("OP_SERVICE_ACCOUNT_TOKEN".to_owned(), 2),
                ("NPM_TOKEN".to_owned(), 3),
                ("QUOTED".to_owned(), 4),
            ]
        );
    }

    #[test]
    fn secret_reads_skip_engine_tokens_comments_and_lookalikes() {
        let text = "a: ${{ secrets.GITHUB_TOKEN }}\n\
                    b: ${{ secrets.ACTIONS_RUNTIME_TOKEN }}\n\
                    c: ${{ mysecrets.NOT_A_SECRET }}\n\
                    d: ${{ my-secrets.NOT_EITHER }}\n\
                    # e: ${{ secrets.COMMENTED_OUT }}\n\
                    secrets:\n  inherit: true\n";
        assert!(names(text).is_empty(), "{:?}", names(text));
    }

    #[test]
    fn composite_action_references_are_found() {
        let reads = secret_reads(
            ".github/actions/deploy/action.yml",
            "runs:\n  using: composite\n  steps:\n    - run: echo ${{ secrets.DEPLOY_KEY }}\n",
        );
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0].name, "DEPLOY_KEY");
        assert_eq!(reads[0].path, ".github/actions/deploy/action.yml");
        assert_eq!(reads[0].line, 4);
    }

    #[test]
    fn repeated_reads_collapse_to_one_row_with_every_location() {
        let scan = scan_with(
            Some("preloopdev/example"),
            &[
                (
                    ".github/workflows/ci.yml",
                    "a: ${{ secrets.NPM_TOKEN }}\nb: ${{ secrets.NPM_TOKEN }}\n",
                ),
                (
                    ".github/workflows/release.yml",
                    "x: ${{ secrets.NPM_TOKEN }}\n",
                ),
            ],
        );
        let rows = secret_rows(&scan, &Stored::default());
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].reads,
            vec![
                ".github/workflows/ci.yml:1",
                ".github/workflows/ci.yml:2",
                ".github/workflows/release.yml:1",
            ]
        );
    }

    #[test]
    fn stored_secrets_report_the_most_specific_scope_and_match_exactly() {
        let stored = Stored::from_listing(&[
            (
                "OP_SERVICE_ACCOUNT_TOKEN".to_owned(),
                Some("o/r".to_owned()),
                None,
            ),
            (
                "DEPLOY_KEY".to_owned(),
                Some("o/r".to_owned()),
                Some("prod".to_owned()),
            ),
            ("NPM_TOKEN".to_owned(), None, None),
            ("op_lower".to_owned(), None, None),
        ]);
        assert_eq!(
            stored
                .scope_of("OP_SERVICE_ACCOUNT_TOKEN", Some("o/r"))
                .as_deref(),
            Some("repository o/r")
        );
        assert_eq!(
            stored.scope_of("DEPLOY_KEY", Some("o/r")).as_deref(),
            Some("environment prod of o/r")
        );
        assert_eq!(stored.scope_of("DEPLOY_KEY", Some("other/x")), None);
        assert_eq!(
            stored.scope_of("NPM_TOKEN", Some("other/x")).as_deref(),
            Some("global")
        );
        assert_eq!(stored.scope_of("OP_LOWER", Some("o/r")), None);
        assert_eq!(
            stored.spelled_differently("OP_LOWER").as_deref(),
            Some("op_lower")
        );
    }

    #[test]
    fn a_case_variant_is_missing_with_the_stored_spelling() {
        let scan = scan_with(
            Some("o/r"),
            &[(".github/workflows/ci.yml", "t: ${{ secrets.OP_LOWER }}\n")],
        );
        let stored = Stored::from_listing(&[("op_lower".to_owned(), None, None)]);
        let rows = secret_rows(&scan, &stored);
        assert_eq!(rows[0].scope, None);
        assert_eq!(rows[0].stored_as.as_deref(), Some("op_lower"));
        assert_eq!(secrets_decision(&rows), "missing");
    }

    #[test]
    fn missing_secret_gets_the_set_command_and_no_value() {
        let scan = scan_with(
            Some("preloopdev/python-starter-template"),
            &[(
                ".github/workflows/ci.yml",
                "t: ${{ secrets.OP_SERVICE_ACCOUNT_TOKEN }}\n",
            )],
        );
        let rows = secret_rows(&scan, &Stored::default());
        let json = secret_rows_json(&scan, &rows);
        assert_eq!(
            json[0]["command"],
            "preloop secret set OP_SERVICE_ACCOUNT_TOKEN --repo preloopdev/python-starter-template"
        );
        assert_eq!(json[0]["configured"], Value::Null);
        assert_eq!(set_command("lower_case", None), None);
        assert_eq!(
            set_command("NPM_TOKEN", None).as_deref(),
            Some("preloop secret set NPM_TOKEN")
        );
    }

    #[test]
    fn workflow_listing_selects_workflows_and_composite_actions_only() {
        let tree = json!({"tree": [
            {"path": ".github/workflows/ci.yml", "type": "blob"},
            {"path": ".github/workflows/nested/x.yml", "type": "blob"},
            {"path": ".github/actions/deploy/action.yaml", "type": "blob"},
            {"path": ".github/actions/action.yml", "type": "blob"},
            {"path": ".github/workflows", "type": "tree"},
            {"path": "README.md", "type": "blob"},
        ]});
        assert_eq!(
            workflow_paths(&tree),
            vec![
                ".github/actions/action.yml",
                ".github/actions/deploy/action.yaml",
                ".github/workflows/ci.yml",
            ]
        );
    }

    #[test]
    fn checkout_scan_reads_top_level_workflows_and_nested_actions() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join(".github/workflows/nested")).unwrap();
        std::fs::create_dir_all(root.join(".github/actions/x")).unwrap();
        std::fs::write(root.join(".github/workflows/ci.yml"), "on: push\n").unwrap();
        std::fs::write(root.join(".github/workflows/nested/no.yml"), "on: push\n").unwrap();
        std::fs::write(root.join(".github/workflows/notes.txt"), "x").unwrap();
        std::fs::write(root.join(".github/actions/x/action.yml"), "runs: {}\n").unwrap();
        let mut notes = Vec::new();
        let files = scan_checkout(root, &mut notes);
        let paths: Vec<&str> = files.iter().map(|file| file.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![".github/actions/x/action.yml", ".github/workflows/ci.yml"]
        );
        assert!(notes.is_empty());
    }

    #[test]
    fn targets_follow_the_checkout_and_deduplicate_repos() {
        let root = Path::new("/work/app");
        let targets = targets(
            &["o/app".to_owned(), "O/APP".to_owned(), "o/other".to_owned()],
            Some(root),
            Some("o/app"),
        );
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].checkout.as_deref(), Some(root));
        assert_eq!(targets[1].checkout, None);

        let here = targets_for_working_directory(Some(root), Some("o/app"));
        assert_eq!(here.len(), 1);
        assert_eq!(here[0].repo.as_deref(), Some("o/app"));
        assert!(targets_for_working_directory(None, None).is_empty());
    }

    fn targets_for_working_directory(root: Option<&Path>, origin: Option<&str>) -> Vec<Target> {
        targets(&[], root, origin)
    }

    fn pool(golden: &str) -> Pool {
        Pool::new(
            vec![
                "self-hosted".to_owned(),
                "Linux".to_owned(),
                "x86_64".to_owned(),
                "custom-lab".to_owned(),
            ],
            "x86_64",
            golden,
        )
    }

    #[test]
    fn each_label_kind_gets_the_engine_verdict() {
        let pool = pool(OFFICIAL_GOLDEN);
        assert_eq!(classify("self-hosted", &pool), Verdict::Default);
        assert_eq!(classify("linux", &pool), Verdict::Default);
        assert_eq!(classify("x86_64", &pool), Verdict::Default);
        assert_eq!(classify("custom-lab", &pool), Verdict::Declared);
        assert_eq!(
            classify("ubuntu-latest", &pool),
            Verdict::HostedLinux { official: true }
        );
        assert_eq!(
            classify("ubuntu-22.04", &pool),
            Verdict::HostedLinux { official: true },
            "the configured golden is the official one here"
        );
        assert_eq!(classify("macos-14", &pool), Verdict::ExternalOs("macos"));
        assert_eq!(
            classify("windows-latest", &pool),
            Verdict::ExternalOs("windows")
        );
        assert_eq!(classify("X64", &pool), Verdict::Undeclared);
        assert_eq!(classify("gpu-box", &pool), Verdict::Undeclared);
        assert_eq!(classify("${{ matrix.os }}", &pool), Verdict::Dynamic);
    }

    #[test]
    fn ubuntu_versions_other_than_latest_and_24_04_run_on_the_configured_golden() {
        let custom = pool("ghcr.io/example/golden:1");
        assert_eq!(
            classify("ubuntu-22.04", &custom),
            Verdict::HostedLinux { official: false }
        );
        assert_eq!(
            classify("ubuntu-24.04", &custom),
            Verdict::HostedLinux { official: true },
            "ubuntu-24.04 always selects the official golden"
        );
        assert_eq!(
            classify("ubuntu-latest", &custom),
            Verdict::HostedLinux { official: true }
        );
        let message = verdict_message(
            Verdict::HostedLinux { official: true },
            "ubuntu-latest",
            &custom,
        );
        assert!(message.contains("not your configured golden"), "{message}");
    }

    #[test]
    fn arch_labels_name_the_engine_architecture() {
        let pool = pool(OFFICIAL_GOLDEN);
        let x64 = verdict_message(Verdict::Undeclared, "X64", &pool);
        assert!(
            x64.contains("GitHub's name for this host's x86_64"),
            "{x64}"
        );
        let arm = verdict_message(Verdict::Undeclared, "ARM64", &pool);
        assert!(
            arm.contains("GitHub's name for aarch64, not this host's x86_64"),
            "{arm}"
        );
    }

    #[test]
    fn runs_on_forms_parse_into_labels_groups_and_dynamic_values() {
        let text = "on: push\n\
                    jobs:\n\
                    \x20 plain:\n\
                    \x20   runs-on: ubuntu-latest\n\
                    \x20 list:\n\
                    \x20   runs-on: [self-hosted, X64]\n\
                    \x20 grouped:\n\
                    \x20   runs-on:\n\
                    \x20     group: big\n\
                    \x20     labels: [gpu]\n\
                    \x20 matrix:\n\
                    \x20   strategy:\n\
                    \x20     matrix:\n\
                    \x20       os: [ubuntu-latest]\n\
                    \x20   runs-on: ${{ matrix.os }}\n";
        let requests = runs_on_requests(text).unwrap();
        let by_job = |job: &str| requests.iter().find(|r| r.job == job).unwrap();
        assert_eq!(by_job("plain").labels, vec!["ubuntu-latest"]);
        assert_eq!(by_job("list").labels, vec!["self-hosted", "X64"]);
        assert_eq!(by_job("grouped").group.as_deref(), Some("big"));
        assert_eq!(by_job("grouped").labels, vec!["gpu"]);
        assert_eq!(by_job("matrix").dynamic, vec!["${{ matrix.os }}"]);
    }

    #[test]
    fn report_decisions_and_the_declare_line() {
        let pool = pool(OFFICIAL_GOLDEN);
        let scan = scan_with(
            Some("o/r"),
            &[(
                ".github/workflows/agent.yml",
                "on: push\njobs:\n  test:\n    runs-on: [self-hosted, X64]\n  build:\n    runs-on: gpu-box\n",
            )],
        );
        let report = label_report(&[scan], &pool);
        assert_eq!(report.decision(), "action_required");
        assert_eq!(
            declare_line(&pool, &report).as_deref(),
            Some("PRELOOP_RUNNER_LABELS=custom-lab,gpu-box,X64")
        );
        let rows = label_rows_json(&report, &pool);
        assert!(
            rows.iter()
                .any(|row| row["label"] == "gpu-box" && row["severity"] == "action")
        );
    }

    #[test]
    fn runner_group_objects_are_reported() {
        let pool = pool(OFFICIAL_GOLDEN);
        let scan = scan_with(
            Some("o/r"),
            &[(
                ".github/workflows/groups.yml",
                "on: push\njobs:\n  default:\n    runs-on:\n      group: Default\n      labels: [self-hosted]\n  external:\n    runs-on:\n      group: mac-pool\n      labels: [Linux]\n",
            )],
        );
        let report = label_report(&[scan], &pool);
        let default_group = report
            .rows
            .iter()
            .find(|row| row.label == "group: Default")
            .expect("default group row");
        assert_eq!(default_group.verdict, Verdict::Default);
        let external_group = report
            .rows
            .iter()
            .find(|row| row.label == "group: mac-pool")
            .expect("external group row");
        assert_eq!(external_group.verdict, Verdict::RunnerGroup);
        assert_eq!(report.decision(), "warning");
    }

    #[test]
    fn a_workflow_that_does_not_parse_is_reported_not_guessed() {
        let pool = pool(OFFICIAL_GOLDEN);
        let scan = scan_with(Some("o/r"), &[(".github/workflows/bad.yml", "jobs: [\n")]);
        let report = label_report(&[scan], &pool);
        assert!(report.rows.is_empty());
        assert_eq!(report.unparsed.len(), 1);
        assert_eq!(report.decision(), "warning");
    }

    #[test]
    fn remote_path_segments_are_percent_encoded() {
        assert_eq!(encode_segment("feature/x"), "feature%2Fx");
        assert_eq!(
            encode_path(".github/workflows/a b.yml"),
            ".github/workflows/a%20b.yml"
        );
    }

    #[test]
    fn tables_align_columns_and_leave_the_last_unpadded() {
        let lines = table(
            &["SECRET", "STATE"],
            &[
                vec!["A".to_owned(), "MISSING".to_owned()],
                vec!["LONGER".to_owned(), "ok".to_owned()],
            ],
        );
        assert_eq!(lines[0], "SECRET  STATE");
        assert_eq!(lines[1], "A       MISSING");
        assert_eq!(lines[2], "LONGER  ok");
    }
}
