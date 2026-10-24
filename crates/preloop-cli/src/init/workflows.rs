//! The workflow pass of `preloop init`: the secrets the repositories' workflows
//! read, and the `runs-on` labels they request, each checked against the rule
//! the engine applies to it. Read-only: nothing here stores a secret or edits a
//! workflow. `init` decides what to offer.

use preloop_gha_parser::secrets::ENGINE_PROVIDED_SECRETS;
use preloop_gha_parser::{Job, RunsOn, Workflow, parse_workflow};
use preloop_gha_protocol::expr_scan::find_expression_end;
use preloop_orchestrator::environment::{EnvironmentSpec, OFFICIAL_GOLDEN};
use preloop_runner_server::config::ConfigFile;
use preloop_runner_server::github_app;
use preloop_runner_server::runtime_scheduling::{job_matches_runner, unhostable_platform};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::{Path, PathBuf};

/// The largest workflow or action file the scan reads.
const MAX_FILE_BYTES: u64 = 1024 * 1024;
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
    collect_actions(&root.join(".github/actions"), &mut paths);
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

/// Every `action.yml` / `action.yaml` under `dir`, however deeply nested: the
/// engine has no depth limit for composite actions, so a checkout scan and a
/// GitHub scan of the same repository must not disagree about which ones run.
/// Symlinked directories are not followed (`file_type` does not resolve them).
fn collect_actions(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if kind.is_dir() {
            collect_actions(&path, out);
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
///
/// A GitHub `owner/repo` slug is required before any credential is loaded:
/// the name is interpolated into the request URL below, so a value carrying
/// URL delimiters or a dot segment would steer the token somewhere the caller
/// never chose. Dot-prefixed names like `.github` are ordinary repositories
/// and pass.
async fn fetch_repo(
    config: &ConfigFile,
    repo: &str,
) -> Result<(Vec<WorkflowFile>, Vec<String>), String> {
    if !preloop_runner_server::secrets_api::valid_repo_slug(repo) {
        return Err(format!("`{repo}` is not a GitHub owner/repository name"));
    }
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

/// Every `secrets.NAME` the raw text spells inside a `${{ … }}` span, or on
/// an `if:` line, except the tokens the engine mints itself (`GITHUB_TOKEN`
/// and friends are never stored, so never missing) and the names spelled
/// inside an expression's single-quoted string literal, which the engine
/// passes through as text.
///
/// The raw text alone cannot tell a YAML comment from a `run:` block line:
/// the engine substitutes a `${{ … }}` span inside a `run:` block wherever it
/// sits, `#` or not, while the same span in a YAML comment is never
/// evaluated. So this scan is not the authority on its own — a file that
/// parses has its names read from the parse ([`parsed_reads`]), and this scan
/// supplies their `path:line` places. It stays the only name authority for a
/// file that does not parse — a workflow the label step already reports as
/// unparsed.
pub(super) fn secret_reads(path: &str, text: &str) -> Vec<SecretRead> {
    let bodies = expression_bodies(text);
    // A name spelled inside an expression's string literal is text the engine
    // passes through, so it is not a read even where the scan finds it.
    let quoted = quoted_spans(text, &bodies);
    let mut reads = Vec::new();
    let mut line_start = 0;
    let mut condition_block = None;
    for (index, line) in text.split_inclusive('\n').enumerate() {
        let condition = is_condition_line(line, &mut condition_block);
        for (offset, name) in names_read_on(line) {
            let start = line_start + offset;
            if quoted.iter().any(|span| span.contains(&start)) {
                continue;
            }
            if !condition && !bodies.iter().any(|body| body.contains(&start)) {
                continue;
            }
            if is_engine_provided(name) {
                continue;
            }
            reads.push(SecretRead {
                name: name.to_owned(),
                path: path.to_owned(),
                line: index + 1,
            });
        }
        line_start += line.len();
    }
    reads
}

/// The byte ranges of the `${{ … }}` expression bodies in `text`, found with
/// the scanner the parser and the runner share.
fn expression_bodies(text: &str) -> Vec<Range<usize>> {
    let mut bodies = Vec::new();
    let mut from = 0;
    while let Some(offset) = text[from..].find("${{") {
        let body_start = from + offset + 3;
        let Some(end) = find_expression_end(&text[body_start..]) else {
            break;
        };
        bodies.push(body_start..body_start + end);
        from = body_start + end + 2;
    }
    bodies
}

/// The byte ranges of the single-quoted string literals inside every expression
/// body of `text`: a `secrets.NAME` spelled there is text, not a read. `''` is
/// one escaped quote, as the shared expression scanner reads it.
fn quoted_spans(text: &str, bodies: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut spans = Vec::new();
    for body in bodies {
        let mut opened: Option<usize> = None;
        let mut characters = text[body.clone()].char_indices().peekable();
        while let Some((offset, character)) = characters.next() {
            if character != '\'' {
                continue;
            }
            match opened {
                None => opened = Some(body.start + offset),
                Some(start) => {
                    if characters.peek().map(|&(_, next)| next) == Some('\'') {
                        characters.next();
                    } else {
                        spans.push(start..body.start + offset + 1);
                        opened = None;
                    }
                }
            }
        }
        if let Some(start) = opened {
            spans.push(start..body.end);
        }
    }
    spans
}

/// Whether `line` holds or continues an `if:` value, which the engine reads
/// as an expression with or without the `${{ }}` markers. `block` carries the
/// indentation of a folded (`if: >-`) or literal (`if: |`) value across
/// lines — its continuations are part of the condition — and is cleared by
/// the first non-blank line indented no deeper than the key's own line. Only
/// a real parse can tell an `if:` from text that looks like one inside a
/// `run:` block, so this scan decides only where no parse answered.
fn is_condition_line(line: &str, block: &mut Option<usize>) -> bool {
    let indent = line.len() - line.trim_start().len();
    if let Some(key_indent) = *block {
        if !line.trim().is_empty() && indent <= key_indent {
            *block = None;
        } else {
            return true;
        }
    }
    let value = line.trim_start();
    let value = value.strip_prefix("- ").unwrap_or(value).trim_start();
    let Some(value) = value.strip_prefix("if:") else {
        return false;
    };
    // A folded or literal block scalar holds its value on the lines below.
    if value.trim_start().starts_with(['>', '|']) {
        *block = Some(indent);
        return true;
    }
    // `iffy:` and `if_me:` are other keys, and `if:secrets.X` is one scalar.
    value.is_empty() || value.starts_with([' ', '\t', '\'', '"'])
}

/// The names read on one line, in order, each with the byte offset of the
/// `secrets` that reads it: `secrets.NAME` and `secrets['NAME']`. The context
/// is matched without regard to case, as the engine's expression evaluator
/// resolves it, and must start a word, so `mysecrets.X` and `my-secrets.X` do
/// not match.
fn names_read_on(line: &str) -> Vec<(usize, &str)> {
    let mut names = Vec::new();
    let mut from = 0;
    while let Some(offset) = find_ignore_case(&line[from..], "secrets") {
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
        names.extend(name.map(|name| (start, name)));
    }
    names
}

/// The offset of `needle` (ASCII) in `haystack`, ignoring ASCII case. Every
/// byte of a match is ASCII, so the offset is a char boundary of `haystack`.
fn find_ignore_case(haystack: &str, needle: &str) -> Option<usize> {
    let needle = needle.as_bytes();
    haystack
        .as_bytes()
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle))
}

/// The tokens the engine mints per job: they are never stored, so never
/// missing.
fn is_engine_provided(name: &str) -> bool {
    ENGINE_PROVIDED_SECRETS
        .iter()
        .any(|token| token.eq_ignore_ascii_case(name))
}

/// The names one field's text reads: its `${{ … }}` expressions, or the whole
/// text when it is an `if:` value. A name spelled inside a single-quoted string
/// literal of an expression is not a read — the engine evaluates the literal as
/// text — so it is skipped.
fn names_read_in(text: &str, whole: bool) -> Vec<&str> {
    let mut names = Vec::new();
    if whole {
        collect_expression_names(text, 0..text.len(), &mut names);
        return names;
    }
    for body in expression_bodies(text) {
        collect_expression_names(text, body, &mut names);
    }
    names
}

/// Add the names read in one expression range of `text`: `secrets.NAME` and
/// `secrets['NAME']`, minus the ones spelled inside a string literal.
fn collect_expression_names<'a>(text: &'a str, body: Range<usize>, names: &mut Vec<&'a str>) {
    let body_text = &text[body];
    let quoted = quoted_spans(body_text, std::slice::from_ref(&(0..body_text.len())));
    for (offset, name) in names_read_on(body_text) {
        if quoted.iter().any(|span| span.contains(&offset)) {
            continue;
        }
        names.push(name);
    }
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

/// The environments of the jobs that read one name, as they read it: `Some` of
/// the environment a job declares, `None` for a read no job's fields carry (a
/// composite action, a workflow that does not parse) or a job with no literal
/// environment. The engine looks the environment tier up with each job's own
/// `environment:` (`secret_provider.rs`), so no tier can be credited for a
/// name no reading job can be matched to one.
type ReadingEnvironments = BTreeSet<Option<String>>;

/// A read no job's fields carry: a composite action (the engine supplies its
/// secrets through the job that calls it) or a workflow that does not parse.
/// Nothing can be credited to an environment tier for it.
fn unattributed() -> ReadingEnvironments {
    std::iter::once(None).collect()
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

    /// Where `name` is stored for a job of `repo` that declares no
    /// environment: the repository tier, then global. Both reach every job, so
    /// either one is a scope a read can always rely on.
    pub(super) fn scope_of(&self, name: &str, repo: Option<&str>) -> Option<String> {
        self.scope_of_reads(name, repo, &ReadingEnvironments::new())
    }

    /// Where `name` is stored for the jobs in `repo` that read it.
    ///
    /// The repository tier and global are supplied to every job, so either one
    /// makes the read ready. An environment tier is looked up with each job's
    /// own `environment:` (`secret_provider.rs`), so it counts only when
    /// `reading` — the environments of the jobs that read `name` — is
    /// non-empty, every entry is a declared environment, and every one of them
    /// holds the name. When only an environment tier holds it, this returns
    /// `None` and [`Stored::environment_scope_of`] names the tier it is stored
    /// in.
    ///
    /// Exact spelling, as the engine matches it.
    pub(super) fn scope_of_reads(
        &self,
        name: &str,
        repo: Option<&str>,
        reading: &ReadingEnvironments,
    ) -> Option<String> {
        if let Some(repo) = repo {
            if self
                .repo
                .get(repo)
                .is_some_and(|names| names.contains(name))
            {
                return Some(format!("repository {repo}"));
            }
            let holding = self.environments_holding(name, repo);
            let mut environments: Vec<&str> = Vec::new();
            let mut covered = !reading.is_empty();
            for environment in reading {
                match environment {
                    Some(environment) => environments.push(environment),
                    None => covered = false,
                }
            }
            if covered
                && environments
                    .iter()
                    .all(|environment| holding.contains(environment))
            {
                return Some(environment_scope(&environments, repo));
            }
        }
        self.global.contains(name).then(|| "global".to_owned())
    }

    /// The environment tier `name` is stored in for `repo`, when no tier every
    /// job receives holds it: `environment prod of o/r`.
    pub(super) fn environment_scope_of(&self, name: &str, repo: &str) -> Option<String> {
        let holding = self.environments_holding(name, repo);
        if holding.is_empty() {
            return None;
        }
        Some(environment_scope(&holding, repo))
    }

    /// Every environment of `repo` that holds `name`, in name order.
    fn environments_holding(&self, name: &str, repo: &str) -> Vec<&str> {
        self.env
            .get(repo)
            .map(|envs| {
                envs.iter()
                    .filter(|(_, names)| names.contains(name))
                    .map(|(environment, _)| environment.as_str())
                    .collect()
            })
            .unwrap_or_default()
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

/// `environment prod of o/r`, or `environments prod, staging of o/r` when more
/// than one holds the name. Callers pass a non-empty list.
fn environment_scope(environments: &[&str], repo: &str) -> String {
    if let [environment] = environments {
        return format!("environment {environment} of {repo}");
    }
    format!("environments {} of {repo}", environments.join(", "))
}

/// One secret a target reads, and whether the engine holds it for that target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SecretRow {
    pub name: String,
    /// `path:line` of every read, sorted.
    pub reads: Vec<String>,
    /// Where the engine stores it for the jobs of this target; `None` when
    /// missing, or stored only where not every reading job reaches it.
    pub scope: Option<String>,
    /// The environment tier that holds the name when `scope` is `None`: the
    /// engine supplies it only to jobs that declare that environment, so the
    /// read counts as missing.
    pub environment_scope: Option<String>,
    /// The spelling the engine does hold, when only a case variant is stored.
    pub stored_as: Option<String>,
}

impl SecretRow {
    /// The configured column: the scope, or why it is missing.
    pub(super) fn status(&self) -> String {
        if let Some(scope) = &self.scope {
            return scope.clone();
        }
        if let Some(environment) = &self.environment_scope {
            return format!("MISSING (stored only in {environment})");
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

/// What a file that parses reads: the secret names, and the environments of
/// the jobs that read each name — `None` for a read no job's fields carry, as
/// in a composite action. `None` when the file does not parse: the raw span
/// scan stays that file's only authority (the label step reports a workflow
/// it cannot parse).
///
/// A file that parses answers for itself: the engine evaluates the values the
/// parse carries, and a YAML comment is not a value. A `${{ … }}` span inside
/// a `run:` block does survive the parse, `#` or not, so it is still read.
fn parsed_reads(path: &str, text: &str) -> Option<BTreeMap<String, ReadingEnvironments>> {
    if is_workflow(path) {
        let workflow = parse_workflow(text).ok()?;
        let mut by_name: BTreeMap<String, ReadingEnvironments> = BTreeMap::new();
        // Workflow-level fields (`env:` and friends) reach every job: the
        // engine merges the workflow environment into each job's, so their
        // reads belong to every job.
        let shared = workflow_fields_secret_names(&workflow)?;
        for job in workflow.jobs.values() {
            let names = job_secret_names(job)?;
            let environment = job_environment(job);
            for name in names.into_iter().chain(shared.iter().cloned()) {
                by_name.entry(name).or_default().insert(environment.clone());
            }
        }
        return Some(by_name);
    }
    let file_name = path.rsplit('/').next().unwrap_or(path);
    if !is_action_file_name(file_name) {
        // No other kind of file is scanned, and none of these is read.
        return None;
    }
    // A composite action is parsed the way the runner loads it: the whole
    // document as YAML. Its `if` values are conditions like a step's, which
    // the engine evaluates with or without the markers.
    let value: Value = serde_yaml::from_str(text).ok()?;
    let mut names = BTreeSet::new();
    collect_secret_names(&value, false, &mut names);
    Some(
        names
            .into_iter()
            .map(|name| (name, unattributed()))
            .collect::<BTreeMap<String, ReadingEnvironments>>(),
    )
}

/// The 1-based line of the first line of `text` that spells `name` as a read
/// (`secrets.NAME` / `secrets['NAME']`), else the first line that contains
/// `name` at all. Places a read the parse found but the context scan did not
/// recognize as one — the continuation of a folded `if:`, a name split across
/// lines — so no live read goes unplaced.
fn first_line_spelling(text: &str, name: &str) -> Option<usize> {
    text.split_inclusive('\n')
        .position(|line| names_read_on(line).iter().any(|(_, read)| *read == name))
        .or_else(|| {
            text.split_inclusive('\n')
                .position(|line| line.contains(name))
        })
        .map(|index| index + 1)
}

/// The literal `environment:` one job declares, as the engine reads it off the
/// job to look the environment tier up: a scalar or a `name:` mapping. An
/// expression resolves only at run time, so it is not literal.
fn job_environment(job: &Job) -> Option<String> {
    let name = match job.environment.as_ref() {
        Some(Value::String(name)) => name,
        Some(Value::Object(map)) => map.get("name").and_then(Value::as_str)?,
        _ => return None,
    };
    let name = name.trim();
    (!name.is_empty() && !name.contains("${{")).then(|| name.to_owned())
}

/// Every secret one parsed job reads: every string the job carries, with `if`
/// values read as whole expressions, engine-minted tokens dropped. `None` when
/// the job cannot be walked at all, which no environment may be credited for.
fn job_secret_names(job: &Job) -> Option<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    collect_secret_names(&serde_json::to_value(job).ok()?, false, &mut names);
    Some(names)
}

/// The secrets a workflow's own fields (`env:`, `run-name:`, `concurrency:`)
/// read, which every job of the workflow inherits.
fn workflow_fields_secret_names(workflow: &Workflow) -> Option<BTreeSet<String>> {
    let Value::Object(mut fields) = serde_json::to_value(workflow).ok()? else {
        return None;
    };
    fields.remove("jobs");
    let mut names = BTreeSet::new();
    collect_secret_names(&Value::Object(fields), false, &mut names);
    Some(names)
}

/// Every secret the strings of `value` read, with `if` values taken as whole
/// expressions.
fn collect_secret_names(value: &Value, condition: bool, names: &mut BTreeSet<String>) {
    match value {
        Value::String(text) => names.extend(
            names_read_in(text, condition)
                .into_iter()
                .filter(|name| !is_engine_provided(name))
                .map(str::to_owned),
        ),
        Value::Array(items) => {
            for item in items {
                collect_secret_names(item, false, names);
            }
        }
        Value::Object(map) => {
            for (key, item) in map {
                collect_secret_names(item, key == "if", names);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// The secrets one target reads, with where the engine stores each.
pub(super) fn secret_rows(scan: &Scan, stored: &Stored) -> Vec<SecretRow> {
    let repo = scan.target.repo.as_deref();
    let mut reads: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut reading: BTreeMap<String, ReadingEnvironments> = BTreeMap::new();
    for file in &scan.files {
        // What the file reads: the names its parse carries, or — when it does
        // not parse — every name the raw text spells.
        let parsed = parsed_reads(&file.path, &file.text);
        let raw = secret_reads(&file.path, &file.text);
        let live: BTreeMap<&str, ReadingEnvironments> = match &parsed {
            Some(names) => names
                .iter()
                .map(|(name, environments)| (name.as_str(), environments.clone()))
                .collect(),
            None => raw
                .iter()
                .map(|read| (read.name.as_str(), unattributed()))
                .collect(),
        };
        // Every `path:line` the raw scan spells for a live name. A comment
        // outside a `run:` block is spelled by the scan but is not a name the
        // parse carries, so it places nothing.
        let mut places: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
        for read in &raw {
            if live.contains_key(read.name.as_str()) {
                places
                    .entry(read.name.as_str())
                    .or_default()
                    .insert(format!("{}:{}", read.path, read.line));
            }
        }
        for (name, environments) in live {
            let mut at = places.remove(name).unwrap_or_default();
            // The context scan can miss the spelling of a read the parse found
            // — it does not recognize the continuations of a folded `if:` — so
            // a live read it placed nowhere is still reported, at the first
            // line that spells it.
            if at.is_empty()
                && let Some(line) = first_line_spelling(&file.text, name)
            {
                at.insert(format!("{}:{line}", file.path));
            }
            reads.entry(name.to_owned()).or_default().extend(at);
            reading
                .entry(name.to_owned())
                .or_default()
                .extend(environments);
        }
    }
    reads
        .into_iter()
        .map(|(name, places)| {
            let reading = match reading.get(&name) {
                Some(job_environments) => job_environments.clone(),
                None => ReadingEnvironments::new(),
            };
            let scope = stored.scope_of_reads(&name, repo, &reading);
            let environment_scope = match (&scope, repo) {
                (None, Some(repo)) => stored.environment_scope_of(&name, repo),
                _ => None,
            };
            let stored_as = match (&scope, &environment_scope) {
                (None, None) => stored.spelled_differently(&name),
                _ => None,
            };
            SecretRow {
                name,
                reads: places.into_iter().collect(),
                scope,
                environment_scope,
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
/// `preloop secret set` would refuse the name — or when the name is held in an
/// environment tier (`environment` is `Some`): the only command this row could
/// suggest is a repository-wide write, and that would silently widen a value
/// the operator scoped to one environment to every job of the repository.
/// Widening stays a deliberate act (`--env`, or `--repo` alone), never a
/// suggestion.
pub(super) fn set_command(
    name: &str,
    repo: Option<&str>,
    environment: Option<&str>,
) -> Option<String> {
    if environment.is_some() || !is_upper_snake(name) {
        return None;
    }
    Some(match repo {
        Some(repo) => format!("preloop secret set {name} --repo {repo}"),
        None => format!("preloop secret set {name}"),
    })
}

/// What to do about a read the engine holds only in an environment tier, in
/// place of the `set` command the other missing rows get: the tier reaches only
/// the jobs that declare that `environment:`, and widening it repository-wide
/// is the operator's call, not the wizard's.
pub(super) fn environment_only_advice(row: &SecretRow, repo: Option<&str>) -> String {
    let environment = row
        .environment_scope
        .as_deref()
        .unwrap_or("an environment tier");
    let mut advice = format!(
        "{name} is stored only in {environment}, which reaches only the jobs that declare that \
         `environment:` — declare it on the jobs that read the secret",
        name = row.name
    );
    if let Some(repo) = repo {
        advice.push_str(&format!(
            ", or widen the scope on purpose (`preloop secret set {name} --repo {repo} --env \
             <environment>` for one environment, `preloop secret set {name} --repo {repo}` for \
             every job of the repository)",
            name = row.name
        ));
    }
    advice
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

/// The engine's name for the architecture a label names, when it names one:
/// GitHub spells them `X64`/`ARM64`, the engine `x86_64`/`aarch64`.
fn engine_arch(label: &str) -> Option<&'static str> {
    let label = label.trim();
    if label.eq_ignore_ascii_case("X64") || label.eq_ignore_ascii_case("x86_64") {
        return Some("x86_64");
    }
    if label.eq_ignore_ascii_case("ARM64") || label.eq_ignore_ascii_case("aarch64") {
        return Some("aarch64");
    }
    None
}

/// Why a label for another architecture can never be served here.
fn arch_note(label: &str, host_arch: &str) -> Option<String> {
    let engine_arch = engine_arch(label)?;
    Some(if engine_arch == host_arch {
        format!("{label} is this host's architecture ({host_arch})")
    } else {
        format!("{label} names {engine_arch}, not this host's {host_arch}")
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
                    // The scheduler parses the group as an integer and treats
                    // the Default group's id (1) as the name `Default`
                    // (`job_matches_runner_group`), so `1`, `01`, and `+1` all
                    // name it.
                    let group = group.trim();
                    let verdict =
                        if group.eq_ignore_ascii_case("default") || group.parse::<i64>() == Ok(1) {
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
            // `group:` is read as a string only, exactly as the engine's own
            // `RunsOn::group()` reads it: an unquoted numeric group is dropped
            // there, so the engine schedules such a job with no group
            // constraint and this scan must not invent one.
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
        // A label for another architecture can never be served on this host,
        // so declaring it would only advertise a runner that does not exist.
        if engine_arch(&row.label).is_some_and(|arch| arch != pool.host_arch.as_str()) {
            continue;
        }
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
                // An environment-only row gets no command: the report says
                // what to do instead (see `environment_only_advice`).
                set_command(&row.name, repo, row.environment_scope.as_deref())
            } else {
                None
            };
            // Once the row is configured the environment is no longer the
            // reason it would be missing, so it is not reported.
            let environment_scope = match row.scope {
                Some(_) => None,
                None => row.environment_scope.as_deref(),
            };
            json!({
                "name": row.name,
                "repo": repo,
                "reads": row.reads,
                "configured": row.scope,
                "environment_scope": environment_scope,
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
    fn secret_reads_skip_engine_tokens_and_lookalikes() {
        let text = "a: ${{ secrets.GITHUB_TOKEN }}\n\
                    b: ${{ secrets.ACTIONS_RUNTIME_TOKEN }}\n\
                    c: ${{ mysecrets.NOT_A_SECRET }}\n\
                    d: ${{ my-secrets.NOT_EITHER }}\n\
                    secrets:\n  inherit: true\n";
        assert!(names(text).is_empty(), "{:?}", names(text));
    }

    #[test]
    fn secret_reads_count_expressions_only() {
        let text = "# uses secrets.FROM_COMMENT\n\
                    run: echo \"kept in secrets.FROM_PROSE\"\n\
                    env:\n\
                    \x20 A: ${{ secrets.FROM_EXPRESSION }}\n\
                    \x20 B: ${{ secrets['BRACKETED'] }}\n\
                    \x20 C: ${{ SECRETS.UPPER_CONTEXT }}\n\
                    rule: ${{ env.X }} > secrets.AFTER_EXPRESSION\n\
                    if: secrets.FROM_CONDITION != ''\n\
                    \x20 step:\n\
                    \x20   - if: ${{ secrets.FROM_STEP_CONDITION }}\n\
                    \x20     run: echo ${{ secrets.IN_STEP }}\n\
                    \x20 run: |\n\
                    \x20   # echo ${{ secrets.IN_RUN_BLOCK }}\n";
        assert_eq!(
            names(text),
            vec![
                ("FROM_EXPRESSION".to_owned(), 4),
                ("BRACKETED".to_owned(), 5),
                ("UPPER_CONTEXT".to_owned(), 6),
                ("FROM_CONDITION".to_owned(), 8),
                ("FROM_STEP_CONDITION".to_owned(), 10),
                ("IN_STEP".to_owned(), 11),
                ("IN_RUN_BLOCK".to_owned(), 13),
            ]
        );
    }

    #[test]
    fn secret_reads_ignore_expression_string_literals() {
        // A `secrets.NAME` spelled inside an expression's string literal is
        // text the engine passes through, not a read.
        let text = "a: ${{ 'secrets.FROM_LITERAL' }}\n\
                    b: ${{ secrets.REAL }}\n\
                    c: ${{ format('secrets.{0}', 'ALSO_LITERAL') }}\n\
                    d: ${{ secrets['BRACKETED'] }}\n";
        assert_eq!(
            names(text),
            vec![("REAL".to_owned(), 2), ("BRACKETED".to_owned(), 4)]
        );
    }

    /// A workflow reads what the parser evaluates, not what it spells: a YAML
    /// comment is not a value, so its example is inert; a `#` line inside a
    /// `run:` block is content the engine substitutes, so it is a read; and an
    /// expression's string literal is spelling, not a lookup.
    #[test]
    fn a_workflow_reads_what_the_parser_evaluates_not_what_it_spells() {
        let text = r#"# example: ${{ secrets.FROM_COMMENT }}
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ${{ 'secrets.FROM_LITERAL' }}
      - run: |
          # echo ${{ secrets.IN_RUN_BLOCK }}
"#;
        let scan = scan_with(Some("o/r"), &[(".github/workflows/ci.yml", text)]);
        let rows = secret_rows(&scan, &Stored::default());
        assert_eq!(
            rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
            vec!["IN_RUN_BLOCK"]
        );
        assert_eq!(rows[0].reads, vec![".github/workflows/ci.yml:9"]);
    }

    /// A composite action is read from its raw text (the parser walk covers
    /// workflows), and its step conditions are read the same whether the value
    /// sits on the `if:` line or on the lines below it: a name folded onto its
    /// own line is a read, and the `run:` after the block is not mistaken for
    /// part of the condition.
    #[test]
    fn a_folded_condition_inside_an_action_is_read_like_a_single_line_one() {
        let reads = secret_reads(
            ".github/actions/deploy/action.yml",
            "runs:\n  using: composite\n  steps:\n    - if: >-\n        secrets.DEPLOY_KEY != ''\n      run: echo ${{ secrets.IN_RUN }}\n",
        );
        assert_eq!(
            reads
                .iter()
                .map(|read| (read.name.as_str(), read.line))
                .collect::<Vec<_>>(),
            vec![("DEPLOY_KEY", 5), ("IN_RUN", 6)]
        );
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

    /// A YAML comment is not a value the engine evaluates, so a workflow that
    /// parses must not read one — while a `#` line inside a `run:` block is a
    /// script line whose `${{ }}` span the engine substitutes.
    #[test]
    fn a_parsed_workflow_reads_its_values_not_its_comments() {
        let text = r#"# env: ${{ secrets.FROM_COMMENT }}
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: |
          # echo ${{ secrets.IN_RUN_BLOCK }}
"#;
        let scan = scan_with(Some("o/r"), &[(".github/workflows/ci.yml", text)]);
        let rows = secret_rows(&scan, &Stored::default());
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].name, "IN_RUN_BLOCK");
        assert_eq!(rows[0].reads, vec![".github/workflows/ci.yml:8"]);
    }

    /// A folded `if:` value is one string the engine evaluates whole, so a
    /// bare name on any of its lines is a read, placed on the line that
    /// spells it.
    #[test]
    fn a_folded_condition_reads_its_whole_value() {
        let text = r#"on: push
jobs:
  deploy:
    runs-on: ubuntu-latest
    if: >-
      secrets.RELEASE_KEY != '' &&
      github.ref == 'refs/heads/main'
    steps:
      - run: ./deploy.sh
"#;
        let scan = scan_with(Some("o/r"), &[(".github/workflows/deploy.yml", text)]);
        let rows = secret_rows(&scan, &Stored::default());
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].name, "RELEASE_KEY");
        assert_eq!(rows[0].reads, vec![".github/workflows/deploy.yml:6"]);
    }

    /// A condition the line scan cannot recognize — an `if:` in a flow
    /// mapping, off the start of its line — is still read from the parse, and
    /// placed on the line that spells it.
    #[test]
    fn a_condition_the_line_scan_misses_is_still_placed() {
        let text = r#"name: deploy
runs:
  using: composite
  steps:
    - {if: "secrets.FLOW_KEY != ''", run: deploy --key $KEY}
"#;
        let scan = scan_with(Some("o/r"), &[(".github/actions/deploy/action.yml", text)]);
        let rows = secret_rows(&scan, &Stored::default());
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].name, "FLOW_KEY");
        assert_eq!(rows[0].reads, vec![".github/actions/deploy/action.yml:5"]);
    }

    /// A name the raw text splits across lines is read (the parse sees one
    /// string) and placed on the line that spells it.
    #[test]
    fn a_read_split_across_lines_is_still_placed() {
        let text = r#"on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo ${{ secrets[
          'SPLIT_KEY'] }}
"#;
        let scan = scan_with(Some("o/r"), &[(".github/workflows/ci.yml", text)]);
        let rows = secret_rows(&scan, &Stored::default());
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].name, "SPLIT_KEY");
        assert_eq!(rows[0].reads, vec![".github/workflows/ci.yml:8"]);
    }

    /// The raw scan supplies the `path:line` of an unparsed file's reads, so
    /// it has to follow a folded `if:` too — and stop when the value does.
    #[test]
    fn raw_condition_lines_follow_a_folded_value() {
        let text = "if: >-\n\
                    \x20 secrets.FOLDED != ''\n\
                    # secrets.IN_COMMENT\n\
                    b: 2\n";
        assert_eq!(names(text), vec![("FOLDED".to_owned(), 2)]);
    }

    /// A composite action is a parsed YAML document like a workflow: its
    /// comment and prose strings are not values, its `run` and `if` strings
    /// are.
    #[test]
    fn a_composite_action_reads_only_the_reads_its_parse_carries() {
        let text = r#"# ${{ secrets.IN_COMMENT }}
name: deploy
description: deploy with secrets.PROSE_KEY set
runs:
  using: composite
  steps:
    - run: deploy --key ${{ secrets.DEPLOY_KEY }}
      if: secrets.GATED_KEY != ''
    - run: |
        # ${{ secrets.IN_BLOCK }}
"#;
        let scan = scan_with(Some("o/r"), &[(".github/actions/deploy/action.yml", text)]);
        let rows = secret_rows(&scan, &Stored::default());
        let reads: Vec<(String, Vec<String>)> =
            rows.into_iter().map(|row| (row.name, row.reads)).collect();
        let at = |line: usize| format!(".github/actions/deploy/action.yml:{line}");
        assert_eq!(
            reads,
            vec![
                ("DEPLOY_KEY".to_owned(), vec![at(7)]),
                ("GATED_KEY".to_owned(), vec![at(8)]),
                ("IN_BLOCK".to_owned(), vec![at(10)]),
            ],
            "comment and prose strings are not values"
        );
    }

    /// With no parse to go by, the raw text decides, as before.
    #[test]
    fn a_workflow_that_does_not_parse_keeps_every_raw_read() {
        // An empty `jobs:`: the parser rejects the workflow, and the label
        // step reports it as unparsed.
        let text = "# env: ${{ secrets.FROM_COMMENT }}\njobs: {}\n";
        let scan = scan_with(Some("o/r"), &[(".github/workflows/ci.yml", text)]);
        let rows = secret_rows(&scan, &Stored::default());
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].name, "FROM_COMMENT");
        assert_eq!(rows[0].reads, vec![".github/workflows/ci.yml:1"]);
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
    fn stored_secrets_report_the_most_specific_ready_scope_and_match_exactly() {
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
        let reading = |environment: Option<&str>| -> ReadingEnvironments {
            BTreeSet::from([environment.map(str::to_owned)])
        };
        assert_eq!(
            stored
                .scope_of("OP_SERVICE_ACCOUNT_TOKEN", Some("o/r"))
                .as_deref(),
            Some("repository o/r"),
            "the repository tier reaches every job"
        );
        assert_eq!(
            stored
                .scope_of_reads("DEPLOY_KEY", Some("o/r"), &reading(Some("prod")))
                .as_deref(),
            Some("environment prod of o/r"),
            "the reading job declares the environment the tier is stored for"
        );
        assert_eq!(
            stored.scope_of_reads("DEPLOY_KEY", Some("o/r"), &reading(Some("staging"))),
            None,
            "another environment's jobs are not supplied"
        );
        assert_eq!(
            stored.scope_of_reads("DEPLOY_KEY", Some("o/r"), &reading(None)),
            None,
            "a job with no environment is not supplied"
        );
        assert_eq!(
            stored.environment_scope_of("DEPLOY_KEY", "o/r").as_deref(),
            Some("environment prod of o/r"),
            "the tier it is stored in is named instead"
        );
        assert_eq!(
            stored.scope_of_reads("DEPLOY_KEY", Some("other/x"), &reading(Some("prod"))),
            None
        );
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
    fn an_environment_only_secret_is_ready_only_for_a_job_that_declares_it() {
        let stored = Stored::from_listing(&[(
            "DEPLOY_KEY".to_owned(),
            Some("o/r".to_owned()),
            Some("prod".to_owned()),
        )]);
        let rows_for = |job: &str| {
            let text = format!(
                "on: push\njobs:\n  deploy:\n{job}    runs-on: ubuntu-latest\n    \
                 steps:\n      - run: deploy --key ${{{{ secrets.DEPLOY_KEY }}}}\n"
            );
            let scan = scan_with(
                Some("o/r"),
                &[(".github/workflows/deploy.yml", text.as_str())],
            );
            secret_rows(&scan, &stored)
        };
        // The job declares the environment the tier is stored for: the engine
        // supplies the value.
        let rows = rows_for("    environment: prod\n");
        assert_eq!(rows[0].scope.as_deref(), Some("environment prod of o/r"));
        assert_eq!(secrets_decision(&rows), "ok");
        // Any other job — none, another environment, one that only resolves at
        // run time — gets no value, so the read is missing and the tier it is
        // stored in is named.
        for job in [
            "",
            "    environment: staging\n",
            "    environment: ${{ inputs.stage }}\n",
        ] {
            let rows = rows_for(job);
            assert_eq!(rows[0].scope, None, "{job:?}");
            assert_eq!(
                rows[0].environment_scope.as_deref(),
                Some("environment prod of o/r"),
                "{job:?}"
            );
            assert_eq!(
                rows[0].status(),
                "MISSING (stored only in environment prod of o/r)"
            );
            assert_eq!(secrets_decision(&rows), "missing");
        }
        // One reading job without the environment is enough to fail the whole
        // read, wherever the other reads and the workflow's own fields sit.
        let text = "on: push\nenv:\n  KEY: ${{ secrets.DEPLOY_KEY }}\njobs:\n  \
                    deploy:\n    environment: prod\n    runs-on: ubuntu-latest\n    \
                    steps:\n      - run: echo $KEY\n  \
                    test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo $KEY\n";
        let scan = scan_with(Some("o/r"), &[(".github/workflows/deploy.yml", text)]);
        let rows = secret_rows(&scan, &stored);
        assert_eq!(rows[0].scope, None);
        assert_eq!(secrets_decision(&rows), "missing");
        // A read no job carries — a composite action, a workflow that does not
        // parse — is credited to no environment either.
        let action = scan_with(
            Some("o/r"),
            &[(
                ".github/actions/deploy/action.yml",
                "runs:\n  using: composite\n  steps:\n    - run: echo ${{ secrets.DEPLOY_KEY }}\n",
            )],
        );
        let rows = secret_rows(&action, &stored);
        assert_eq!(rows[0].scope, None);
        assert_eq!(
            rows[0].environment_scope.as_deref(),
            Some("environment prod of o/r")
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
        assert_eq!(set_command("lower_case", None, None), None);
        assert_eq!(
            set_command("NPM_TOKEN", None, None).as_deref(),
            Some("preloop secret set NPM_TOKEN")
        );
    }

    /// A read the engine holds only in an environment tier gets no
    /// repository-wide `set` offer — the only command this row could suggest
    /// would widen the value to every job of the repository — and the report
    /// says what to do instead. The row itself stays visible, with the tier it
    /// is stored in.
    #[test]
    fn an_environment_only_row_gets_no_repository_wide_set_offer() {
        let stored = Stored::from_listing(&[(
            "DEPLOY_KEY".to_owned(),
            Some("o/r".to_owned()),
            Some("prod".to_owned()),
        )]);
        let scan = scan_with(
            Some("o/r"),
            &[(
                ".github/workflows/deploy.yml",
                "on: push\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n      - \
                 run: deploy --key ${{ secrets.DEPLOY_KEY }}\n",
            )],
        );
        let rows = secret_rows(&scan, &stored);
        let row = rows
            .iter()
            .find(|row| row.name == "DEPLOY_KEY")
            .expect("the stored secret is read");
        assert_eq!(row.scope, None, "the reading job declares no environment");
        assert_eq!(
            row.environment_scope.as_deref(),
            Some("environment prod of o/r")
        );

        // The interactive pass and the JSON both come through `set_command`:
        // neither may offer a repository-wide write for this row.
        assert_eq!(
            set_command(&row.name, Some("o/r"), row.environment_scope.as_deref()),
            None
        );
        let json = secret_rows_json(&scan, &rows);
        assert_eq!(json[0]["command"], Value::Null);
        assert_eq!(
            json[0]["environment_scope"],
            json!("environment prod of o/r"),
            "the tier it is stored in is kept in the JSON"
        );
        assert_eq!(json[0]["configured"], Value::Null);
        assert_eq!(secrets_decision(&rows), "missing");

        // A name nothing holds still gets the command at the same scope: the
        // refusal is about the environment tier, not about the target.
        let rows = secret_rows(&scan, &Stored::default());
        assert_eq!(
            set_command(
                &rows[0].name,
                Some("o/r"),
                rows[0].environment_scope.as_deref()
            )
            .as_deref(),
            Some("preloop secret set DEPLOY_KEY --repo o/r")
        );

        // The advice names the tier, how to declare it, and both deliberate
        // widenings — a repository-wide write stays a hand-made choice.
        let advice = environment_only_advice(row, Some("o/r"));
        assert!(advice.contains("environment prod of o/r"), "{advice}");
        assert!(advice.contains("`environment:`"), "{advice}");
        assert!(advice.contains("--env"), "{advice}");
        assert!(
            advice.contains("preloop secret set DEPLOY_KEY --repo o/r"),
            "{advice}"
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
    fn deep_composite_actions_are_read_by_both_paths() {
        // The engine has no depth limit for composite actions, so a checkout
        // scan and a GitHub tree listing must both read an action nested
        // deeper than any walk limit used to stop at.
        let deep = format!(
            ".github/actions/{}/action.yml",
            (1..=10)
                .map(|depth| depth.to_string())
                .collect::<Vec<_>>()
                .join("/")
        );
        let tree = json!({"tree": [
            {"path": ".github/workflows/ci.yml", "type": "blob"},
            {"path": deep, "type": "blob"},
        ]});
        assert_eq!(
            workflow_paths(&tree),
            vec![deep.clone(), ".github/workflows/ci.yml".to_owned()]
        );

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let nested = (1..=10).map(|depth| depth.to_string()).collect::<PathBuf>();
        let action = root.join(".github/actions").join(nested).join("action.yml");
        std::fs::create_dir_all(action.parent().unwrap()).unwrap();
        std::fs::write(&action, "runs: {}\n").unwrap();
        let mut notes = Vec::new();
        let files = scan_checkout(root, &mut notes);
        assert!(notes.is_empty());
        let paths: Vec<&str> = files.iter().map(|file| file.path.as_str()).collect();
        assert_eq!(paths, vec![deep.as_str()]);
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
        assert_eq!(
            arch_note("X64", &pool.host_arch).as_deref(),
            Some("X64 is this host's architecture (x86_64)")
        );
        assert_eq!(
            arch_note("x86_64", &pool.host_arch).as_deref(),
            Some("x86_64 is this host's architecture (x86_64)")
        );
        assert_eq!(
            arch_note("ARM64", &pool.host_arch).as_deref(),
            Some("ARM64 names aarch64, not this host's x86_64")
        );
        assert_eq!(
            arch_note("aarch64", &pool.host_arch).as_deref(),
            Some("aarch64 names aarch64, not this host's x86_64")
        );
        assert_eq!(arch_note("gpu-box", &pool.host_arch), None);
        let arm = verdict_message(Verdict::Undeclared, "ARM64", &pool);
        assert!(
            arm.contains("ARM64 names aarch64, not this host's x86_64"),
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
                "on: push\njobs:\n  named:\n    runs-on:\n      group: Default\n      labels: [self-hosted]\n  by_id:\n    runs-on:\n      group: '1'\n      labels: [self-hosted]\n  padded_id:\n    runs-on:\n      group: \"01\"\n      labels: [self-hosted]\n  signed_id:\n    runs-on:\n      group: '+1'\n      labels: [self-hosted]\n  unquoted:\n    runs-on:\n      group: 1\n      labels: [self-hosted]\n  external:\n    runs-on:\n      group: mac-pool\n      labels: [Linux]\n",
            )],
        );
        let report = label_report(&[scan], &pool);
        let row = |label: &str| {
            report
                .rows
                .iter()
                .find(|row| row.label == label)
                .unwrap_or_else(|| panic!("{label} row"))
        };
        // The scheduler parses the group as an integer, so `1`, `01`, and `+1`
        // all name the Default group, like the name `Default` itself.
        for label in ["group: Default", "group: 1", "group: 01", "group: +1"] {
            assert_eq!(row(label).verdict, Verdict::Default, "{label}");
        }
        // An unquoted numeric group is not a string and the engine's own
        // `RunsOn::group()` drops it: such a job schedules with no group
        // constraint, so only the quoted job carries the `group: 1` row.
        assert_eq!(row("group: 1").uses.len(), 1, "{:?}", row("group: 1").uses);
        assert!(row("group: 1").uses[0].contains("by_id"));
        assert_eq!(row("group: mac-pool").verdict, Verdict::RunnerGroup);
        assert_eq!(report.decision(), "warning");
    }

    #[test]
    fn the_declare_line_skips_labels_of_another_architecture() {
        let pool = pool(OFFICIAL_GOLDEN);
        let scan = scan_with(
            Some("o/r"),
            &[(
                ".github/workflows/agents.yml",
                "on: push\njobs:\n  arm:\n    runs-on: ARM64\n  engine_arm:\n    runs-on: aarch64\n  same_host:\n    runs-on: X64\n  gpu:\n    runs-on: gpu-box\n",
            )],
        );
        let report = label_report(&[scan], &pool);
        // `X64` names this host, so its jobs could be served by declaring it;
        // `ARM64` and `aarch64` name the other architecture, so declaring them
        // would advertise a runner that cannot serve those jobs.
        assert_eq!(
            declare_line(&pool, &report).as_deref(),
            Some("PRELOOP_RUNNER_LABELS=custom-lab,gpu-box,X64")
        );
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

    /// The remote scan end to end against a stub GitHub: the tree listing names
    /// the files, the contents endpoint hands over their bytes, and the rows
    /// come from what the repository actually contains. This is the path the
    /// repository-name preflight and the credential load sit on.
    #[tokio::test]
    async fn a_remote_target_is_scanned_through_the_github_api() {
        let ci = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      \
                  - run: deploy --key ${{ secrets.DEPLOY_KEY }}\n";
        let action =
            "runs:\n  using: composite\n  steps:\n    - run: echo ${{ secrets.NPM_TOKEN }}\n";
        let tree = json!({
            "truncated": false,
            "tree": [
                {"type": "blob", "path": ".github/workflows/ci.yml"},
                {"type": "blob", "path": ".github/actions/deploy/action.yml"},
                {"type": "blob", "path": "src/main.rs"},
            ],
        });
        let router = {
            let (ci, action, tree) = (ci.to_owned(), action.to_owned(), tree.clone());
            axum::Router::new()
                .route(
                    "/repos/o/r",
                    axum::routing::get(|| async { axum::Json(json!({"default_branch": "main"})) }),
                )
                .route(
                    "/repos/o/r/git/trees/main",
                    axum::routing::get(move || {
                        let tree = tree.clone();
                        async move { axum::Json(tree) }
                    }),
                )
                .route(
                    "/repos/o/r/contents/.github/workflows/ci.yml",
                    axum::routing::get(move || {
                        let ci = ci.clone();
                        async move { ci }
                    }),
                )
                .route(
                    "/repos/o/r/contents/.github/actions/deploy/action.yml",
                    axum::routing::get(move || {
                        let action = action.clone();
                        async move { action }
                    }),
                )
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut config = ConfigFile::default();
        config.github.legacy_pat = Some("stub-token".to_owned());

        let _env_lock = crate::TEST_ENV_LOCK.lock().await;
        let previous = std::env::var_os("PRELOOP_GITHUB_API_URL");
        // SAFETY: TEST_ENV_LOCK serializes this process-global env access, and
        // the scan reaches GitHub through this variable alone.
        unsafe { std::env::set_var("PRELOOP_GITHUB_API_URL", &base) };
        let scan = read_target(
            Target {
                repo: Some("o/r".to_owned()),
                checkout: None,
            },
            &config,
        )
        .await;
        let refused = read_target(
            Target {
                repo: Some("o/r/../../users/x?y".to_owned()),
                checkout: None,
            },
            &config,
        )
        .await;
        match previous {
            Some(value) => unsafe { std::env::set_var("PRELOOP_GITHUB_API_URL", value) },
            None => unsafe { std::env::remove_var("PRELOOP_GITHUB_API_URL") },
        }
        drop(_env_lock);

        assert_eq!(scan.source, "github");
        assert_eq!(
            scan.files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec![
                ".github/actions/deploy/action.yml",
                ".github/workflows/ci.yml"
            ],
            "only workflow files and composite actions are read"
        );
        let rows = secret_rows(&scan, &Stored::default());
        assert_eq!(
            rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
            vec!["DEPLOY_KEY", "NPM_TOKEN"]
        );
        assert!(rows.iter().all(|row| row.scope.is_none()));

        // A name that is not a plain `owner/repo` is refused before a token is
        // loaded, so nothing is sent to a host the caller never chose.
        assert_eq!(refused.source, "none");
        assert!(
            refused
                .skipped
                .as_deref()
                .unwrap_or_default()
                .contains("is not a GitHub"),
            "{:?}",
            refused.skipped
        );
    }

    #[tokio::test]
    async fn a_repo_that_is_not_a_slug_is_refused_before_any_request() {
        let config = ConfigFile::default();
        for bad in ["o/r/../../app/hook/deliveries", "o/r?x=1", "o/r#f", "a/b/c"] {
            let error = fetch_repo(&config, bad).await.unwrap_err();
            assert!(
                error.contains("owner/repository"),
                "{bad} was not refused before a request: {error}"
            );
        }
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
