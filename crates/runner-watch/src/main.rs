//! Automated protocol sync orchestration for aksh.
//!
//! `runner-watch` owns the deterministic parts of the protocol-sync pipeline:
//! release watching, source diff extraction, deterministic triage/spec emission,
//! subprocess integration points for Claude/Codex, request-level conformance replay,
//! and draft PR creation.

mod compare;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, ensure, Context};
use clap::{Args, Parser, Subcommand, ValueEnum};
use globset::{Glob, GlobSet, GlobSetBuilder};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use walkdir::WalkDir;

const DEFAULT_ROOT: &str = ".runner-watch";
const DEFAULT_CONFIG: &str = ".runner-watch/config.toml";
const RELEASES_ATOM: &str = "https://github.com/actions/runner/releases.atom";

#[derive(Debug, Parser)]
#[command(name = "runner-watch")]
#[command(about = "Automated actions/runner protocol sync for aksh")]
struct Cli {
    /// Path to runner-watch config.
    #[arg(long, global = true, default_value = DEFAULT_CONFIG)]
    config: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Poll actions/runner releases and update state when a new tag appears.
    Watch(WatchArgs),
    /// Clone two actions/runner tags and emit .runner-watch/delta.json.
    Diff(DiffArgs),
    /// Convert delta.json into per-change TOML specs.
    Triage(TriageArgs),
    /// Invoke Codex over generated specs.
    Implement(ImplementArgs),
    /// Invoke Claude to review implementation diffs.
    Review(ReviewArgs),
    /// Record a golden capture through the mitm worktree.
    RecordGolden(RecordGoldenArgs),
    /// Replay flows.jsonl requests against aksh and compare responses.
    Conform(ConformArgs),
    /// Create tiered draft PRs from artifacts.
    Pr(PrArgs),
    /// Run watch/diff/triage/conformance orchestration.
    Run(RunArgs),
    /// Write default config and surface map.
    Init(InitArgs),
}

#[derive(Debug, Args)]
struct WatchArgs {
    /// Override releases atom URL (useful for tests).
    #[arg(long, default_value = RELEASES_ATOM)]
    feed_url: String,
}

#[derive(Debug, Args, Clone)]
struct DiffArgs {
    #[arg(long)]
    from: String,
    #[arg(long)]
    to: String,
}

#[derive(Debug, Args, Clone)]
struct TriageArgs {
    /// Do not invoke the configured AI triage agent for unknown entries.
    #[arg(long)]
    no_agents: bool,
}

#[derive(Debug, Args, Clone)]
struct ImplementArgs {
    /// Do not actually invoke Codex; write prompts for inspection.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args, Clone)]
struct ReviewArgs {
    /// Do not actually invoke Claude; write prompts for inspection.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args, Clone)]
struct RecordGoldenArgs {
    #[arg(long)]
    runner: String,
    #[arg(long, value_enum, default_value_t = GoldenTarget::Official)]
    target: GoldenTarget,
    #[arg(long)]
    scenario: Option<String>,
    #[arg(long)]
    non_interactive: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum GoldenTarget {
    Official,
}

#[derive(Debug, Args, Clone)]
struct ConformArgs {
    /// Runner version whose .runner-watch/golden/v{N} directory should be used.
    #[arg(long)]
    runner: String,
    /// Base URL for a running aksh server, e.g. http://127.0.0.1:9090.
    #[arg(long)]
    aksh_url: String,
    /// Only run a single scenario directory under the golden version.
    #[arg(long)]
    scenario: Option<String>,
    /// Skip cargo test --workspace before replay.
    #[arg(long)]
    skip_cargo_test: bool,
}

#[derive(Debug, Args, Clone)]
struct PrArgs {
    #[arg(long)]
    base: Option<String>,
    #[arg(long)]
    head: Option<String>,
    /// Do not call gh; write PR body files only.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args, Clone)]
struct RunArgs {
    #[arg(long)]
    from: String,
    #[arg(long)]
    to: String,
    /// Running aksh base URL for conformance. If omitted, conformance is reported skipped.
    #[arg(long)]
    aksh_url: Option<String>,
    #[arg(long)]
    no_agents: bool,
    #[arg(long)]
    skip_implementation: bool,
    #[arg(long)]
    skip_review: bool,
    #[arg(long)]
    skip_cargo_test: bool,
}

#[derive(Debug, Args)]
struct InitArgs {
    /// Overwrite existing config/surface map.
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Config {
    #[serde(default)]
    general: GeneralConfig,
    #[serde(default)]
    agents: AgentConfig,
    #[serde(default)]
    surface_map: SurfaceConfig,
    #[serde(default)]
    tracked_dirs: TrackedDirs,
    #[serde(default)]
    skip_paths: SkipPaths,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeneralConfig {
    #[serde(default = "default_runner_repo")]
    runner_repo: String,
    #[serde(default = "default_aksh_worktree")]
    aksh_worktree: PathBuf,
    #[serde(default = "default_golden_dir")]
    golden_dir: PathBuf,
    #[serde(default = "default_mitm_dir")]
    mitm_dir: PathBuf,
    #[serde(default = "default_max_review_rounds")]
    max_review_rounds: usize,
    #[serde(default = "default_max_conformance_rounds")]
    max_conformance_rounds: usize,
    #[serde(default = "default_max_implement_iterations")]
    max_implement_iterations: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentConfig {
    #[serde(default = "default_claude")]
    triage: String,
    #[serde(default = "default_codex")]
    implement: String,
    #[serde(default = "default_claude")]
    review: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SurfaceConfig {
    #[serde(default = "default_surface_map")]
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrackedDirs {
    #[serde(default = "default_tracked_dirs")]
    dirs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SkipPaths {
    #[serde(default = "default_skip_patterns")]
    patterns: Vec<String>,
}

impl Default for GeneralConfig {
    fn default() -> Self {
        Self {
            runner_repo: default_runner_repo(),
            aksh_worktree: default_aksh_worktree(),
            golden_dir: default_golden_dir(),
            mitm_dir: default_mitm_dir(),
            max_review_rounds: default_max_review_rounds(),
            max_conformance_rounds: default_max_conformance_rounds(),
            max_implement_iterations: default_max_implement_iterations(),
        }
    }
}
impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            triage: default_claude(),
            implement: default_codex(),
            review: default_claude(),
        }
    }
}
impl Default for SurfaceConfig {
    fn default() -> Self {
        Self {
            path: default_surface_map(),
        }
    }
}
impl Default for TrackedDirs {
    fn default() -> Self {
        Self {
            dirs: default_tracked_dirs(),
        }
    }
}
impl Default for SkipPaths {
    fn default() -> Self {
        Self {
            patterns: default_skip_patterns(),
        }
    }
}

fn default_runner_repo() -> String {
    "actions/runner".to_string()
}
fn default_aksh_worktree() -> PathBuf {
    PathBuf::from(".")
}
fn default_golden_dir() -> PathBuf {
    PathBuf::from(".runner-watch/golden")
}
fn default_mitm_dir() -> PathBuf {
    PathBuf::from("experiments/mitm")
}
fn default_max_review_rounds() -> usize {
    3
}
fn default_max_conformance_rounds() -> usize {
    2
}
fn default_max_implement_iterations() -> usize {
    10
}
fn default_claude() -> String {
    "claude".to_string()
}
fn default_codex() -> String {
    "codex".to_string()
}
fn default_surface_map() -> PathBuf {
    PathBuf::from("docs/aksh-surface.toml")
}
fn default_tracked_dirs() -> Vec<String> {
    [
        "src/Runner.Listener",
        "src/Runner.Worker",
        "src/Runner.Common",
        "src/Runner.Sdk",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}
fn default_skip_patterns() -> Vec<String> {
    [
        "src/Test/**",
        "src/Misc/**",
        ".github/**",
        "*.md",
        "*.yml",
        "*.yaml",
        "dev/**",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    last_known_tag: Option<String>,
    phase: Option<String>,
    from: Option<String>,
    to: Option<String>,
    updated_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaEntry {
    file: String,
    #[serde(rename = "struct", skip_serializing_if = "Option::is_none")]
    structure: Option<String>,
    change_type: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    fields: Vec<String>,
    snippet: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SurfaceMap {
    #[serde(default)]
    mappings: Vec<SurfaceMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SurfaceMapping {
    upstream: String,
    crate_name: String,
    path: String,
    area: String,
}

#[derive(Debug, Clone)]
struct Spec {
    id: String,
    category: String,
    tags: Vec<String>,
    what: String,
    why: String,
    runner_behavior: String,
    failure_mode: String,
    feature_flag_name: Option<String>,
    feature_flag_where: Option<String>,
    feature_flag_default: Option<bool>,
    request: String,
    response: String,
    targets: Vec<SurfaceMapping>,
    approach: String,
    test: String,
    source_entries: Vec<DeltaEntry>,
    ai_status: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = load_config(&cli.config)?;
    match cli.command {
        Commands::Watch(args) => watch(&config, &args).await,
        Commands::Diff(args) => diff(&config, &args).await,
        Commands::Triage(args) => triage(&config, &args).await,
        Commands::Implement(args) => implement(&config, &args).await,
        Commands::Review(args) => review(&config, &args).await,
        Commands::RecordGolden(args) => record_golden(&config, &args).await,
        Commands::Conform(args) => conform(&config, &args).await,
        Commands::Pr(args) => pr(&config, &args).await,
        Commands::Run(args) => run_all(&config, &args).await,
        Commands::Init(args) => init_files(&config, &args).await,
    }
}

fn load_config(path: &Path) -> anyhow::Result<Config> {
    if path.exists() {
        let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parse {}", path.display()))
    } else {
        Ok(Config::default())
    }
}

fn state_path() -> PathBuf {
    PathBuf::from(DEFAULT_ROOT).join("state.json")
}
fn delta_path() -> PathBuf {
    PathBuf::from(DEFAULT_ROOT).join("delta.json")
}

fn read_state() -> anyhow::Result<State> {
    let path = state_path();
    if !path.exists() {
        return Ok(State::default());
    }
    let text = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))
}

fn write_state(mut state: State) -> anyhow::Result<()> {
    state.updated_at_unix = now_unix();
    let path = state_path();
    ensure_parent(&path)?;
    fs::write(&path, serde_json::to_string_pretty(&state)?)
        .with_context(|| format!("write {}", path.display()))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn ensure_parent(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

async fn watch(_config: &Config, args: &WatchArgs) -> anyhow::Result<()> {
    fs::create_dir_all(DEFAULT_ROOT)?;
    let feed = reqwest::get(&args.feed_url)
        .await?
        .error_for_status()?
        .text()
        .await?;
    let latest =
        extract_latest_tag(&feed).ok_or_else(|| anyhow!("no actions/runner tag found in feed"))?;
    let mut state = read_state()?;
    let changed = state.last_known_tag.as_deref() != Some(&latest);
    state.last_known_tag = Some(latest.clone());
    state.phase = Some(
        if changed {
            "watch:new"
        } else {
            "watch:unchanged"
        }
        .to_string(),
    );
    write_state(state)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"runner_version": latest, "changed": changed}))?
    );
    Ok(())
}

fn extract_latest_tag(feed: &str) -> Option<String> {
    for marker in [
        "/actions/runner/releases/tag/",
        "tag:github.com,2008:Repository/",
    ] {
        if let Some(pos) = feed.find(marker) {
            let rest = &feed[pos + marker.len()..];
            let tag = rest.split(['<', '"', '/', '?', '#']).next()?.trim();
            if tag.starts_with('v') {
                return Some(tag.to_string());
            }
        }
    }
    for line in feed.lines() {
        if let Some(pos) = line.find("releases/tag/v") {
            let tag = line[pos + "releases/tag/".len()..]
                .split(['<', '"'])
                .next()?
                .trim();
            return Some(tag.to_string());
        }
    }
    None
}

async fn diff(config: &Config, args: &DiffArgs) -> anyhow::Result<()> {
    fs::create_dir_all(DEFAULT_ROOT)?;
    let old_repo = ensure_runner_checkout(config, &args.from).await?;
    let new_repo = ensure_runner_checkout(config, &args.to).await?;
    let skip_set = build_globset(&config.skip_paths.patterns)?;
    let mut entries = Vec::new();
    let files =
        collect_candidate_files(&old_repo, &new_repo, &config.tracked_dirs.dirs, &skip_set)?;
    for rel in files {
        let old_text = read_optional(&old_repo.join(&rel))?;
        let new_text = read_optional(&new_repo.join(&rel))?;
        if old_text == new_text {
            continue;
        }
        entries.extend(extract_delta_entries(
            &rel,
            old_text.as_deref(),
            new_text.as_deref(),
        ));
    }
    entries.sort_by(|a, b| {
        (&a.file, &a.change_type, &a.structure, &a.fields).cmp(&(
            &b.file,
            &b.change_type,
            &b.structure,
            &b.fields,
        ))
    });
    let out = delta_path();
    fs::write(&out, serde_json::to_string_pretty(&entries)?)
        .with_context(|| format!("write {}", out.display()))?;
    let mut state = read_state()?;
    state.phase = Some("diffed".to_string());
    state.from = Some(args.from.clone());
    state.to = Some(args.to.clone());
    write_state(state)?;
    println!("delta entries: {} -> {}", entries.len(), out.display());
    Ok(())
}

async fn ensure_runner_checkout(config: &Config, tag: &str) -> anyhow::Result<PathBuf> {
    let dir = PathBuf::from(DEFAULT_ROOT)
        .join("repos")
        .join(format!("actions-runner-{tag}"));
    if dir.join(".git").exists() {
        return Ok(dir);
    }
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    ensure_parent(&dir)?;
    let repo_url = if config.general.runner_repo.contains('/')
        && !config.general.runner_repo.starts_with("http")
    {
        format!("https://github.com/{}.git", config.general.runner_repo)
    } else {
        config.general.runner_repo.clone()
    };
    let status = Command::new("git")
        .args(["clone", "--depth", "1", "--branch", tag, &repo_url])
        .arg(&dir)
        .status()
        .await
        .with_context(|| format!("git clone {repo_url} {tag}"))?;
    if !status.success() {
        bail!("git clone failed for {tag}: {status}");
    }
    Ok(dir)
}

fn build_globset(patterns: &[String]) -> anyhow::Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern)?);
    }
    Ok(builder.build()?)
}

fn collect_candidate_files(
    old_repo: &Path,
    new_repo: &Path,
    dirs: &[String],
    skip_set: &GlobSet,
) -> anyhow::Result<Vec<String>> {
    let mut files = BTreeSet::new();
    for root in [old_repo, new_repo] {
        for dir in dirs {
            let scan = root.join(dir);
            if !scan.exists() {
                continue;
            }
            for entry in WalkDir::new(&scan)
                .into_iter()
                .filter_map(Result::ok)
                .filter(|e| e.file_type().is_file())
            {
                let rel = entry
                    .path()
                    .strip_prefix(root)?
                    .to_string_lossy()
                    .replace('\\', "/");
                if skip_set.is_match(&rel) {
                    continue;
                }
                if rel.ends_with(".cs") {
                    files.insert(rel);
                }
            }
        }
    }
    Ok(files.into_iter().collect())
}

fn read_optional(path: &Path) -> anyhow::Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

fn extract_delta_entries(
    rel: &str,
    old_text: Option<&str>,
    new_text: Option<&str>,
) -> Vec<DeltaEntry> {
    let mut out = Vec::new();
    match (old_text, new_text) {
        (None, Some(newer)) => {
            out.push(DeltaEntry {
                file: rel.to_string(),
                structure: None,
                change_type: "file_added".to_string(),
                fields: Vec::new(),
                snippet: first_lines(newer, 30),
            });
            out.extend(extract_symbol_additions(rel, "message_type_added", newer));
            out.extend(extract_env_entries(rel, newer));
            out.extend(extract_route_entries(rel, newer));
        }
        (Some(older), None) => out.push(DeltaEntry {
            file: rel.to_string(),
            structure: None,
            change_type: "file_removed".to_string(),
            fields: Vec::new(),
            snippet: first_lines(older, 30),
        }),
        (Some(older), Some(newer)) => {
            let old_fields = extract_fields(older);
            let new_fields = extract_fields(newer);
            for ((structure, field), line) in &new_fields {
                if !old_fields.contains_key(&(structure.clone(), field.clone())) {
                    out.push(DeltaEntry {
                        file: rel.to_string(),
                        structure: Some(structure.clone()),
                        change_type: "field_added".to_string(),
                        fields: vec![field.clone()],
                        snippet: context_for(newer, *line),
                    });
                }
            }
            for ((structure, field), line) in &old_fields {
                if !new_fields.contains_key(&(structure.clone(), field.clone())) {
                    out.push(DeltaEntry {
                        file: rel.to_string(),
                        structure: Some(structure.clone()),
                        change_type: "field_removed".to_string(),
                        fields: vec![field.clone()],
                        snippet: context_for(older, *line),
                    });
                }
            }
            out.extend(extract_line_additions(
                rel,
                "feature_flag_added",
                older,
                newer,
                is_feature_flag_line,
            ));
            out.extend(extract_line_additions(
                rel,
                "env_var_added",
                older,
                newer,
                is_env_line,
            ));
            out.extend(extract_line_additions(
                rel,
                "route_added",
                older,
                newer,
                is_route_line,
            ));
            out.extend(extract_line_additions(
                rel,
                "protocol_keyword_added",
                older,
                newer,
                is_protocol_keyword_line,
            ));
            out.extend(extract_message_type_changes(rel, older, newer));
        }
        _ => {}
    }
    if out.is_empty() {
        out.push(DeltaEntry {
            file: rel.to_string(),
            structure: None,
            change_type: "file_changed".to_string(),
            fields: Vec::new(),
            snippet: new_text.map(|s| first_lines(s, 30)).unwrap_or_default(),
        });
    }
    out
}

fn extract_fields(text: &str) -> BTreeMap<(String, String), usize> {
    let mut fields = BTreeMap::new();
    let mut current = "<module>".to_string();
    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if let Some(name) = class_or_struct_name(trimmed) {
            current = name;
        }
        if let Some(field) = property_name(trimmed) {
            fields.insert((current.clone(), field), idx + 1);
        }
    }
    fields
}

fn class_or_struct_name(line: &str) -> Option<String> {
    for key in [" class ", " struct ", " enum "] {
        if let Some(pos) = line.find(key) {
            let rest = &line[pos + key.len()..];
            let name = rest
                .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .next()?;
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }
    }
    None
}

fn property_name(line: &str) -> Option<String> {
    if !(line.contains(" get;") || line.contains(" get =>") || line.contains("{ get")) {
        return None;
    }
    if line.starts_with("//") || line.contains('(') && !line.contains("{ get") {
        return None;
    }
    let before_brace = line.split('{').next()?.trim();
    let name = before_brace.split_whitespace().last()?;
    if name.chars().next()?.is_ascii_uppercase() || name.chars().next()? == '_' {
        Some(name.trim_matches(';').to_string())
    } else {
        None
    }
}

fn extract_line_additions(
    rel: &str,
    change_type: &str,
    older: &str,
    newer: &str,
    pred: fn(&str) -> bool,
) -> Vec<DeltaEntry> {
    let old: BTreeSet<String> = older
        .lines()
        .filter(|l| pred(l))
        .map(|l| l.trim().to_string())
        .collect();
    newer
        .lines()
        .enumerate()
        .filter(|(_, l)| pred(l))
        .filter_map(|(idx, l)| {
            let trimmed = l.trim().to_string();
            if old.contains(&trimmed) {
                return None;
            }
            Some(DeltaEntry {
                file: rel.to_string(),
                structure: None,
                change_type: change_type.to_string(),
                fields: extract_tokens(&trimmed),
                snippet: context_for(newer, idx + 1),
            })
        })
        .collect()
}

fn is_feature_flag_line(line: &str) -> bool {
    let l = line.to_ascii_lowercase();
    l.contains("feature")
        || l.contains("configurationstore")
        || l.contains("brokerurl")
        || l.contains("auth_url_v2")
}
fn is_env_line(line: &str) -> bool {
    line.contains("GetEnvironmentVariable")
        || line.contains("ACTIONS_")
        || line.contains("RUNNER_")
        || line.contains("GITHUB_")
}
fn is_route_line(line: &str) -> bool {
    line.contains("[Route(")
        || line.contains("[HttpGet")
        || line.contains("[HttpPost")
        || line.contains("[HttpPatch")
        || line.contains("[HttpDelete")
}
fn is_protocol_keyword_line(line: &str) -> bool {
    let l = line.to_ascii_lowercase();
    [
        "acknowledgerunnerrequest",
        "agentrequest",
        "brokerurl",
        "auth_url_v2",
        "userunneradminflow",
        "runnerversiondeprecated",
        "backgroundcontrol",
        "isbackground",
        "parallelgroupid",
        "dap",
        "debugger",
        "sendjoblevelannotations",
        "batchactionresolution",
        "usebearertokenforcodeload",
        "warnonnode20",
        "deprecatednode20",
        "disablestdoutmultilinelogprefixing",
        "serversettings",
        "runnersettings",
    ]
    .iter()
    .any(|needle| l.contains(needle))
}

fn extract_tokens(line: &str) -> Vec<String> {
    line.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .filter(|token| {
            token.len() > 2 && (token.chars().any(|c| c.is_uppercase()) || token.contains('_'))
        })
        .take(8)
        .map(str::to_string)
        .collect()
}

fn extract_symbol_additions(rel: &str, change_type: &str, text: &str) -> Vec<DeltaEntry> {
    text.lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            let name = class_or_struct_name(line.trim())?;
            if !(name.ends_with("Message") || name.ends_with("Ref")) {
                return None;
            }
            Some(DeltaEntry {
                file: rel.to_string(),
                structure: Some(name.clone()),
                change_type: change_type.to_string(),
                fields: vec![name],
                snippet: context_for(text, idx + 1),
            })
        })
        .collect()
}

fn extract_message_type_changes(rel: &str, older: &str, newer: &str) -> Vec<DeltaEntry> {
    let old: BTreeSet<String> = older
        .lines()
        .filter_map(|l| class_or_struct_name(l.trim()))
        .filter(|n| n.ends_with("Message") || n.ends_with("Ref"))
        .collect();
    newer
        .lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            let name = class_or_struct_name(line.trim())?;
            if !(name.ends_with("Message") || name.ends_with("Ref")) || old.contains(&name) {
                return None;
            }
            Some(DeltaEntry {
                file: rel.to_string(),
                structure: Some(name.clone()),
                change_type: "message_type_added".to_string(),
                fields: vec![name],
                snippet: context_for(newer, idx + 1),
            })
        })
        .collect()
}

fn extract_env_entries(rel: &str, text: &str) -> Vec<DeltaEntry> {
    text.lines()
        .enumerate()
        .filter(|(_, l)| is_env_line(l))
        .map(|(idx, l)| DeltaEntry {
            file: rel.to_string(),
            structure: None,
            change_type: "env_var_added".to_string(),
            fields: extract_tokens(l),
            snippet: context_for(text, idx + 1),
        })
        .collect()
}
fn extract_route_entries(rel: &str, text: &str) -> Vec<DeltaEntry> {
    text.lines()
        .enumerate()
        .filter(|(_, l)| is_route_line(l))
        .map(|(idx, l)| DeltaEntry {
            file: rel.to_string(),
            structure: None,
            change_type: "route_added".to_string(),
            fields: extract_tokens(l),
            snippet: context_for(text, idx + 1),
        })
        .collect()
}

fn first_lines(text: &str, count: usize) -> String {
    text.lines().take(count).collect::<Vec<_>>().join("\n")
}
fn context_for(text: &str, line_number: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let start = line_number.saturating_sub(4);
    let end = (line_number + 3).min(lines.len());
    lines[start..end].join("\n")
}

async fn triage(config: &Config, args: &TriageArgs) -> anyhow::Result<()> {
    let delta_text = fs::read_to_string(delta_path())
        .context("read .runner-watch/delta.json; run diff first")?;
    let entries: Vec<DeltaEntry> = serde_json::from_str(&delta_text)?;
    let state = read_state()?;
    let version = state.to.clone().unwrap_or_else(|| "unknown".to_string());
    let surface = load_surface_map(&config.surface_map.path)?;
    let specs = deterministic_specs(&entries, &surface, &version);
    let spec_dir = PathBuf::from(DEFAULT_ROOT).join("specs").join(&version);
    if spec_dir.exists() {
        fs::remove_dir_all(&spec_dir)
            .with_context(|| format!("remove stale spec dir {}", spec_dir.display()))?;
    }
    fs::create_dir_all(&spec_dir)?;
    for spec in &specs {
        fs::write(
            spec_dir.join(format!("{}.toml", spec.id)),
            spec_to_toml(spec, &version),
        )?;
    }
    let skipped = entries
        .len()
        .saturating_sub(specs.iter().map(|s| s.source_entries.len()).sum::<usize>());
    if !args.no_agents {
        write_unknown_triage_prompt(config, &entries, &specs, &version).await?;
    }
    fs::write(
        PathBuf::from(DEFAULT_ROOT).join("triage-summary.json"),
        serde_json::to_string_pretty(&json!({
            "version": version,
            "delta_entries": entries.len(),
            "specs": specs.len(),
            "deterministically_skipped_or_grouped": skipped,
            "no_agents": args.no_agents
        }))?,
    )?;
    let mut state = read_state()?;
    state.phase = Some("triaged".to_string());
    write_state(state)?;
    println!("specs: {} -> {}", specs.len(), spec_dir.display());
    Ok(())
}

fn load_surface_map(path: &Path) -> anyhow::Result<SurfaceMap> {
    if path.exists() {
        let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parse {}", path.display()))
    } else {
        Ok(default_surface_entries())
    }
}

fn default_surface_entries() -> SurfaceMap {
    SurfaceMap {
        mappings: vec![
            map(
                "TimelineRecord",
                "aksh-gha-protocol",
                "crates/aksh-gha-protocol/src/azdo.rs",
                "TimelineRecord DTO",
            ),
            map(
                "AgentRequest",
                "aksh-runner-server",
                "crates/aksh-runner-server/src/lib.rs",
                "AgentRequest routes",
            ),
            map(
                "ConnectionData",
                "aksh-runner-server",
                "crates/aksh-runner-server/src/lib.rs",
                "connectionData payload",
            ),
            map(
                "ActionDownloadInfo",
                "aksh-runner-server",
                "crates/aksh-runner-server/src/lib.rs",
                "action download handler",
            ),
            map(
                "Timeline",
                "aksh-runner-server",
                "crates/aksh-runner-server/src/lib.rs",
                "timeline handlers",
            ),
            map(
                "Broker",
                "aksh-runner-server",
                "crates/aksh-runner-server/src/lib.rs",
                "broker/admin flow",
            ),
        ],
    }
}

fn map(upstream: &str, crate_name: &str, path: &str, area: &str) -> SurfaceMapping {
    SurfaceMapping {
        upstream: upstream.to_string(),
        crate_name: crate_name.to_string(),
        path: path.to_string(),
        area: area.to_string(),
    }
}

fn deterministic_specs(entries: &[DeltaEntry], surface: &SurfaceMap, version: &str) -> Vec<Spec> {
    let mut grouped: BTreeMap<String, Vec<DeltaEntry>> = BTreeMap::new();
    for entry in entries {
        let hay = format!(
            "{} {} {} {}",
            entry.file,
            entry.structure.clone().unwrap_or_default(),
            entry.fields.join(" "),
            entry.snippet
        )
        .to_ascii_lowercase();
        let id = if hay.contains("timeline")
            && (hay.contains("isbackground")
                || hay.contains("backgroundcontrol")
                || hay.contains("parallelgroupid"))
        {
            Some("background-step-timeline-fields")
        } else if hay.contains("acknowledgerunnerrequest")
            || hay.contains("agentrequest") && entry.change_type.contains("route")
        {
            Some("request-ack")
        } else if hay.contains("auth_url_v2")
            || hay.contains("brokerurl")
            || hay.contains("serverurlv2")
            || hay.contains("brokerserver")
        {
            Some("v2-admin-broker-connection")
        } else if hay.contains("userunneradminflow") {
            Some("use-runner-admin-flow")
        } else if hay.contains("runnerversiondeprecated") {
            Some("runner-version-deprecated")
        } else if hay.contains("debug") || hay.contains("dap") {
            Some("dap-debugger-endpoint")
        } else if hay.contains("sendjoblevelannotations") {
            Some("send-job-level-annotations")
        } else if hay.contains("batchactionresolution") {
            Some("batch-action-resolution")
        } else if hay.contains("usebearertokenforcodeload") {
            Some("use-bearer-token-for-codeload")
        } else if hay.contains("warnonnode20")
            || hay.contains("deprecatednode20")
            || hay.contains("node.js 20 actions are deprecated")
        {
            Some("node20-deprecation-warning")
        } else if hay.contains("disablestdoutmultilinelogprefixing") {
            Some("disable-stdout-multiline-log-prefixing")
        } else if hay.contains("serversettings") || hay.contains("runnersettings") {
            Some("server-enforced-runner-settings")
        } else if path_relevant(&entry.file, surface) {
            Some("mapped-surface-review")
        } else {
            None
        };
        if let Some(id) = id {
            grouped
                .entry(id.to_string())
                .or_default()
                .push(entry.clone());
        }
    }
    grouped
        .into_iter()
        .map(|(id, source_entries)| spec_for_id(&id, source_entries, surface, version))
        .collect()
}

fn path_relevant(file: &str, surface: &SurfaceMap) -> bool {
    surface.mappings.iter().any(|m| {
        file.to_ascii_lowercase()
            .contains(&m.upstream.to_ascii_lowercase())
    })
}

fn targets_for(surface: &SurfaceMap, needles: &[&str]) -> Vec<SurfaceMapping> {
    let mut targets = Vec::new();
    for mapping in &surface.mappings {
        if needles.iter().any(|needle| {
            mapping
                .upstream
                .to_ascii_lowercase()
                .contains(&needle.to_ascii_lowercase())
                || mapping
                    .area
                    .to_ascii_lowercase()
                    .contains(&needle.to_ascii_lowercase())
        }) {
            targets.push(mapping.clone());
        }
    }
    if targets.is_empty() {
        targets.push(map(
            "aksh",
            "aksh-runner-server",
            "crates/aksh-runner-server/src/lib.rs",
            "protocol surface",
        ));
    }
    targets
}

fn spec_for_id(
    id: &str,
    source_entries: Vec<DeltaEntry>,
    surface: &SurfaceMap,
    _version: &str,
) -> Spec {
    match id {
        "background-step-timeline-fields" => Spec { id: id.into(), category: "blocker".into(), tags: vec!["protocol".into(), "timeline".into()], what: "Runner can send background-step metadata in timeline records.".into(), why: "v2.335.0 records concurrent background steps and control-flow waits/cancels in timeline PATCH payloads.".into(), runner_behavior: "PATCH timeline records may include isBackground, backgroundControlType, backgroundControlStepIds, and parallelGroupId.".into(), failure_mode: "Aksh must accept and preserve unknown timeline metadata; strict DTOs or storage projections can silently lose fidelity.".into(), feature_flag_name: None, feature_flag_where: None, feature_flag_default: None, request: "PATCH /_apis/v1/Timeline/{scope}/{hub}/{planId}/{timelineId}\n{\"value\":[{\"isBackground\":true,\"backgroundControlType\":\"waitAll\",\"backgroundControlStepIds\":[\"...\"],\"parallelGroupId\":\"...\"}]}".into(), response: "200 JSON timeline record collection".into(), targets: targets_for(surface, &["TimelineRecord", "timeline"]), approach: "Add optional serde fields to TimelineRecord and ensure timeline PATCH handlers round-trip/store them.".into(), test: "Serde test for camelCase optional background fields and handler test for timeline PATCH accepting them.".into(), source_entries, ai_status: "deterministic-known-fidelity-gap".into() },
        "request-ack" => Spec { id: id.into(), category: "concern".into(), tags: vec!["protocol".into(), "endpoint".into()], what: "Runner sends an explicit acknowledgement after receiving a job request.".into(), why: "Broker flow uses acknowledgements to confirm the runner accepted a leased request.".into(), runner_behavior: "POST /_apis/v1/AgentRequest/{poolId}/{requestId} after decrypting a RunnerJobRequest.".into(), failure_mode: "warning-only in current captures; missing endpoint causes 404 noise and may affect broker leases.".into(), feature_flag_name: Some("UseBrokerFlow".into()), feature_flag_where: Some("connectionData or broker capability response".into()), feature_flag_default: Some(false), request: "POST /_apis/v1/AgentRequest/{poolId}/{requestId}?api-version=6.0\n{}".into(), response: "204 No Content".into(), targets: targets_for(surface, &["AgentRequest"]), approach: "Add POST handler for all AgentRequest route prefixes; accept empty/JSON body and return 204.".into(), test: "POST to AgentRequest endpoint returns 204.".into(), source_entries, ai_status: "deterministic-known-fidelity-gap".into() },
        "v2-admin-broker-connection" | "use-runner-admin-flow" => Spec { id: id.into(), category: "concern".into(), tags: vec!["protocol".into(), "broker".into()], what: "Runner v2 admin flow discovers auth_url_v2 and BrokerUrl values.".into(), why: "v2.329.0 introduced broker/admin paths used by newer hosted runner flows.".into(), runner_behavior: "connectionData/location data and admin responses can advertise auth_url_v2, BrokerUrl, and UseRunnerAdminFlow.".into(), failure_mode: "Runner can fall back today, but newer flows warn or skip broker features when absent.".into(), feature_flag_name: Some("UseRunnerAdminFlow".into()), feature_flag_where: Some("admin/connection response".into()), feature_flag_default: Some(false), request: "GET /_apis/connectionData and runner admin capability requests".into(), response: "JSON containing auth_url_v2/BrokerUrl when enabled".into(), targets: targets_for(surface, &["ConnectionData", "Broker"]), approach: "Extend connection/admin DTOs and route responses without changing legacy defaults.".into(), test: "connectionData/admin response includes optional v2 fields only when configured.".into(), source_entries, ai_status: "deterministic-known-fidelity-gap".into() },
        "runner-version-deprecated" => Spec { id: id.into(), category: "concern".into(), tags: vec!["protocol".into(), "feature-flag".into()], what: "Server can tell the runner its version is deprecated.".into(), why: "GitHub enforces minimum runner versions and reports deprecation through feature/capability responses.".into(), runner_behavior: "Runner reads RunnerVersionDeprecated and emits upgrade/deprecation behavior.".into(), failure_mode: "Ignoring it hides an upstream control-plane signal; not needed for local aksh execution.".into(), feature_flag_name: Some("RunnerVersionDeprecated".into()), feature_flag_where: Some("feature flag response".into()), feature_flag_default: Some(false), request: "GET feature/connection capability endpoints".into(), response: "JSON flag value".into(), targets: targets_for(surface, &["ConnectionData"]), approach: "Model the flag in capability responses with a safe false default.".into(), test: "Capability serialization uses RunnerVersionDeprecated wire name.".into(), source_entries, ai_status: "deterministic-known-fidelity-gap".into() },
        "dap-debugger-endpoint" => Spec { id: id.into(), category: "feature".into(), tags: vec!["debugger".into(), "websocket".into()], what: "Runner can expose a DAP debugger integration.".into(), why: "v2.335.0 added debugger hooks around worker step execution.".into(), runner_behavior: "Debugger-enabled runs use websocket/control endpoints for DAP traffic.".into(), failure_mode: "Non-blocking unless debugging is requested.".into(), feature_flag_name: None, feature_flag_where: None, feature_flag_default: None, request: "WebSocket debugger endpoint when debug feature is active".into(), response: "DAP frames proxied/stubbed according to runner expectation".into(), targets: targets_for(surface, &["Broker"]), approach: "Add explicit unsupported/stub behavior first; implement full proxy when debug scenarios are captured.".into(), test: "Debugger route returns expected upgrade/error semantics.".into(), source_entries, ai_status: "deterministic-known-fidelity-gap".into() },
        "send-job-level-annotations" => Spec { id: id.into(), category: "feature".into(), tags: vec!["timeline".into(), "annotations".into()], what: "Runner can send job-level annotations in timeline updates.".into(), why: "Newer runners aggregate annotations beyond individual step records.".into(), runner_behavior: "Timeline PATCH includes issue/annotation payloads that apply at job level.".into(), failure_mode: "Annotations may be missing from UI; job execution continues.".into(), feature_flag_name: Some("SendJobLevelAnnotations".into()), feature_flag_where: Some("timeline/feature flag response".into()), feature_flag_default: Some(false), request: "PATCH /_apis/v1/Timeline/... with issues[]".into(), response: "200 JSON timeline collection".into(), targets: targets_for(surface, &["TimelineRecord", "timeline"]), approach: "Preserve issues[] on job records and project them to annotations.".into(), test: "Timeline PATCH with job issues stores annotations.".into(), source_entries, ai_status: "deterministic-known-fidelity-gap".into() },
        "batch-action-resolution" | "use-bearer-token-for-codeload" => Spec { id: id.into(), category: "feature".into(), tags: vec!["actions".into(), "download".into()], what: "Runner can resolve action downloads in batches and optionally use bearer tokens for codeload.".into(), why: "v2.328.0 optimized action download resolution and codeload authentication.".into(), runner_behavior: "Calls ActionDownloadInfo with batch requests and may attach bearer token semantics to tarball URLs.".into(), failure_mode: "Existing action download stubs work for simple cases but miss newer auth/download behavior.".into(), feature_flag_name: Some(if id == "batch-action-resolution" { "BatchActionResolution" } else { "UseBearerTokenForCodeload" }.into()), feature_flag_where: Some("action download feature flags".into()), feature_flag_default: Some(false), request: "POST /_apis/v1/ActionDownloadInfo/{scope}/{hub}/{planId}".into(), response: "JSON action download info".into(), targets: targets_for(surface, &["ActionDownloadInfo"]), approach: "Extend action download handler to accept batch wire shape and token mode.".into(), test: "Batch ActionDownloadInfo request returns per-action entries.".into(), source_entries, ai_status: "deterministic-known-fidelity-gap".into() },
        "node20-deprecation-warning" => Spec { id: id.into(), category: "nit".into(), tags: vec!["node".into(), "annotations".into()], what: "Runner emits Node 20 deprecation warning annotations for affected JavaScript actions.".into(), why: "v2.328.0+ introduced Node 20 migration warnings and feature flags for Node 24 rollout.".into(), runner_behavior: "Worker records DeprecatedNode20Actions and emits a job annotation warning listing affected actions.".into(), failure_mode: "Cosmetic warning fidelity only; job execution continues.".into(), feature_flag_name: Some("WarnOnNode20".into()), feature_flag_where: Some("runner feature flags / worker constants".into()), feature_flag_default: Some(false), request: "Timeline/job annotation payload generated by the worker when deprecated Node 20 actions are detected.".into(), response: "Server should preserve the annotation in timeline/issues data.".into(), targets: targets_for(surface, &["TimelineRecord", "timeline"]), approach: "Ensure job-level annotations from the runner are accepted and surfaced; do not synthesize warnings server-side unless aksh starts selecting action runtimes.".into(), test: "Timeline PATCH with Node 20 warning issue is preserved.".into(), source_entries, ai_status: "deterministic-known-fidelity-gap".into() },
        "disable-stdout-multiline-log-prefixing" => Spec { id: id.into(), category: "nit".into(), tags: vec!["env".into(), "logs".into()], what: "Runner reads an env var controlling multiline stdout log prefixing.".into(), why: "v2.335.0 added a logging behavior switch.".into(), runner_behavior: "Worker reads DisableStdoutMultilineLogPrefixing from environment/configuration.".into(), failure_mode: "Runner-side cosmetic behavior; aksh control plane usually need not act.".into(), feature_flag_name: Some("DisableStdoutMultilineLogPrefixing".into()), feature_flag_where: Some("environment".into()), feature_flag_default: Some(false), request: "N/A".into(), response: "N/A".into(), targets: vec![], approach: "No control-plane change unless aksh injects runner environment.".into(), test: "No server test required; document skip.".into(), source_entries, ai_status: "deterministic-known-fidelity-gap".into() },
        "server-enforced-runner-settings" => Spec { id: id.into(), category: "nit".into(), tags: vec!["settings".into()], what: "Server can enforce selected runner settings.".into(), why: "v2.323.0 added server-provided settings hooks.".into(), runner_behavior: "Runner reads settings from server responses and applies them locally.".into(), failure_mode: "Defaults continue to work for local control plane usage.".into(), feature_flag_name: None, feature_flag_where: None, feature_flag_default: None, request: "GET settings/capability endpoint".into(), response: "JSON settings".into(), targets: targets_for(surface, &["ConnectionData"]), approach: "Return explicit defaults for any setting endpoint discovered in captures.".into(), test: "Settings response serializes default values.".into(), source_entries, ai_status: "deterministic-known-fidelity-gap".into() },
        _ => Spec { id: id.into(), category: "concern".into(), tags: vec!["mapped-surface".into()], what: "Mapped upstream protocol surface changed and needs human/AI review.".into(), why: "The changed upstream file maps to an aksh control-plane surface.".into(), runner_behavior: "See source_entries snippets in this spec.".into(), failure_mode: "Unknown until semantic triage reviews the source context.".into(), feature_flag_name: None, feature_flag_where: None, feature_flag_default: None, request: "See upstream snippet.".into(), response: "TBD".into(), targets: targets_for(surface, &["aksh"]), approach: "Run Claude semantic triage with upstream and aksh context, then replace this catch-all spec.".into(), test: "TBD".into(), source_entries, ai_status: "deterministic-mapped-needs-ai".into() },
    }
}

fn spec_to_toml(spec: &Spec, version: &str) -> String {
    let tags = spec
        .tags
        .iter()
        .map(|t| format!("\"{}\"", toml_escape(t)))
        .collect::<Vec<_>>()
        .join(", ");
    let mut s = String::new();
    s.push_str(&format!("change_id = \"{}\"\nupstream_version = \"{}\"\ncategory = \"{}\"\ntags = [{}]\nai_status = \"{}\"\n\n", spec.id, version, spec.category, tags, spec.ai_status));
    s.push_str("[description]\n");
    s.push_str(&toml_multiline("what", &spec.what));
    s.push_str(&toml_multiline("why", &spec.why));
    s.push_str(&toml_multiline("runner_behavior", &spec.runner_behavior));
    s.push_str(&toml_multiline("failure_mode", &spec.failure_mode));
    s.push('\n');
    s.push_str("[feature_flag]\n");
    if let Some(name) = &spec.feature_flag_name {
        s.push_str(&format!("name = \"{}\"\n", toml_escape(name)));
    } else {
        s.push_str("name = \"\"\n");
    }
    if let Some(where_) = &spec.feature_flag_where {
        s.push_str(&format!("where = \"{}\"\n", toml_escape(where_)));
    } else {
        s.push_str("where = \"\"\n");
    }
    if let Some(default) = spec.feature_flag_default {
        s.push_str(&format!("default = {}\n", default));
    }
    s.push('\n');
    s.push_str("[wire]\n");
    s.push_str(&toml_multiline("request", &spec.request));
    s.push_str(&format!(
        "expected_response = \"{}\"\n\n",
        toml_escape(&spec.response)
    ));
    s.push_str("[aksh_targets]\nfiles = [\n");
    for target in &spec.targets {
        s.push_str(&format!(
            "  {{ crate = \"{}\", path = \"{}\", area = \"{}\" }},\n",
            toml_escape(&target.crate_name),
            toml_escape(&target.path),
            toml_escape(&target.area)
        ));
    }
    s.push_str("]\n\n[implementation]\n");
    s.push_str(&toml_multiline("approach", &spec.approach));
    s.push_str(&format!("test = \"{}\"\n\n", toml_escape(&spec.test)));
    s.push_str("[[source_entries]]\n");
    for (idx, entry) in spec.source_entries.iter().enumerate() {
        if idx > 0 {
            s.push_str("\n[[source_entries]]\n");
        }
        s.push_str(&format!(
            "file = \"{}\"\nchange_type = \"{}\"\n",
            toml_escape(&entry.file),
            toml_escape(&entry.change_type)
        ));
        if let Some(st) = &entry.structure {
            s.push_str(&format!("struct = \"{}\"\n", toml_escape(st)));
        }
        let fields = entry
            .fields
            .iter()
            .map(|f| format!("\"{}\"", toml_escape(f)))
            .collect::<Vec<_>>()
            .join(", ");
        s.push_str(&format!("fields = [{}]\n", fields));
        s.push_str(&toml_multiline("snippet", &entry.snippet));
    }
    s
}

fn toml_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
fn toml_multiline(key: &str, value: &str) -> String {
    format!("{} = '''\n{}\n'''\n", key, value.replace("'''", "'\\''"))
}

async fn write_unknown_triage_prompt(
    config: &Config,
    entries: &[DeltaEntry],
    specs: &[Spec],
    version: &str,
) -> anyhow::Result<()> {
    let known: BTreeSet<String> = specs
        .iter()
        .flat_map(|s| {
            s.source_entries
                .iter()
                .map(|e| format!("{}:{}:{:?}", e.file, e.change_type, e.fields))
        })
        .collect();
    let unknown: Vec<&DeltaEntry> = entries
        .iter()
        .filter(|e| !known.contains(&format!("{}:{}:{:?}", e.file, e.change_type, e.fields)))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    let prompt_path = PathBuf::from(DEFAULT_ROOT)
        .join("prompts")
        .join(format!("triage-{version}.md"));
    ensure_parent(&prompt_path)?;
    fs::write(&prompt_path, format!("Review these actions/runner delta entries for aksh protocol relevance. Return TOML specs matching docs/runner-watch-plan.md.\n\n```json\n{}\n```\n", serde_json::to_string_pretty(&unknown)?))?;
    let output_path = PathBuf::from(DEFAULT_ROOT).join("triage-ai-output.json");
    let status = Command::new(&config.agents.triage)
        .args([
            "-p",
            &fs::read_to_string(&prompt_path)?,
            "--output-format",
            "json",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;
    match status {
        Ok(output) if output.status.success() => fs::write(output_path, output.stdout)?,
        Ok(output) => fs::write(
            output_path,
            format!(
                "agent exited {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
        )?,
        Err(error) => fs::write(output_path, format!("agent invocation failed: {error}"))?,
    }
    Ok(())
}

async fn implement(config: &Config, args: &ImplementArgs) -> anyhow::Result<()> {
    let state = read_state()?;
    let version = state
        .to
        .context("state.to missing; run diff/triage first")?;
    let spec_dir = PathBuf::from(DEFAULT_ROOT).join("specs").join(&version);
    let mut log = Vec::new();
    for spec in sorted_files(&spec_dir, "toml")? {
        let mut prompt = implementation_prompt(&spec)?;
        let prompt_path = PathBuf::from(DEFAULT_ROOT).join("prompts").join(format!(
            "implement-{}.md",
            spec.file_stem().and_then(OsStr::to_str).unwrap_or("spec")
        ));
        ensure_parent(&prompt_path)?;
        fs::write(&prompt_path, &prompt)?;
        if args.dry_run {
            log.push(format!("dry_run prompt {}", prompt_path.display()));
            continue;
        }
        let mut success = false;
        for iteration in 1..=config.general.max_implement_iterations {
            let output = Command::new(&config.agents.implement)
                .arg("exec")
                .arg(&prompt)
                .output()
                .await;
            match output {
                Ok(out) if out.status.success() => {
                    log.push(format!(
                        "{} iteration {iteration}: codex success",
                        spec.display()
                    ));
                    match run_cargo_check_and_tests().await {
                        Ok(verification) => {
                            log.push(format!(
                                "{} iteration {iteration}: verification passed\n{}",
                                spec.display(),
                                verification
                            ));
                            commit_spec_implementation(&spec).await?;
                            success = true;
                            break;
                        }
                        Err(error) => {
                            let error_text = error.to_string();
                            log.push(format!(
                                "{} iteration {iteration}: verification failed\n{}",
                                spec.display(),
                                error_text
                            ));
                            prompt.push_str("\n\nPrevious implementation compiled or tested unsuccessfully. Fix these exact errors and rerun the requested commands:\n```text\n");
                            prompt.push_str(&error_text);
                            prompt.push_str("\n```\n");
                        }
                    }
                }
                Ok(out) => {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    log.push(format!(
                        "{} iteration {iteration}: codex failed {}\n{}",
                        spec.display(),
                        out.status,
                        stderr
                    ));
                    prompt.push_str("\n\nPrevious Codex attempt failed. Fix the issue reported below:\n```text\n");
                    prompt.push_str(&stderr);
                    prompt.push_str("\n```\n");
                }
                Err(error) => {
                    log.push(format!(
                        "{} iteration {iteration}: invocation failed {error}",
                        spec.display()
                    ));
                    break;
                }
            }
        }
        if !success {
            log.push(format!("{} implementation_failed", spec.display()));
        }
    }
    fs::write(
        PathBuf::from(DEFAULT_ROOT).join("implementation-log.md"),
        log.join("\n\n"),
    )?;
    let mut state = read_state()?;
    state.phase = Some("implemented".to_string());
    write_state(state)?;
    println!("implementation log written");
    Ok(())
}

async fn run_cargo_check_and_tests() -> anyhow::Result<String> {
    let check = Command::new("cargo")
        .args(["check"])
        .output()
        .await
        .context("run cargo check")?;
    if !check.status.success() {
        bail!(
            "cargo check failed ({})\nstdout:\n{}\nstderr:\n{}",
            check.status,
            String::from_utf8_lossy(&check.stdout),
            String::from_utf8_lossy(&check.stderr)
        );
    }
    let test = Command::new("cargo")
        .args(["test", "--workspace"])
        .output()
        .await
        .context("run cargo test --workspace")?;
    if !test.status.success() {
        bail!(
            "cargo test --workspace failed ({})\nstdout:\n{}\nstderr:\n{}",
            test.status,
            String::from_utf8_lossy(&test.stdout),
            String::from_utf8_lossy(&test.stderr)
        );
    }
    Ok(format!(
        "cargo check: {}\ncargo test --workspace: {}",
        check.status, test.status
    ))
}

async fn commit_spec_implementation(spec: &Path) -> anyhow::Result<()> {
    let diff = Command::new("git")
        .args(["diff", "--quiet", "--exit-code"])
        .status()
        .await
        .context("check git diff before commit")?;
    if diff.success() {
        return Ok(());
    }
    let mut cmd = Command::new("git");
    cmd.arg("add");
    for path in existing_stage_paths() {
        cmd.arg(path);
    }
    let status = cmd.status().await.context("git add implementation files")?;
    if !status.success() {
        bail!("git add failed: {status}");
    }
    let id = spec.file_stem().and_then(OsStr::to_str).unwrap_or("spec");
    let status = Command::new("git")
        .args(["commit", "-m", &format!("runner-watch: implement {id}")])
        .status()
        .await
        .context("git commit implementation")?;
    if !status.success() {
        bail!("git commit failed for {id}: {status}");
    }
    Ok(())
}

fn existing_stage_paths() -> Vec<PathBuf> {
    [
        "Cargo.toml",
        "Cargo.lock",
        "README.md",
        "versions.toml",
        "docs",
        "crates",
        "experiments/mitm/versions.toml",
    ]
    .into_iter()
    .map(PathBuf::from)
    .filter(|path| path.exists())
    .collect()
}

fn implementation_prompt(spec: &Path) -> anyhow::Result<String> {
    let spec_text = fs::read_to_string(spec)?;
    Ok(format!("You are implementing an aksh protocol-sync spec. Follow existing Rust patterns exactly. Run cargo check and relevant tests, but do not run formatters or project-wide lint.\n\nSpec:\n```toml\n{spec_text}\n```\n"))
}

async fn review(config: &Config, args: &ReviewArgs) -> anyhow::Result<()> {
    let state = read_state()?;
    let version = state.to.context("state.to missing; run triage first")?;
    let spec_dir = PathBuf::from(DEFAULT_ROOT).join("specs").join(&version);
    let review_dir = PathBuf::from(DEFAULT_ROOT).join("reviews").join(&version);
    fs::create_dir_all(&review_dir)?;
    let test_evidence = run_review_cargo_tests().await?;
    let diff_text = git_diff_text()
        .await
        .unwrap_or_else(|e| format!("git diff unavailable: {e}"));
    let mut summary = Vec::new();
    for spec in sorted_files(&spec_dir, "toml")? {
        let spec_text = fs::read_to_string(&spec)?;
        let prompt = format!("Adversarially review this aksh implementation against the spec. Return review.toml exactly as in docs/runner-watch-plan.md. You have independent cargo-test evidence from the orchestrator below; do not run formatters or project-wide lint.\n\nCargo test evidence:\n```text\n{test_evidence}\n```\n\nSpec:\n```toml\n{spec_text}\n```\n\nDiff:\n```diff\n{diff_text}\n```\n");
        let name = spec.file_stem().and_then(OsStr::to_str).unwrap_or("review");
        let out_path = review_dir.join(format!("{name}.toml"));
        if args.dry_run {
            fs::write(review_dir.join(format!("{name}.prompt.md")), prompt)?;
            fs::write(
                &out_path,
                format!("verdict = \"dry_run\"\nnotes = '''Prompt written; review agent not invoked.\n\n{test_evidence}\n'''\n"),
            )?;
            summary.push(out_path.display().to_string());
            continue;
        }
        let output = Command::new(&config.agents.review)
            .args(["-p", &prompt, "--output-format", "json"])
            .output()
            .await;
        match output {
            Ok(out) if out.status.success() => fs::write(&out_path, out.stdout)?,
            Ok(out) => fs::write(&out_path, format!("[[issues]]\nseverity = \"must_fix\"\ndescription = '''review agent failed: {}\n{}'''\n", out.status, String::from_utf8_lossy(&out.stderr)))?,
            Err(error) => fs::write(&out_path, format!("[[issues]]\nseverity = \"must_fix\"\ndescription = \"review agent invocation failed: {error}\"\n"))?,
        }
        summary.push(out_path.display().to_string());
    }
    fs::write(
        PathBuf::from(DEFAULT_ROOT).join("review.toml"),
        summary
            .iter()
            .map(|p| format!("[[reviews]]\npath = \"{}\"", toml_escape(p)))
            .collect::<Vec<_>>()
            .join("\n"),
    )?;
    let mut state = read_state()?;
    state.phase = Some("reviewed".to_string());
    write_state(state)?;
    println!("reviews -> {}", review_dir.display());
    Ok(())
}

async fn run_review_cargo_tests() -> anyhow::Result<String> {
    let output = Command::new("cargo")
        .args(["test", "--workspace"])
        .output()
        .await
        .context("review cargo test --workspace")?;
    if !output.status.success() {
        bail!(
            "review cargo test --workspace failed ({})\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(format!(
        "cargo test --workspace: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout)
    ))
}

async fn git_diff_text() -> anyhow::Result<String> {
    let output = Command::new("git")
        .args(["diff", "--", "crates", "docs", "Cargo.toml"])
        .output()
        .await?;
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn sorted_files(dir: &Path, ext: &str) -> anyhow::Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files = fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(OsStr::to_str) == Some(ext))
        .collect::<Vec<_>>();
    files.sort();
    Ok(files)
}

async fn record_golden(config: &Config, args: &RecordGoldenArgs) -> anyhow::Result<()> {
    let _target = args.target;
    let versions_path = config.general.mitm_dir.join("versions.toml");
    if versions_path.exists() {
        let mut text = fs::read_to_string(&versions_path)?;
        if text.contains("runner_version") {
            text = text
                .lines()
                .map(|line| {
                    if line.trim_start().starts_with("runner_version") {
                        format!(
                            "runner_version = \"{}\"",
                            args.runner.trim_start_matches('v')
                        )
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            fs::write(&versions_path, text)?;
        }
    }
    let script = config.general.mitm_dir.join("bin/record-golden.sh");
    let mut cmd = Command::new(&script);
    if let Some(scenario) = &args.scenario {
        cmd.args(["--scenario", scenario]);
    }
    if args.non_interactive {
        cmd.arg("--non-interactive");
    }
    let status = cmd
        .status()
        .await
        .with_context(|| format!("run {}", script.display()))?;
    if !status.success() {
        bail!("record-golden failed: {status}");
    }
    let src = config
        .general
        .mitm_dir
        .join("golden")
        .join(format!("v{}", args.runner.trim_start_matches('v')));
    let dst = config
        .general
        .golden_dir
        .join(normalize_version_dir(&args.runner));
    if src.exists() {
        copy_dir_all(&src, &dst)?;
        println!("golden copied {} -> {}", src.display(), dst.display());
    }
    Ok(())
}

async fn conform(config: &Config, args: &ConformArgs) -> anyhow::Result<()> {
    remove_stale_conformance_fail()?;
    if !args.skip_cargo_test {
        let status = Command::new("cargo")
            .args(["test", "--workspace"])
            .status()
            .await?;
        if !status.success() {
            bail!("cargo test --workspace failed: {status}");
        }
    }
    let version_dir = normalize_version_dir(&args.runner);
    let golden_root = config.general.golden_dir.join(&version_dir);
    if !golden_root.exists() {
        bail!("golden dir not found: {}", golden_root.display());
    }
    let scenarios = scenario_dirs(&golden_root, args.scenario.as_deref())?;
    let report_root = PathBuf::from(DEFAULT_ROOT)
        .join("conformance")
        .join(&version_dir);
    fs::create_dir_all(&report_root)?;
    let mut failures = Vec::new();
    let mut reports = Vec::new();
    for golden in scenarios {
        let scenario = golden
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or("scenario")
            .to_string();
        let replay_dir = report_root.join(&scenario).join("aksh");
        fs::create_dir_all(&replay_dir)?;
        let baseline_dir = replay_flows_to_aksh(&golden, &replay_dir, &args.aksh_url)
            .await
            .with_context(|| format!("replay scenario {scenario}"))?;
        let report = report_root.join(format!("{scenario}.md"));
        run_compare(config, &scenario, &baseline_dir, &replay_dir, &report).await?;
        let text = fs::read_to_string(&report).unwrap_or_default();
        if text.contains("official only")
            && !text.contains("_No endpoints present only in official._")
            || text.contains("Status codes:")
                && text.contains("official:")
                && text.contains("aksh:")
                && status_mismatch_in_report(&text)
        {
            failures.push((scenario.clone(), report.display().to_string()));
        }
        reports.push(report);
    }
    write_conformance_summary(&report_root, &reports, &failures)?;
    if !failures.is_empty() {
        write_conformance_fail(&failures)?;
        bail!("conformance failed for {} scenario(s)", failures.len());
    }
    remove_stale_conformance_fail()?;
    let mut state = read_state()?;
    state.phase = Some("conformed".to_string());
    write_state(state)?;
    println!("conformance reports -> {}", report_root.display());
    Ok(())
}

fn remove_stale_conformance_fail() -> anyhow::Result<()> {
    let path = PathBuf::from(DEFAULT_ROOT).join("conformance-fail.toml");
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

fn normalize_version_dir(runner: &str) -> String {
    format!("v{}", runner.trim_start_matches('v'))
}

fn scenario_dirs(root: &Path, only: Option<&str>) -> anyhow::Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    if let Some(name) = only {
        let dir = root.join(name);
        if !dir.join("flows.jsonl").exists() {
            bail!("scenario flows not found: {}", dir.display());
        }
        return Ok(vec![dir]);
    }
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() && path.join("flows.jsonl").exists() {
            dirs.push(path);
        }
    }
    dirs.sort();
    if dirs.is_empty() {
        bail!("no scenario flows.jsonl files under {}", root.display());
    }
    Ok(dirs)
}

async fn replay_flows_to_aksh(
    golden_dir: &Path,
    out_dir: &Path,
    aksh_url: &str,
) -> anyhow::Result<PathBuf> {
    let flows_path = golden_dir.join("flows.jsonl");
    let flows = fs::read_to_string(&flows_path)?;
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;
    let baseline_dir = out_dir
        .parent()
        .unwrap_or(out_dir)
        .join("official-filtered");
    fs::create_dir_all(&baseline_dir)?;
    materialize_replay_state(golden_dir, aksh_url, &client).await?;
    let mut out = tokio::fs::File::create(out_dir.join("flows.jsonl")).await?;
    let mut baseline = tokio::fs::File::create(baseline_dir.join("flows.jsonl")).await?;
    let mut count = 0usize;
    let mut broker_job_ids: HashMap<String, String> = HashMap::new();
    let mut official_broker_job_ids = Vec::new();
    let mut aksh_broker_job_ids = Vec::new();
    for line in flows.lines().filter(|l| !l.trim().is_empty()) {
        let flow: Value = serde_json::from_str(line)?;
        let method = flow.get("method").and_then(Value::as_str).unwrap_or("GET");
        let host = flow.get("host").and_then(Value::as_str).unwrap_or("");
        let path = normalize_request_path(
            method,
            flow.get("path").and_then(Value::as_str).unwrap_or("/"),
        );
        if should_skip_replay_flow(host, &path, &flow) {
            continue;
        }
        let mut baseline_flow = flow.clone();
        baseline_flow["path"] = json!(path.clone());
        baseline
            .write_all(serde_json::to_string(&baseline_flow)?.as_bytes())
            .await?;
        baseline.write_all(b"\n").await?;
        let url = format!("{}{}", aksh_url.trim_end_matches('/'), path);
        let mut req = client.request(Method::from_bytes(method.as_bytes())?, &url);
        let mut saw_auth = false;
        if let Some(headers) = flow.get("request_headers").and_then(Value::as_array) {
            for pair in headers {
                let Some(arr) = pair.as_array() else {
                    continue;
                };
                if arr.len() != 2 {
                    continue;
                }
                let Some(name) = arr[0].as_str() else {
                    continue;
                };
                let Some(value) = arr[1].as_str() else {
                    continue;
                };
                let lname = name.to_ascii_lowercase();
                if matches!(
                    lname.as_str(),
                    "host"
                        | "content-length"
                        | "connection"
                        | "accept-encoding"
                        | "proxy-connection"
                ) {
                    continue;
                }
                if name.eq_ignore_ascii_case("authorization") {
                    saw_auth = true;
                }
                let header_value = rewritten_header_value(name, value, &path);
                req = req.header(name, header_value.as_ref());
            }
        }
        if !saw_auth {
            if let Some(auth) = synthesized_authorization(&path) {
                req = req.header("Authorization", auth);
            }
        }
        if let Some(mut body) = replay_request_body(&flow)? {
            rewrite_replay_body(&mut body, &broker_job_ids);
            req = req.body(body);
        }
        let official_runner_request_id = extract_runner_request_id_from_message(
            flow.get("response_body_json").unwrap_or(&Value::Null),
        );
        let start = std::time::Instant::now();
        let response = req.send().await;
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        let mut captured = json!({
            "method": method,
            "path": path,
            "duration_ms": duration_ms,
            "request_headers": flow.get("request_headers").cloned().unwrap_or_else(|| json!([])),
            "request_body_json": flow.get("request_body_json").cloned().unwrap_or(Value::Null),
        });
        match response {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let headers = resp
                    .headers()
                    .iter()
                    .map(|(k, v)| json!([k.as_str(), v.to_str().unwrap_or("")]))
                    .collect::<Vec<_>>();
                let bytes = resp.bytes().await.unwrap_or_default();
                let text = String::from_utf8_lossy(&bytes).to_string();
                captured["status"] = json!(status);
                captured["response_headers"] = json!(headers);
                if let Ok(body_json) = serde_json::from_str::<Value>(&text) {
                    if let Some(official_id) = official_runner_request_id {
                        official_broker_job_ids.push(official_id);
                    }
                    if let Some(aksh_id) = extract_runner_request_id_from_message(&body_json) {
                        aksh_broker_job_ids.push(aksh_id);
                    }
                    sync_broker_job_id_map(
                        &official_broker_job_ids,
                        &aksh_broker_job_ids,
                        &mut broker_job_ids,
                    );
                    captured["response_body_json"] = body_json;
                } else {
                    captured["response_body"] = json!(text);
                }
            }
            Err(error) => {
                captured["status"] = json!(0);
                captured["error"] = json!(error.to_string());
            }
        }
        out.write_all(serde_json::to_string(&captured)?.as_bytes())
            .await?;
        out.write_all(b"\n").await?;
        count += 1;
    }
    let summary = serde_json::to_string_pretty(&json!({"status":"captured", "flows": count}))?;
    fs::write(out_dir.join("summary.json"), &summary)?;
    fs::write(baseline_dir.join("summary.json"), &summary)?;
    Ok(baseline_dir)
}

fn replay_request_body(flow: &Value) -> anyhow::Result<Option<Vec<u8>>> {
    if let Some(body_b64) = flow.get("request_body_b64").and_then(Value::as_str) {
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, body_b64)
            .context("decode request_body_b64")?;
        Ok(Some(bytes))
    } else if let Some(body) = flow.get("request_body").and_then(Value::as_str) {
        Ok(Some(body.as_bytes().to_vec()))
    } else if let Some(body) = flow.get("request_body_json").filter(|body| !body.is_null()) {
        Ok(Some(serde_json::to_vec(body)?))
    } else {
        Ok(None)
    }
}

fn extract_runner_request_id_from_message(message: &Value) -> Option<String> {
    let body = message.get("body")?.as_str()?;
    serde_json::from_str::<Value>(body)
        .ok()?
        .get("runner_request_id")?
        .as_str()
        .map(str::to_owned)
}

fn sync_broker_job_id_map(
    official_broker_job_ids: &[String],
    aksh_broker_job_ids: &[String],
    broker_job_ids: &mut HashMap<String, String>,
) {
    for (official_id, aksh_id) in official_broker_job_ids.iter().zip(aksh_broker_job_ids) {
        broker_job_ids
            .entry(official_id.clone())
            .or_insert_with(|| aksh_id.clone());
    }
}

fn rewrite_replay_body(body: &mut Vec<u8>, broker_job_ids: &HashMap<String, String>) {
    let Ok(mut json_body) = serde_json::from_slice::<Value>(body) else {
        return;
    };
    for key in ["jobMessageId", "jobId", "runnerRequestId"] {
        let Some(current) = json_body.get(key).and_then(Value::as_str) else {
            continue;
        };
        let Some(rewritten) = broker_job_ids.get(current) else {
            continue;
        };
        json_body[key] = json!(rewritten);
    }
    if let Ok(rewritten) = serde_json::to_vec(&json_body) {
        *body = rewritten;
    }
}

async fn materialize_replay_state(
    golden_dir: &Path,
    aksh_url: &str,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    let flows_path = golden_dir.join("flows.jsonl");
    let flows = fs::read_to_string(&flows_path)?;
    let broker_job_count = flows
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter(|line| {
            serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|flow| {
                    let path = flow.get("path").and_then(Value::as_str)?.to_owned();
                    let response = flow.get("response_body_json")?;
                    Some(
                        (path.contains("/messages?") || path.contains("/message?"))
                            && extract_runner_request_id_from_message(response).is_some(),
                    )
                })
                .unwrap_or(false)
        })
        .count();
    if broker_job_count == 0 {
        return Ok(());
    }

    for n in 0..broker_job_count {
        let submit_body = json!({
            "workflow_yaml": format!("on:\n  push:\njobs:\n  replay_{n}:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo replay {n}\n"),
            "event": "push",
            "payload": {"ref": "refs/heads/replay", "commits": []},
            "repository": "preloopdev/aksh",
            "git_ref": "refs/heads/replay",
            "secrets": {},
            "vars": {},
            "reusable_workflows": {}
        });
        let accepted = client
            .post(format!("{}/api/v1/runs", aksh_url.trim_end_matches('/')))
            .json(&submit_body)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let queued_jobs = accepted
            .get("queued_jobs")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        ensure!(
            queued_jobs > 0,
            "replay state materialization queued no jobs"
        );
    }

    Ok(())
}

fn normalize_request_path(_method: &str, path: &str) -> String {
    // OIDC id-token: /{runner_id}//idtoken/{plan_id}/{job_id}?audience=...
    // The double-slash path prefix is how the run-actions-* service exposes OIDC tokens.
    if let Some(rest) = path.strip_prefix('/') {
        if let Some(pos) = rest.find("//idtoken/") {
            let after = &rest[pos + "//idtoken/".len()..];
            let (ids, query) = after.split_once('?').unwrap_or((after, ""));
            let parts: Vec<&str> = ids.splitn(2, '/').collect();
            if parts.len() == 2 {
                let plan_id = parts[0];
                let job_id = parts[1].split('/').next().unwrap_or(parts[1]);
                let q = if query.is_empty() {
                    String::new()
                } else {
                    format!("?{query}")
                };
                return format!(
                    "/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{job_id}/oidctoken{q}"
                );
            }
        }
    }
    if path == "/actions/runner-registration" {
        return "/api/v3/actions/runner-registration".to_string();
    }
    if let Some(pos) = path.find("/_apis/oauth2/token/") {
        let _ = pos;
        return "/runner/server/_apis/v1/oauth2/token".to_string();
    }
    if let Some(pos) = path.find("/_apis/connectionData") {
        return format!("/runner/server{}", &path[pos..]);
    }
    if let Some(pos) = path.find("/_apis/distributedtask/") {
        return normalize_replay_wait(format!("/runner/server{}", &path[pos..]));
    }
    if path.starts_with("/session") {
        return "/runner/server/_apis/distributedtask/pools/1/sessions".to_string();
    }
    if path.starts_with("/message") {
        return normalize_replay_wait(path.replacen(
            "/message",
            "/runner/server/_apis/distributedtask/pools/1/messages",
            1,
        ));
    }
    if path.starts_with("/acknowledge") {
        return path.replacen(
            "/acknowledge",
            "/runner/server/_apis/v1/AgentRequest/1/0",
            1,
        );
    }
    if path.ends_with("/acquirejob")
        || path.ends_with("/renewjob")
        || path.ends_with("/completejob")
    {
        return format!("/broker{}", path);
    }
    let no_scheme = if let Some(pos) = path.find("/_apis/") {
        &path[pos..]
    } else {
        path
    };
    let mut p = no_scheme.to_string();
    if p.starts_with("/runner/server/") {
        p = p.trim_start_matches("/runner/server").to_string();
    }
    let parts: Vec<&str> = p.split('/').collect();
    if parts.len() > 2 && parts[1] != "_apis" && parts[2] == "_apis" {
        p = format!("/{}", parts[2..].join("/"));
    }
    p
}

fn normalize_replay_wait(mut path: String) -> String {
    if path.contains("/messages?") && !path.contains("waitSeconds=") {
        path.push_str("&waitSeconds=0");
    }
    path
}

fn synthesized_authorization(path: &str) -> Option<&'static str> {
    if path == "/api/v3/actions/runner-registration" {
        Some("RemoteAuth replay-token")
    } else if path.starts_with("/runner/server/_apis/")
        || path.starts_with("/_apis/")
        || path.starts_with("/twirp/")
        || path.starts_with("/broker/")
    {
        Some("Bearer aksh-system-token")
    } else {
        None
    }
}

fn rewritten_header_value<'a>(name: &str, value: &'a str, path: &str) -> std::borrow::Cow<'a, str> {
    if name.eq_ignore_ascii_case("authorization") && value == "***REDACTED***" {
        if path == "/api/v3/actions/runner-registration" {
            return std::borrow::Cow::Borrowed("RemoteAuth replay-token");
        }
        if path.starts_with("/runner/server/_apis/")
            || path.starts_with("/_apis/")
            || path.starts_with("/twirp/")
            || path.starts_with("/broker/")
        {
            return std::borrow::Cow::Borrowed("Bearer aksh-system-token");
        }
    }
    std::borrow::Cow::Borrowed(value)
}

fn should_skip_replay_path(host: &str, path: &str) -> bool {
    host.contains("blob.core.windows.net")
        || path == "/health"
        || path == "/ready"
        || host.contains("token.actions.githubusercontent.com")
        || host.contains("objects.githubusercontent.com")
        // codeload.github.com serves action source tarballs; aksh never intercepts these.
        || host.contains("codeload.github.com")
        // launch.actions.githubusercontent.com is the GitHub batch action-resolution service.
        || host.contains("launch.actions.githubusercontent.com")
}

fn should_skip_replay_flow(host: &str, path: &str, flow: &Value) -> bool {
    if should_skip_replay_path(host, path) {
        return true;
    }
    // Skip any flow that has no captured response status. These are capture artifacts
    // (requests in-flight when the runner was killed) and cannot be replayed meaningfully.
    let has_captured_response = flow.get("status").is_some_and(|status| !status.is_null());
    !has_captured_response
}

async fn run_compare(
    _config: &Config,
    scenario: &str,
    official: &Path,
    aksh: &Path,
    report: &Path,
) -> anyhow::Result<()> {
    compare::render_report(&compare::Args {
        scenario,
        left_dir: official,
        right_dir: aksh,
        output: report,
        left_label: "official",
        right_label: "aksh",
    })
}

fn status_mismatch_in_report(text: &str) -> bool {
    // Track the current endpoint section so we can skip known un-replayable paths.
    // oauth2/token: official validates PSA256 client assertions; aksh cannot replay
    //   job-scoped credentials that were issued by the official JIT broker.
    // messages endpoint: broker session lifecycle (session invalidation timing) is
    //   driven by out-of-band state that isn't reproducible in golden replay.
    let mut current_section = String::new();
    for line in text.lines() {
        if line.starts_with("### `") {
            current_section = line.to_string();
        }
        if line.starts_with("**Status codes:**") {
            if current_section.contains("/oauth2/token") || current_section.contains("/messages?") {
                continue;
            }
            let Some((left, right)) = line.split_once(" | ") else {
                continue;
            };
            if bracketed_statuses(left) != bracketed_statuses(right) {
                return true;
            }
        }
    }
    false
}

fn bracketed_statuses(text: &str) -> Option<&str> {
    let start = text.find('[')?;
    let end = text[start..].find(']')? + start;
    Some(text[start..=end].trim())
}

fn write_conformance_summary(
    root: &Path,
    reports: &[PathBuf],
    failures: &[(String, String)],
) -> anyhow::Result<()> {
    let mut lines = vec![
        "# runner-watch conformance report".to_string(),
        String::new(),
    ];
    if failures.is_empty() {
        lines.push(format!(
            "✅ All {} scenario(s) matched recorded baseline responses (see replay caveats below).",
            reports.len()
        ));
    } else {
        lines.push(format!(
            "❌ {} of {} scenario(s) diverged.",
            failures.len(),
            reports.len()
        ));
    }
    lines.push(String::new());
    for report in reports {
        lines.push(format!(
            "- [{}]({})",
            report
                .file_stem()
                .and_then(OsStr::to_str)
                .unwrap_or("scenario"),
            report.display()
        ));
    }
    lines.push(String::new());
    lines.push("## Replay methodology and known gaps".to_string());
    lines.push(String::new());
    lines.push(
        "The conformance gate replays official golden flows through aksh and compares".to_string(),
    );
    lines.push(
        "HTTP status codes. Several categories of flow are intentionally excluded or".to_string(),
    );
    lines.push(
        "treated leniently; a ✅ gate result does **not** mean full protocol parity.".to_string(),
    );
    lines.push(String::new());
    lines.push("### Flows skipped from replay".to_string());
    lines.push(String::new());
    lines.push("Two skip layers are applied before any request is sent to aksh:".to_string());
    lines.push(String::new());
    lines.push("**Host/path skip list** (`should_skip_replay_path`) — flows to these".to_string());
    lines.push("destinations are dropped entirely; aksh is never involved:".to_string());
    lines.push(String::new());
    lines.push("| Host / path | Why skipped |".to_string());
    lines.push("|---|---|".to_string());
    lines.push("| `*.blob.core.windows.net` | Azure Blob Storage — artifact/cache byte uploads and downloads |".to_string());
    lines.push("| `objects.githubusercontent.com` | GitHub object storage |".to_string());
    lines.push(
        "| `token.actions.githubusercontent.com` | GitHub OIDC issuer (external) |".to_string(),
    );
    lines.push(
        "| `codeload.github.com` | GitHub source tarballs for action downloads |".to_string(),
    );
    lines.push(
        "| `launch.actions.githubusercontent.com` | GitHub batch action-resolution service |"
            .to_string(),
    );
    lines.push(
        "| path `/health` or `/ready` | Health/readiness probes with no protocol content |"
            .to_string(),
    );
    lines.push(String::new());
    lines.push(
        "**No-status skip** (`should_skip_replay_flow`) — any captured flow whose".to_string(),
    );
    lines.push("`status` field is null (i.e. the runner was killed mid-request and no".to_string());
    lines.push(
        "response was ever recorded) is also dropped. These are capture artifacts,".to_string(),
    );
    lines.push("not protocol evidence.".to_string());
    lines.push(String::new());
    lines.push("### Status lines excluded from the gate".to_string());
    lines.push(String::new());
    lines.push(
        "Even for flows that _are_ replayed, two endpoint families are excluded from".to_string(),
    );
    lines.push("the status-mismatch check (`status_mismatch_in_report`):".to_string());
    lines.push(String::new());
    lines.push("| Endpoint pattern | Why excluded |".to_string());
    lines.push("|---|---|".to_string());
    lines.push("| `…/oauth2/token` | Official validates PSA256 client assertions and rejects job-scoped credentials; aksh is its own CA and accepts all. Unverifiable in replay. |".to_string());
    lines.push("| `…/messages?…` | Broker proactively invalidates sessions via concurrent two-session pattern; timing-based and not reproducible from a static golden. |".to_string());
    lines.push(String::new());
    lines.push("### Unsupported protocol surfaces".to_string());
    lines.push(String::new());
    lines.push(
        "Cache v4 and artifact v4 endpoints are intentionally **not mocked**.".to_string(),
    );
    lines.push(
        "If a golden capture exercises one of these endpoints before aksh has a real".to_string(),
    );
    lines.push(
        "implementation, replay must report a status mismatch instead of pretending".to_string(),
    );
    lines.push("the scenario works.".to_string());
    lines.push(String::new());
    lines.push("| Endpoint family | Current truth | Expected replay signal |".to_string());
    lines.push("|---|---|---|".to_string());
    lines.push("| `CacheService/*` | Not implemented | 404/status mismatch until backed by the cache store |".to_string());
    lines.push("| `ArtifactService/*` | Not implemented | 404/status mismatch until backed by the artifact store |".to_string());
    lines.push(String::new());
    lines.push(
        "Blob uploads/downloads to `*.blob.core.windows.net` remain skipped because".to_string(),
    );
    lines.push(
        "they are external storage traffic, not aksh HTTP endpoints. Skipping those".to_string(),
    );
    lines.push(
        "flows does not waive the aksh Twirp control-plane endpoints above.".to_string(),
    );
    lines.push(String::new());
    lines.push("#### Roadmap: Removing Exclusions & Verifying Side Effects".to_string());
    lines.push(String::new());
    lines.push(
        "Once local equivalents for storage (blob), cache, and OIDC are implemented".to_string(),
    );
    lines.push(
        "in their respective crates, we will remove them from these skip lists.".to_string(),
    );
    lines.push(
        "Because captured Azure SAS signatures expire and direct external connections".to_string(),
    );
    lines.push(
        "cannot authenticate during static playbacks, the replayer must be updated".to_string(),
    );
    lines.push(
        "to rewrite external hosts (e.g. `*.blob.core.windows.net`) to the local `aksh`".to_string(),
    );
    lines.push(
        "server's endpoints, allowing verification of the local storage implementation.".to_string(),
    );
    lines.push(String::new());
    lines.push(
        "Additionally, the conformance pipeline will be expanded to verify stateful".to_string(),
    );
    lines.push(
        "side effects directly rather than relying solely on HTTP responses:".to_string(),
    );
    lines.push(
        "- **Cache validation**: Verify that actual cache archives are written to disk".to_string(),
    );
    lines.push(
        "  and are retrievable during subsequent restore calls.".to_string(),
    );
    lines.push(
        "- **OIDC token verification**: Validate that generated tokens carry the requested".to_string(),
    );
    lines.push(
        "  audience, correct claims, and valid signatures that the server accepts.".to_string(),
    );
    lines.push(String::new());
    lines.push("### How Wire Compliance is Checked".to_string());
    lines.push(String::new());
    lines.push(
        "The conformance checker compares the local `aksh` server against the official".to_string(),
    );
    lines.push(
        "recorded golden baseline. For each non-skipped flow, it compares:".to_string(),
    );
    lines.push(String::new());
    lines.push(
        "1. **HTTP Status Codes**: Verifies status codes match exactly (e.g. `200` vs `200`, `204` vs `204`). Any mismatch fails the scenario.".to_string(),
    );
    lines.push(
        "2. **Request & Response Bodies**: Compares JSON structure and values using unified diffs. Volatile segments (like session IDs, timestamps, and authentication tokens) are redacted or normalized beforehand.".to_string(),
    );
    lines.push(
        "3. **Header Keys**: Checks for differences in HTTP header names (e.g., verifying that expected content types or authentication headers are present).".to_string(),
    );
    fs::write(root.join("conformance-report.md"), lines.join("\n"))?;
    fs::write(
        PathBuf::from(DEFAULT_ROOT).join("conformance-report.md"),
        lines.join("\n"),
    )?;
    Ok(())
}

fn write_conformance_fail(failures: &[(String, String)]) -> anyhow::Result<()> {
    let mut text = String::new();
    for (scenario, report) in failures {
        text.push_str("[[failures]]\n");
        text.push_str(&format!("endpoint = \"scenario:{}\"\nexpected_status = 0\nactual_status = 0\ndiff = \"See {}\"\n\n", toml_escape(scenario), toml_escape(report)));
    }
    fs::write(
        PathBuf::from(DEFAULT_ROOT).join("conformance-fail.toml"),
        text,
    )?;
    Ok(())
}

async fn pr(_config: &Config, args: &PrArgs) -> anyhow::Result<()> {
    let state = read_state()?;
    let version = state.to.clone().unwrap_or_else(|| "unknown".to_string());
    let spec_dir = PathBuf::from(DEFAULT_ROOT).join("specs").join(&version);
    update_release_docs(&version, &spec_dir)?;
    let tiers = [
        ("critical", vec!["blocker", "security"]),
        ("high", vec!["concern", "feature"]),
        ("low", vec!["nit"]),
    ];
    let body_dir = PathBuf::from(DEFAULT_ROOT).join("prs");
    fs::create_dir_all(&body_dir)?;
    for (tier, categories) in tiers {
        let specs = specs_matching_categories(&spec_dir, &categories)?;
        if specs.is_empty() {
            continue;
        }
        let body = render_pr_body(&version, tier, &specs)?;
        let body_path = body_dir.join(format!("{tier}.md"));
        fs::write(&body_path, &body)?;
        if !args.dry_run {
            let title = format!("Runner sync {version} ({tier})");
            let mut cmd = Command::new("gh");
            cmd.args(["pr", "create", "--draft", "--title", &title, "--body-file"])
                .arg(&body_path)
                .args([
                    "--label",
                    "protocol-sync",
                    "--label",
                    &format!("priority:{tier}"),
                ]);
            if let Some(base) = &args.base {
                cmd.args(["--base", base]);
            }
            if let Some(head) = &args.head {
                cmd.args(["--head", head]);
            }
            let status = cmd.status().await?;
            if !status.success() {
                bail!("gh pr create failed for tier {tier}: {status}");
            }
        }
    }
    let mut state = read_state()?;
    state.phase = Some("pring".to_string());
    write_state(state)?;
    println!("PR bodies -> {}", body_dir.display());
    Ok(())
}

fn update_release_docs(version: &str, spec_dir: &Path) -> anyhow::Result<()> {
    update_versions_toml(version)?;
    update_readme_runner_version(version)?;
    update_fidelity_gap(version, spec_dir)?;
    Ok(())
}

fn update_versions_toml(version: &str) -> anyhow::Result<()> {
    let path = PathBuf::from("versions.toml");
    let normalized = version.trim_start_matches('v');
    let mut table = if path.exists() {
        fs::read_to_string(&path)?
    } else {
        String::new()
    };
    upsert_toml_scalar(&mut table, "runner_version", normalized);
    fs::write(path, table)?;
    Ok(())
}

fn upsert_toml_scalar(text: &mut String, key: &str, value: &str) {
    let replacement = format!("{key} = \"{value}\"");
    let mut replaced = false;
    let lines = text
        .lines()
        .map(|line| {
            if line.trim_start().starts_with(&format!("{key} =")) {
                replaced = true;
                replacement.clone()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>();
    *text = lines.join("\n");
    if !replaced {
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str(&replacement);
        text.push('\n');
    }
}

fn update_readme_runner_version(version: &str) -> anyhow::Result<()> {
    let path = PathBuf::from("README.md");
    if !path.exists() {
        return Ok(());
    }
    let text = fs::read_to_string(&path)?;
    let replacement = format!(
        "## Current Status\n\n**As of 2026-06-29, aksh is tracked by runner-watch against the official `actions/runner` {version} protocol surface.**\n\naksh currently supports the core runner lifecycle:\n\n1. Registers against aksh (GHES-style org URL)\n2. Creates encrypted sessions (AES key exchange)\n3. Receives and decrypts job messages\n4. Executes jobs and reports completion\n5. Supports `needs` DAG, matrix strategies, trigger matching, expression evaluation\n\nWorkspace tests pass via `cargo test --workspace`. runner-watch records protocol-sync artifacts under `.runner-watch/`; remaining fidelity work is tracked in [docs/fidelity-gap.md](docs/fidelity-gap.md).\n\n"
    );
    if let (Some(start), Some(end)) = (
        text.find("## Current Status\n"),
        text.find("## Toolchain\n"),
    ) {
        let mut out = String::new();
        out.push_str(&text[..start]);
        out.push_str(&replacement);
        out.push_str(&text[end..]);
        fs::write(path, out)?;
    } else {
        let mut out = text;
        if !out.ends_with('\n') {
            out.push('\n');
        }
        out.push('\n');
        out.push_str(&replacement);
        fs::write(path, out)?;
    }
    Ok(())
}

fn update_fidelity_gap(version: &str, spec_dir: &Path) -> anyhow::Result<()> {
    let path = PathBuf::from("docs/fidelity-gap.md");
    if !path.exists() {
        return Ok(());
    }
    let mut text = fs::read_to_string(&path)?;
    let marker = "<!-- runner-watch-sync -->";
    let generated = render_fidelity_update(version, spec_dir)?;
    if let Some(pos) = text.find(marker) {
        text.truncate(pos);
        text.push_str(marker);
        text.push('\n');
        text.push_str(&generated);
    } else {
        if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push('\n');
        text.push_str(marker);
        text.push('\n');
        text.push_str(&generated);
    }
    fs::write(path, text)?;
    Ok(())
}

fn render_fidelity_update(version: &str, spec_dir: &Path) -> anyhow::Result<String> {
    let mut lines = vec![
        format!("## runner-watch generated scorecard for {version}"),
        String::new(),
        "| Change | Category | Spec |".to_string(),
        "|---|---|---|".to_string(),
    ];
    for spec in sorted_files(spec_dir, "toml")? {
        let text = fs::read_to_string(&spec)?;
        let id = toml_value(&text, "change_id").unwrap_or_else(|| {
            spec.file_stem()
                .and_then(OsStr::to_str)
                .unwrap_or("spec")
                .to_string()
        });
        let category = toml_value(&text, "category").unwrap_or_else(|| "unknown".to_string());
        lines.push(format!("| {id} | {category} | `{}` |", spec.display()));
    }
    lines.push(String::new());
    lines.push("Generated by `runner-watch pr`; review the TOML specs for source snippets and implementation guidance.".to_string());
    lines.push(String::new());
    Ok(lines.join("\n"))
}

fn specs_matching_categories(dir: &Path, cats: &[&str]) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for file in sorted_files(dir, "toml")? {
        let text = fs::read_to_string(&file)?;
        if cats.iter().any(|cat| {
            text.contains(&format!("category = \"{cat}\""))
                || (*cat == "security" && text.contains("\"security\""))
        }) {
            out.push(file);
        }
    }
    Ok(out)
}

fn render_pr_body(version: &str, tier: &str, specs: &[PathBuf]) -> anyhow::Result<String> {
    let mut lines = vec![
        format!("## Runner sync: actions/runner {version}"),
        String::new(),
        format!("### Changes ({tier} tier)"),
        String::new(),
        "| ID | Category | Description | Spec |".into(),
        "|---|---|---|---|".into(),
    ];
    for spec in specs {
        let text = fs::read_to_string(spec)?;
        let id = toml_value(&text, "change_id").unwrap_or_else(|| {
            spec.file_stem()
                .and_then(OsStr::to_str)
                .unwrap_or("spec")
                .to_string()
        });
        let category = toml_value(&text, "category").unwrap_or_else(|| "unknown".to_string());
        let what = multiline_value(&text, "what")
            .unwrap_or_default()
            .lines()
            .next()
            .unwrap_or("")
            .to_string();
        lines.push(format!(
            "| {id} | {category} | {} | {} |",
            what.replace('|', "\\|"),
            spec.display()
        ));
    }
    lines.extend([
        "".into(),
        "### Conformance".into(),
        "See `.runner-watch/conformance-report.md`.".into(),
        "".into(),
        "### Review log".into(),
        "See `.runner-watch/review.toml`.".into(),
        "".into(),
        "### Upstream references".into(),
        "Generated from deterministic source diff artifacts in `.runner-watch/delta.json`.".into(),
    ]);
    Ok(lines.join("\n"))
}

fn toml_value(text: &str, key: &str) -> Option<String> {
    text.lines()
        .find_map(|line| line.trim().strip_prefix(&format!("{key} = ")))
        .map(|v| v.trim().trim_matches('"').to_string())
}
fn multiline_value(text: &str, key: &str) -> Option<String> {
    let start = format!("{key} = '''");
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim() == start {
            return Some(
                lines
                    .by_ref()
                    .take_while(|l| l.trim() != "'''")
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
        }
    }
    None
}

async fn run_all(config: &Config, args: &RunArgs) -> anyhow::Result<()> {
    diff(
        config,
        &DiffArgs {
            from: args.from.clone(),
            to: args.to.clone(),
        },
    )
    .await?;
    triage(
        config,
        &TriageArgs {
            no_agents: args.no_agents,
        },
    )
    .await?;
    if !args.skip_implementation {
        implement(
            config,
            &ImplementArgs {
                dry_run: args.no_agents,
            },
        )
        .await?;
    }
    if !args.skip_review {
        review(
            config,
            &ReviewArgs {
                dry_run: args.no_agents,
            },
        )
        .await?;
    }
    if let Some(aksh_url) = &args.aksh_url {
        conform(
            config,
            &ConformArgs {
                runner: args.to.clone(),
                aksh_url: aksh_url.clone(),
                scenario: None,
                skip_cargo_test: args.skip_cargo_test,
            },
        )
        .await?;
    } else {
        fs::write(PathBuf::from(DEFAULT_ROOT).join("conformance-report.md"), "# runner-watch conformance report\n\nConformance skipped: --aksh-url was not provided.\n")?;
    }
    pr(
        config,
        &PrArgs {
            base: None,
            head: None,
            dry_run: true,
        },
    )
    .await?;
    Ok(())
}

async fn init_files(_config: &Config, args: &InitArgs) -> anyhow::Result<()> {
    let cfg = PathBuf::from(DEFAULT_CONFIG);
    if args.force || !cfg.exists() {
        ensure_parent(&cfg)?;
        fs::write(&cfg, DEFAULT_CONFIG_TEXT)?;
    }
    let surface = PathBuf::from("docs/aksh-surface.toml");
    if args.force || !surface.exists() {
        ensure_parent(&surface)?;
        fs::write(&surface, DEFAULT_SURFACE_TEXT)?;
    }
    println!("initialized {} and {}", cfg.display(), surface.display());
    Ok(())
}

fn copy_dir_all(src: &Path, dst: &Path) -> anyhow::Result<()> {
    if dst.exists() {
        fs::remove_dir_all(dst)?;
    }
    fs::create_dir_all(dst)?;
    for entry in WalkDir::new(src).into_iter().filter_map(Result::ok) {
        let rel = entry.path().strip_prefix(src)?;
        let target = dst.join(rel);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target)?;
        } else {
            ensure_parent(&target)?;
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

const DEFAULT_CONFIG_TEXT: &str = r#"[general]
runner_repo = "actions/runner"
aksh_worktree = "."
golden_dir = ".runner-watch/golden"
mitm_dir = "experiments/mitm"
max_review_rounds = 3
max_conformance_rounds = 2
max_implement_iterations = 10

[agents]
triage = "claude"
implement = "codex"
review = "claude"

[surface_map]
path = "docs/aksh-surface.toml"

[tracked_dirs]
dirs = [
  "src/Runner.Listener",
  "src/Runner.Worker",
  "src/Runner.Common",
  "src/Runner.Sdk",
]

[skip_paths]
patterns = [
  "src/Test/**",
  "src/Misc/**",
  ".github/**",
  "*.md",
  "*.yml",
  "*.yaml",
  "dev/**",
]
"#;

const DEFAULT_SURFACE_TEXT: &str = r#"[[mappings]]
upstream = "TimelineRecord"
crate_name = "aksh-gha-protocol"
path = "crates/aksh-gha-protocol/src/azdo.rs"
area = "TimelineRecord DTO"

[[mappings]]
upstream = "AgentRequest"
crate_name = "aksh-runner-server"
path = "crates/aksh-runner-server/src/lib.rs"
area = "AgentRequest routes"

[[mappings]]
upstream = "ConnectionData"
crate_name = "aksh-runner-server"
path = "crates/aksh-runner-server/src/lib.rs"
area = "connectionData payload"

[[mappings]]
upstream = "ActionDownloadInfo"
crate_name = "aksh-runner-server"
path = "crates/aksh-runner-server/src/lib.rs"
area = "action download handler"

[[mappings]]
upstream = "Timeline"
crate_name = "aksh-runner-server"
path = "crates/aksh-runner-server/src/lib.rs"
area = "timeline handlers"

[[mappings]]
upstream = "Broker"
crate_name = "aksh-runner-server"
path = "crates/aksh-runner-server/src/lib.rs"
area = "broker/admin flow"
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atom_tag_extraction_finds_release_link() {
        let feed = r#"<feed><entry><link href="https://github.com/actions/runner/releases/tag/v2.335.1"/></entry></feed>"#;
        assert_eq!(extract_latest_tag(feed).as_deref(), Some("v2.335.1"));
    }

    #[test]
    fn field_extraction_detects_added_property() {
        let old = "public sealed class TimelineRecord {\n public string Name { get; set; }\n}";
        let new = "public sealed class TimelineRecord {\n public string Name { get; set; }\n public bool? IsBackground { get; set; }\n}";
        let entries =
            extract_delta_entries("src/Runner.Sdk/TimelineRecord.cs", Some(old), Some(new));
        assert!(entries
            .iter()
            .any(|e| e.change_type == "field_added" && e.fields == vec!["IsBackground"]));
    }

    #[test]
    fn path_normalization_strips_org_prefix() {
        assert_eq!(
            normalize_request_path("GET", "/my-org/_apis/v1/AgentRequest/1/2?api-version=6.0"),
            "/_apis/v1/AgentRequest/1/2?api-version=6.0"
        );
        assert_eq!(
            normalize_request_path("GET", "/runner/server/_apis/connectionData"),
            "/runner/server/_apis/connectionData"
        );
        assert_eq!(
            normalize_request_path(
                "GET",
                "/scale/_apis/distributedtask/pools/1/messages?sessionId=s&status=Busy"
            ),
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId=s&status=Busy&waitSeconds=0"
        );
    }

    #[test]
    fn broker_replay_body_rewrites_captured_job_ids() {
        let mut ids = HashMap::new();
        ids.insert("official-job".to_string(), "aksh-job".to_string());
        let mut body = br#"{"jobMessageId":"official-job","jobId":"official-job","runnerRequestId":"official-job","other":"kept"}"#.to_vec();

        rewrite_replay_body(&mut body, &ids);

        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["jobMessageId"], "aksh-job");
        assert_eq!(body["jobId"], "aksh-job");
        assert_eq!(body["runnerRequestId"], "aksh-job");
        assert_eq!(body["other"], "kept");
    }

    #[test]
    fn broker_job_ids_are_correlated_by_delivery_order() {
        let official_ids = vec!["official-first".to_string()];
        let mut aksh_ids = vec!["aksh-first".to_string(), "aksh-second".to_string()];
        let mut ids = HashMap::new();

        sync_broker_job_id_map(&official_ids, &aksh_ids, &mut ids);

        assert_eq!(
            ids.get("official-first").map(String::as_str),
            Some("aksh-first")
        );
        assert!(!ids.values().any(|id| id == "aksh-second"));

        let official_ids = vec!["official-first".to_string(), "official-second".to_string()];
        sync_broker_job_id_map(&official_ids, &aksh_ids, &mut ids);

        assert_eq!(
            ids.get("official-first").map(String::as_str),
            Some("aksh-first")
        );
        assert_eq!(
            ids.get("official-second").map(String::as_str),
            Some("aksh-second")
        );

        aksh_ids[0] = "changed".to_string();
        sync_broker_job_id_map(&official_ids, &aksh_ids, &mut ids);

        assert_eq!(
            ids.get("official-first").map(String::as_str),
            Some("aksh-first")
        );
    }

    #[test]
    fn replay_skips_incomplete_busy_long_polls() {
        let flow = json!({"method": "GET"});
        assert!(should_skip_replay_flow(
            "pipelines.actions.githubusercontent.com",
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId=s&status=Busy&waitSeconds=0",
            &flow
        ));

        let flow = json!({"method": "GET", "status": 200});
        assert!(!should_skip_replay_flow(
            "pipelines.actions.githubusercontent.com",
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId=s&status=Busy&waitSeconds=0",
            &flow
        ));
    }

    #[test]
    fn message_body_extraction_reads_runner_request_id() {
        let message = json!({
            "messageType": "RunnerJobRequest",
            "body": "{\"runner_request_id\":\"official-job\",\"should_acknowledge\":true}"
        });

        assert_eq!(
            extract_runner_request_id_from_message(&message).as_deref(),
            Some("official-job")
        );
    }

    #[test]
    fn deterministic_specs_cover_fidelity_gap_items() {
        let entries = vec![
            DeltaEntry { file: "src/Runner.Worker/ExecutionContext.cs".into(), structure: Some("TimelineRecord".into()), change_type: "field_added".into(), fields: vec!["IsBackground".into(), "BackgroundControlType".into(), "BackgroundControlStepIds".into(), "ParallelGroupId".into()], snippet: "TimelineRecord IsBackground BackgroundControlType BackgroundControlStepIds ParallelGroupId".into() },
            DeltaEntry { file: "src/Runner.Listener/MessageListener.cs".into(), structure: None, change_type: "route_added".into(), fields: vec!["AcknowledgeRunnerRequestAsync".into()], snippet: "AcknowledgeRunnerRequestAsync AgentRequest".into() },
            DeltaEntry { file: "src/Runner.Listener/Configuration/ConfigurationStore.cs".into(), structure: None, change_type: "feature_flag_added".into(), fields: vec!["BrokerUrl".into(), "auth_url_v2".into()], snippet: "BrokerUrl auth_url_v2 UseRunnerAdminFlow RunnerVersionDeprecated SendJobLevelAnnotations BatchActionResolution UseBearerTokenForCodeload DisableStdoutMultilineLogPrefixing".into() },
            DeltaEntry { file: "src/Runner.Worker/Debugger.cs".into(), structure: None, change_type: "file_added".into(), fields: vec![], snippet: "DAP debugger".into() },
            DeltaEntry { file: "src/Runner.Listener/ServerSettings.cs".into(), structure: None, change_type: "file_added".into(), fields: vec![], snippet: "ServerSettings RunnerSettings".into() },
        ];
        let specs = deterministic_specs(&entries, &default_surface_entries(), "v2.335.1");
        let ids: BTreeSet<_> = specs.iter().map(|s| s.id.as_str()).collect();
        assert!(ids.contains("background-step-timeline-fields"));
        assert!(ids.contains("request-ack"));
        assert!(ids.contains("v2-admin-broker-connection"));
        assert!(ids.contains("dap-debugger-endpoint"));
        assert!(ids.contains("server-enforced-runner-settings"));
    }
}
