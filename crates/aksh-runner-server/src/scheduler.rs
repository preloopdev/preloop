//! Cron-based schedule executor for `on: schedule:` workflows.
//!
//! Registration mirrors MessageController.cs:882-927:
//! - Scan workspace on server startup.
//! - Reconcile (add/remove) cron jobs on every push to the default branch.
//! - On fire, synthesize a schedule payload and call `submit_run_inner`.
//!
//! ## GitHub 5-field → cron crate 7-field conversion
//!
//! GitHub uses standard Unix 5-field cron (`min hour dom month dow`).
//! The `cron` crate expects a 7-field expression (`sec min hour dom month dow year`).
//! Conversion: prepend `0` (second=0) and append `*` (year=any).

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::task::AbortHandle;
use tracing::{info, warn};

use aksh_gha_parser::{parse_workflow, Trigger};
use aksh_gha_protocol::WorkflowSubmission;

use crate::{submit_run_inner, SharedState};

/// Adjust GitHub Day-Of-Week format to cron crate format.
///
/// Unix/GitHub cron DOW: 0..=7 (0=Sunday, 1=Monday, ..., 6=Saturday, 7=Sunday).
/// cron crate DOW: 1..=7 (1=Sunday, 2=Monday, ..., 7=Saturday).
fn adjust_dow(dow: &str) -> String {
    dow.split(',')
        .map(|part| {
            let subparts: Vec<&str> = part.split('/').collect();
            let left = subparts[0];
            let mapped_left = if left == "*" {
                "*".to_owned()
            } else {
                let range_parts: Vec<&str> = left.split('-').collect();
                range_parts
                    .iter()
                    .map(|&p| {
                        if let Ok(val) = p.parse::<u32>() {
                            match val {
                                0 | 7 => "1".to_owned(),
                                1..=6 => (val + 1).to_string(),
                                _ => p.to_owned(),
                            }
                        } else {
                            p.to_owned()
                        }
                    })
                    .collect::<Vec<String>>()
                    .join("-")
            };
            if subparts.len() > 1 {
                format!("{}/{}", mapped_left, subparts[1..].join("/"))
            } else {
                mapped_left
            }
        })
        .collect::<Vec<String>>()
        .join(",")
}

/// Convert a GitHub 5-field cron string to the cron-crate 7-field format.
///
/// GitHub: `"min hour dom month dow"`
///
/// Reference: MC.cs:898-915 (Quartz-style cron from GitHub 5-field).
/// We adjust the day-of-week indexing since the `cron` crate uses 1-based indexing for DOW.
/// Check whether a cron minute field resolves to fewer than 5-minute intervals.
/// Handles `*`, `*/N`, ranges (`0-4`), comma lists (`0,1,2`), and single values.
fn minute_field_too_frequent(minute: &str) -> bool {
    use std::collections::BTreeSet;
    let mut values = BTreeSet::new();
    for part in minute.split(',') {
        if part == "*" {
            return true;
        } else if let Some(step_str) = part.strip_prefix("*/") {
            if let Ok(step) = step_str.parse::<u8>() {
                if step < 5 {
                    return true;
                }
                for m in (0..60u8).step_by(step as usize) {
                    values.insert(m);
                }
            }
        } else if let Some((start, end)) = part.split_once('-') {
            if let (Ok(s), Ok(e)) = (start.parse::<u8>(), end.parse::<u8>()) {
                for m in s..=e {
                    values.insert(m);
                }
            }
        } else if let Ok(m) = part.parse::<u8>() {
            values.insert(m);
        }
    }
    if values.len() < 2 {
        return false; // single fixed value is always fine
    }
    let sorted: Vec<u8> = values.into_iter().collect();
    let min_gap = sorted.windows(2).map(|w| w[1] - w[0]).min().unwrap_or(60);
    min_gap < 5
}

pub fn github_to_cron(github_expr: &str) -> Result<Schedule, String> {
    github_to_crons(github_expr)?
        .into_iter()
        .next()
        .ok_or_else(|| format!("no cron schedules produced for {github_expr:?}"))
}

/// Convert one GitHub cron into one or two schedules. GitHub follows POSIX
/// DOM/DOW union semantics, while the cron crate intersects those fields. When
/// both are restricted, install each side independently and deduplicate the
/// coincident minute in `Scheduler::claim_fire`.
fn github_to_crons(github_expr: &str) -> Result<Vec<Schedule>, String> {
    let fields: Vec<&str> = github_expr.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(format!(
            "expected 5-field GitHub cron, got {} fields in {github_expr:?}",
            fields.len()
        ));
    }
    let (minute, hour, dom, month, dow) = (fields[0], fields[1], fields[2], fields[3], fields[4]);
    if minute_field_too_frequent(minute) {
        return Err("GitHub schedules cannot run more frequently than every 5 minutes".to_owned());
    }
    let adjusted_dow = adjust_dow(dow);
    let expressions = if dom != "*" && dow != "*" {
        vec![
            format!("0 {minute} {hour} {dom} {month} * *"),
            format!("0 {minute} {hour} * {month} {adjusted_dow} *"),
        ]
    } else {
        vec![format!("0 {minute} {hour} {dom} {month} {adjusted_dow} *")]
    };
    expressions
        .into_iter()
        .map(|expression| {
            expression.parse::<Schedule>().map_err(|error| {
                format!("cron parse error for {github_expr:?} (as {expression:?}): {error}")
            })
        })
        .collect()
}

/// Record of one schedule trigger fire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleFire {
    /// Workflow path relative to the workspace root.
    pub workflow_path: String,
    /// The cron expression that fired.
    pub cron_expr: String,
    /// UTC timestamp when the job fired.
    pub fired_at: DateTime<Utc>,
    /// Run ID accepted by the server, if any.
    pub run_id: Option<String>,
    /// Error if the submit was rejected.
    pub error: Option<String>,
}

struct CronJob {
    aborts: Vec<AbortHandle>,
}

/// The cron schedule executor.
///
/// One instance lives in `AppState` when `--enable-scheduler` is set.
/// It owns the set of active cron tasks and the fire history.
pub struct Scheduler {
    /// (workflow_path, cron_expr) → CronJob
    jobs: Mutex<HashMap<(String, String), CronJob>>,
    /// Last fire instant per workflow/cron to deduplicate overlapping DOM/DOW schedules.
    last_fires: Mutex<HashMap<(String, String), DateTime<Utc>>>,
    /// Fire history, newest at the end, capped at 1000.
    pub history: Mutex<Vec<ScheduleFire>>,
}

impl Scheduler {
    /// Create a new empty scheduler.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            jobs: Mutex::new(HashMap::new()),
            last_fires: Mutex::new(HashMap::new()),
            history: Mutex::new(Vec::new()),
        })
    }

    /// Reconcile cron jobs for one workflow file.
    ///
    /// Called:
    /// 1. On server startup (once per workspace workflow).
    /// 2. After every push to the default branch.
    ///
    /// Removes cron jobs whose expressions are no longer in the YAML.
    /// Adds new cron jobs for expressions that are newly present.
    /// MC.cs:882-932.
    pub async fn reconcile(
        self: &Arc<Self>,
        workflow_path: &str,
        workflow_yaml: &str,
        push_payload: serde_json::Value,
        shared: Arc<SharedState>,
    ) {
        let (current_crons, timezones) = match parse_workflow(workflow_yaml) {
            Ok(wf) => (
                extract_schedule_crons(&wf.on),
                extract_schedule_timezones(&wf.on),
            ),
            Err(e) => {
                warn!("scheduler: could not parse {workflow_path}: {e}");
                (vec![], BTreeMap::new())
            }
        };

        let canonical_path = canonical_workflow_path(workflow_path);

        let mut jobs = self.jobs.lock().await;

        // Delete cron jobs that are no longer in the YAML (MC.cs:889-895).
        let path_key = canonical_path.clone();
        jobs.retain(|(path, expr), job| {
            if path == &path_key && !current_crons.contains(expr) {
                for abort in &job.aborts {
                    abort.abort();
                }
                info!("scheduler: removed cron {expr:?} for {path}");
                false
            } else {
                true
            }
        });

        // Always replace surviving schedules. A cron expression alone is not
        // the schedule identity: YAML and default-branch context can change.
        for cron_expr in current_crons {
            let key = (canonical_path.clone(), cron_expr.clone());
            let timezone = timezones.get(&cron_expr).cloned();
            if let Some(previous) = jobs.remove(&key) {
                for abort in previous.aborts {
                    abort.abort();
                }
            }

            let schedules = match github_to_crons(&cron_expr) {
                Ok(schedules) => schedules,
                Err(error) => {
                    warn!("scheduler: invalid cron {cron_expr:?} for {canonical_path}: {error}");
                    continue;
                }
            };

            let mut aborts = Vec::with_capacity(schedules.len());
            for schedule in schedules {
                let this = Arc::clone(self);
                let shared2 = Arc::clone(&shared);
                let workflow_path = canonical_path.clone();
                let cron = cron_expr.clone();
                let context = push_payload.clone();
                let yaml = workflow_yaml.to_owned();
                let timezone = timezone.clone();
                let job = tokio::spawn(async move {
                    cron_loop(
                        this,
                        shared2,
                        workflow_path,
                        cron,
                        context,
                        yaml,
                        schedule,
                        timezone,
                    )
                    .await;
                });
                aborts.push(job.abort_handle());
            }
            jobs.insert(key, CronJob { aborts });
            info!("scheduler: registered cron {cron_expr:?} for {canonical_path}");
        }
    }

    /// Scan a local workspace directory and register all schedule workflows.
    ///
    /// Called once on server startup when `--enable-scheduler` is active.
    pub async fn scan_workspace(self: &Arc<Self>, workspace: &PathBuf, shared: Arc<SharedState>) {
        let wf_dir = workspace.join(".github").join("workflows");
        let entries = match std::fs::read_dir(&wf_dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("yml") | Some("yaml")
            ) {
                continue;
            }
            let rel = canonical_workflow_path(
                path.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .as_ref(),
            );
            let default_branch = detect_default_branch(workspace)
                .await
                .unwrap_or_else(|| "main".to_owned());
            let default_path = rel.trim_start_matches(".github/workflows/");
            let yaml = tokio::process::Command::new("git")
                .arg("-C")
                .arg(workspace)
                .args([
                    "show",
                    &format!("refs/remotes/origin/{default_branch}:{rel}"),
                ])
                .output()
                .await
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok());
            let yaml = match yaml {
                Some(yaml) => Some(yaml),
                None => tokio::process::Command::new("git")
                    .arg("-C")
                    .arg(workspace)
                    .args(["show", &format!("refs/heads/{default_branch}:{rel}")])
                    .output()
                    .await
                    .ok()
                    .filter(|output| output.status.success())
                    .and_then(|output| String::from_utf8(output.stdout).ok()),
            };
            let yaml = yaml.or_else(|| {
                std::fs::read_to_string(workspace.join(".github/workflows").join(default_path)).ok()
            });
            let Some(yaml) = yaml else {
                warn!("scheduler: cannot read {rel} from default branch");
                continue;
            };
            let push_payload = serde_json::json!({
                "ref": format!("refs/heads/{default_branch}"),
                "after": latest_default_branch_sha(workspace, &default_branch).await,
                "repository": {
                    "full_name": shared.state.local_workspace.as_ref()
                        .and_then(|path| path.file_name())
                        .map(|name| format!("local/{}", name.to_string_lossy()))
                        .unwrap_or_else(|| "local/repo".to_owned()),
                    "default_branch": default_branch,
                },
            });
            self.reconcile(&rel, &yaml, push_payload, Arc::clone(&shared))
                .await;
        }
    }

    /// Fetch and install schedules for a remote-backed server at startup.
    /// The repository and token use `AKSH_GITHUB_REPOSITORY` and
    /// `AKSH_GITHUB_TOKEN`, the same explicit remote workflow configuration.
    pub async fn scan_remote(self: &Arc<Self>, shared: Arc<SharedState>) {
        let (Ok(repository), Ok(token)) = (
            std::env::var("AKSH_GITHUB_REPOSITORY"),
            std::env::var("AKSH_GITHUB_TOKEN"),
        ) else {
            warn!("scheduler: remote startup scan requires AKSH_GITHUB_REPOSITORY and AKSH_GITHUB_TOKEN");
            return;
        };
        let client = crate::shared_http::CLIENT.clone();
        let metadata = match client
            .get(format!("https://api.github.com/repos/{repository}"))
            .header("User-Agent", "aksh")
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                warn!(status = %response.status(), "scheduler: remote repository metadata fetch failed");
                return;
            }
            Err(error) => {
                warn!(?error, "scheduler: remote repository metadata fetch failed");
                return;
            }
        };
        let metadata: serde_json::Value = match metadata.json().await {
            Ok(value) => value,
            Err(error) => {
                warn!(?error, "scheduler: remote repository metadata parse failed");
                return;
            }
        };
        let default_branch = metadata
            .get("default_branch")
            .and_then(|value| value.as_str())
            .unwrap_or("main")
            .to_owned();
        let sha = match client
            .get(format!(
                "https://api.github.com/repos/{repository}/commits/{default_branch}"
            ))
            .header("User-Agent", "aksh")
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => response
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|commit| {
                    commit
                        .get("sha")
                        .and_then(|value| value.as_str())
                        .map(str::to_owned)
                }),
            _ => None,
        };
        let workflows = match crate::github::fetch_workflows(
            &None,
            &repository,
            &format!("refs/heads/{default_branch}"),
        )
        .await
        {
            Ok(workflows) => workflows,
            Err(error) => {
                warn!(?error, "scheduler: remote workflow fetch failed");
                return;
            }
        };
        let context = serde_json::json!({
            "ref": format!("refs/heads/{default_branch}"),
            "after": sha,
            "repository": { "full_name": repository, "default_branch": default_branch },
        });
        self.reconcile_all(&workflows, context, shared).await;
    }

    /// Reconcile the complete default-branch workflow inventory, including
    /// removal of schedules belonging to deleted workflow files.
    pub async fn reconcile_all(
        self: &Arc<Self>,
        workflows: &BTreeMap<String, String>,
        push_payload: serde_json::Value,
        shared: Arc<SharedState>,
    ) {
        let desired_paths: std::collections::HashSet<String> = workflows
            .keys()
            .map(|path| canonical_workflow_path(path))
            .collect();
        {
            let mut jobs = self.jobs.lock().await;
            jobs.retain(|(path, _), job| {
                if desired_paths.contains(path) {
                    true
                } else {
                    for abort in &job.aborts {
                        abort.abort();
                    }
                    false
                }
            });
        }
        for (path, yaml) in workflows {
            self.reconcile(path, yaml, push_payload.clone(), Arc::clone(&shared))
                .await;
        }
    }

    async fn record_fire(&self, fire: ScheduleFire) {
        let mut history = self.history.lock().await;
        history.push(fire);
        if history.len() > 1000 {
            history.remove(0);
        }
    }

    async fn claim_fire(
        &self,
        workflow_path: &str,
        cron_expr: &str,
        fired_at: DateTime<Utc>,
    ) -> bool {
        let key = (workflow_path.to_owned(), cron_expr.to_owned());
        let mut last_fires = self.last_fires.lock().await;
        if last_fires
            .get(&key)
            .is_some_and(|last| last.timestamp() == fired_at.timestamp())
        {
            return false;
        }
        last_fires.insert(key, fired_at);
        true
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// Extract every `on.schedule[*].cron` string from a Trigger.
fn extract_schedule_crons(trigger: &Trigger) -> Vec<String> {
    let config = match trigger {
        Trigger::Map(map) => match map.get("schedule") {
            Some(value) => value,
            None => return vec![],
        },
        _ => return vec![],
    };
    config
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| item.get("cron")?.as_str().map(str::to_owned))
        .collect()
}
fn extract_schedule_timezones(trigger: &Trigger) -> BTreeMap<String, String> {
    let Some(config) = (match trigger {
        Trigger::Map(map) => map.get("schedule"),
        _ => None,
    }) else {
        return BTreeMap::new();
    };
    config
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| {
            Some((
                item.get("cron")?.as_str()?.to_owned(),
                item.get("timezone")?.as_str()?.to_owned(),
            ))
        })
        .collect()
}

fn canonical_workflow_path(workflow_path: &str) -> String {
    if workflow_path.starts_with(".github/workflows/") {
        workflow_path.to_owned()
    } else {
        format!(".github/workflows/{workflow_path}")
    }
}

async fn detect_default_branch(workspace: &PathBuf) -> Option<String> {
    let output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["symbolic-ref", "--short", "refs/remotes/origin/HEAD"])
        .output()
        .await
        .ok()?;
    if output.status.success() {
        let value = String::from_utf8(output.stdout).ok()?;
        return value.trim().rsplit('/').next().map(str::to_owned);
    }
    let output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["branch", "--show-current"])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|branch| !branch.is_empty())
}

async fn latest_default_branch_sha(workspace: &PathBuf, branch: &str) -> Option<String> {
    let output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["rev-parse", &format!("refs/remotes/origin/{branch}")])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return latest_commit_sha(workspace).await;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|sha| sha.len() == 40 && sha.chars().all(|character| character.is_ascii_hexdigit()))
}

async fn latest_commit_sha(workspace: &PathBuf) -> Option<String> {
    let output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["rev-parse", "HEAD"])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|sha| sha.len() == 40 && sha.chars().all(|character| character.is_ascii_hexdigit()))
}

fn next_schedule_time(schedule: &Schedule, timezone: Option<&str>) -> Option<DateTime<Utc>> {
    if let Some(timezone) = timezone {
        match timezone.parse::<chrono_tz::Tz>() {
            Ok(zone) => {
                return schedule
                    .upcoming(zone)
                    .next()
                    .map(|time| time.with_timezone(&Utc))
            }
            Err(error) => warn!(%timezone, ?error, "scheduler: invalid timezone; using UTC"),
        }
    }
    schedule.upcoming(Utc).next()
}

/// Persistent async loop: sleeps until next tick, fires, repeats.
async fn cron_loop(
    scheduler: Arc<Scheduler>,
    shared: Arc<SharedState>,
    workflow_path: String,
    cron_expr: String,
    push_context: serde_json::Value,
    workflow_yaml: String,
    schedule: Schedule,
    timezone: Option<String>,
) {
    loop {
        let now = Utc::now();
        let next = match next_schedule_time(&schedule, timezone.as_deref()) {
            Some(t) => t,
            None => {
                warn!("scheduler: no future tick for {cron_expr:?} — stopping task");
                return;
            }
        };

        let delay = match (next - now).to_std() {
            Ok(d) => d,
            Err(_) => std::time::Duration::ZERO,
        };

        info!(
            "scheduler: [{workflow_path}] next cron {cron_expr:?} in {}s (at {next})",
            delay.as_secs()
        );

        tokio::time::sleep(delay).await;

        let fired_at = next;
        if !scheduler
            .claim_fire(&workflow_path, &cron_expr, fired_at)
            .await
        {
            continue;
        }
        info!("scheduler: firing [{workflow_path}] cron {cron_expr:?}");

        let mut schedule_payload = serde_json::json!({ "schedule": cron_expr });
        for key in [
            "sender",
            "repository",
            "organization",
            "enterprise",
            "after",
        ] {
            if let Some(value) = push_context.get(key) {
                schedule_payload[key] = value.clone();
            }
        }
        let default_branch = push_context
            .get("repository")
            .and_then(|repository| repository.get("default_branch"))
            .and_then(|value| value.as_str())
            .unwrap_or("main")
            .to_owned();
        let repository = push_context
            .get("repository")
            .and_then(|value| value.get("full_name"))
            .and_then(|value| value.as_str())
            .unwrap_or("local/repo")
            .to_owned();
        let resolved_sha = push_context
            .get("after")
            .and_then(|value| value.as_str())
            .map(str::to_owned);
        let submission = WorkflowSubmission {
            workflow_yaml: workflow_yaml.clone(),
            event: "schedule".to_owned(),
            payload: schedule_payload,
            repository,
            git_ref: format!("refs/heads/{default_branch}"),
            workflow_path: Some(workflow_path.to_owned()),
            local_workspace: None,
            vars: Default::default(),
            secrets: Default::default(),
            reusable_workflows: Default::default(),
            reusable_workflow_shas: Default::default(),
            enable_debugger: false,
            debugger_welcome_message: None,
            sha: resolved_sha.clone().unwrap_or_else(|| "0".repeat(40)),
            actor: "aksh-scheduler".to_owned(),
            environment: None,
            workflow_file: Some(workflow_path.to_owned()),
            inputs: Default::default(),
            trust_tier: Some("schedule".to_owned()),
            workflow_run_upstream_names: vec![],
            activity_type: Some("schedule".to_owned()),
            resolved_sha,
            changed_paths: vec![],
            changed_paths_known: true,
            filter_branch: None,
            dispatch_inputs: Default::default(),
            dispatch_inputs_stringified: Default::default(),
            selected_jobs: vec![],
            base_ref: None,
            preserve_on_failure: false,
        };
        let (run_id, error) = match submit_run_inner(&shared, submission).await {
            Ok(accepted) => {
                info!(
                    "scheduler: schedule run {} accepted for [{workflow_path}]",
                    accepted.run_id
                );
                (Some(accepted.run_id.to_string()), None)
            }
            Err(error) => {
                warn!("scheduler: run rejected for [{workflow_path}]: {error:?}");
                (None, Some(format!("{error:?}")))
            }
        };

        scheduler
            .record_fire(ScheduleFire {
                workflow_path: workflow_path.clone(),
                cron_expr: cron_expr.clone(),
                fired_at,
                run_id,
                error,
            })
            .await;
    }
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the 7-field expansion is parseable by the cron crate.
    #[test]
    fn github_to_cron_daily() {
        let s = github_to_cron("0 0 * * *").expect("daily midnight");
        let next = s.upcoming(Utc).next().expect("has next");
        assert!(next > Utc::now());
    }

    #[test]
    fn github_to_cron_rejects_sub_five_minute_frequency() {
        assert!(github_to_cron("* * * * *").is_err());
        assert!(github_to_cron("*/1 * * * *").is_err());
    }

    #[test]
    fn github_dom_dow_restricted_creates_union_schedules() {
        assert_eq!(github_to_crons("0 0 1 * 1").unwrap().len(), 2);
    }

    #[test]
    fn github_to_cron_every_5_minutes() {
        let s = github_to_cron("*/5 * * * *").expect("every 5 min");
        let next = s.upcoming(Utc).next().expect("has next");
        assert!(next > Utc::now());
    }

    #[test]
    fn github_to_cron_rejects_6_field() {
        assert!(github_to_cron("0 0 0 * * *").is_err());
    }

    #[test]
    fn github_to_cron_weekday() {
        // "At 09:00 on Monday"
        let s = github_to_cron("0 9 * * 1").expect("monday 9am");
        let next = s.upcoming(Utc).next().expect("has next");
        assert!(next > Utc::now());
    }

    #[test]
    fn extract_schedule_crons_empty_non_schedule() {
        let wf = parse_workflow(
            r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
        )
        .unwrap();
        assert!(extract_schedule_crons(&wf.on).is_empty());
    }

    #[test]
    fn extract_schedule_crons_single() {
        let wf = parse_workflow(
            r#"
on:
  schedule:
    - cron: '0 2 * * *'
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
        )
        .unwrap();
        assert_eq!(extract_schedule_crons(&wf.on), vec!["0 2 * * *"]);
    }

    #[test]
    fn extract_schedule_crons_multiple() {
        let wf = parse_workflow(
            r#"
on:
  schedule:
    - cron: '0 0 * * *'
    - cron: '30 12 * * 1'
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
        )
        .unwrap();
        let crons = extract_schedule_crons(&wf.on);
        assert_eq!(crons, vec!["0 0 * * *", "30 12 * * 1"]);
    }
    #[tokio::test]
    async fn test_scheduler_reconcile_lifecycle() {
        let temp = tempfile::tempdir().unwrap();
        let state = crate::AppState::new(temp.path().to_path_buf())
            .await
            .unwrap();
        let shared = Arc::new(crate::SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        let scheduler = Scheduler::new();

        let workflow_path = ".github/workflows/scheduled.yml";
        let workflow_yaml = r#"
on:
  schedule:
    - cron: '0 2 * * *'
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
        let push_payload = serde_json::json!({
            "repository": {
                "full_name": "local/repo",
                "default_branch": "main",
            }
        });

        // 1. Reconcile with scheduled trigger
        scheduler
            .reconcile(
                workflow_path,
                workflow_yaml,
                push_payload.clone(),
                shared.clone(),
            )
            .await;

        {
            let jobs = scheduler.jobs.lock().await;
            assert_eq!(jobs.len(), 1);
            let key = (workflow_path.to_owned(), "0 2 * * *".to_owned());
            assert!(jobs.contains_key(&key));
        }

        // 2. Reconcile with different cron
        let workflow_yaml_updated = r#"
on:
  schedule:
    - cron: '30 4 * * *'
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
        scheduler
            .reconcile(
                workflow_path,
                workflow_yaml_updated,
                push_payload.clone(),
                shared.clone(),
            )
            .await;

        {
            let jobs = scheduler.jobs.lock().await;
            assert_eq!(jobs.len(), 1);
            let old_key = (workflow_path.to_owned(), "0 2 * * *".to_owned());
            let new_key = (workflow_path.to_owned(), "30 4 * * *".to_owned());
            assert!(!jobs.contains_key(&old_key));
            assert!(jobs.contains_key(&new_key));
        }

        // 3. Reconcile with no schedule trigger (removes all)
        let workflow_yaml_none = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
        scheduler
            .reconcile(workflow_path, workflow_yaml_none, push_payload, shared)
            .await;

        {
            let jobs = scheduler.jobs.lock().await;
            assert!(jobs.is_empty());
        }
    }

    use proptest::prelude::*;

    fn valid_hour() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("*".to_owned()),
            (0..24u8).prop_map(|v| v.to_string()),
            (1..12u8).prop_map(|v| format!("*/{v}")),
            (0..15u8).prop_map(|v| format!("{}-{}", v, v + 5)),
            (0..10u8).prop_map(|v| format!("{},{}", v, v + 5)),
        ]
    }

    fn valid_dom() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("*".to_owned()),
            (1..32u8).prop_map(|v| v.to_string()),
            (1..15u8).prop_map(|v| format!("*/{v}")),
            (1..20u8).prop_map(|v| format!("{}-{}", v, v + 5)),
            (1..15u8).prop_map(|v| format!("{},{}", v, v + 5)),
        ]
    }

    fn valid_month() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("*".to_owned()),
            (1..13u8).prop_map(|v| v.to_string()),
            (1..6u8).prop_map(|v| format!("*/{v}")),
            (1..6u8).prop_map(|v| format!("{}-{}", v, v + 5)),
            (1..5u8).prop_map(|v| format!("{},{}", v, v + 5)),
        ]
    }

    fn valid_dow() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("*".to_owned()),
            (0..8u8).prop_map(|v| v.to_string()),
            (1..3u8).prop_map(|v| format!("*/{v}")),
            (0..3u8).prop_map(|v| format!("{}-{}", v, v + 2)),
            (0..3u8).prop_map(|v| format!("{},{}", v, v + 2)),
        ]
    }

    fn valid_cron_expr() -> impl Strategy<Value = String> {
        (
            prop_oneof![
                (0..60u8).prop_map(|value| value.to_string()),
                (5..13u8).prop_map(|value| format!("*/{value}")),
            ],
            valid_hour(),
            valid_dom(),
            valid_month(),
            valid_dow(),
        )
            .prop_map(|(minute, hour, dom, month, dow)| {
                format!("{minute} {hour} {dom} {month} {dow}")
            })
    }

    fn invalid_cron_expr() -> impl Strategy<Value = String> {
        prop_oneof![
            // 6 fields
            Just("* * * * * *".to_owned()),
            // 4 fields
            Just("* * * *".to_owned()),
            // Invalid minute value
            // GitHub minimum five-minute interval
            Just("* * * * *".to_owned()),
            Just("*/1 * * * *".to_owned()),
            (60..100u8).prop_map(|v| format!("{v} * * * *")),
            // Invalid hour value
            (24..50u8).prop_map(|v| format!("* {v} * * *")),
            // Invalid day of month
            (32..50u8).prop_map(|v| format!("* * {v} * *")),
            Just("* * 0 * *".to_owned()),
            // Invalid month
            (13..20u8).prop_map(|v| format!("* * * {v} *")),
            Just("* * * 0 *".to_owned()),
            // Invalid day of week
            (8..15u8).prop_map(|v| format!("* * * * {v}")),
            // Malformed symbols
            Just("abc * * * *".to_owned()),
            Just("* * * * abc".to_owned()),
        ]
    }

    proptest! {
        #[test]
        fn proptest_valid_github_crons(ref expr in valid_cron_expr()) {
            let sched = github_to_cron(expr);
            prop_assert!(sched.is_ok(), "Expected Ok for {expr:?}, got {sched:?}");
            let s = sched.unwrap();
            if let Some(next1) = s.upcoming(Utc).next() {
                if let Some(next2) = s.upcoming(Utc).nth(1) {
                    prop_assert!(next2 > next1);
                }
            }
        }
        #[test]
        fn proptest_invalid_github_crons(ref expr in invalid_cron_expr()) {
            let sched = github_to_cron(expr);
            prop_assert!(sched.is_err(), "Expected Err for {expr:?}, got {sched:?}");
        }
    }
}
