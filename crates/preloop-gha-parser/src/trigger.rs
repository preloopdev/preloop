//! Workflow trigger matching.

use serde_json::Value;

use crate::{ParserError, Trigger};

/// Why a workflow did not run for an event.
///
/// Carries the offending axis and the values involved so a caller can tell a
/// user what to change, rather than only that "the workflow does not match".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerMismatch {
    /// The workflow has no `on:` entry for this event.
    EventNotDeclared {
        /// Events the workflow does declare, in source order.
        declared: Vec<String>,
    },
    /// The event is filtered by activity type and none was supplied.
    ActivityTypeMissing {
        /// Activity types the workflow accepts.
        accepted: Vec<String>,
    },
    /// An activity type was supplied but this workflow filters it out.
    ActivityTypeRejected {
        /// The activity type supplied.
        got: String,
        /// Activity types the workflow accepts.
        accepted: Vec<String>,
    },
    /// Neither the branch nor the tag axis accepted the ref.
    RefFiltered {
        /// Branch the run is on, when it is a branch.
        branch: Option<String>,
        /// Tag the run is on, when it is a tag.
        tag: Option<String>,
        /// The `branches`/`tags` filter keys present on the event, each with
        /// its patterns, in the order GitHub applies them. One list rather
        /// than four keeps this error small enough to return by value.
        filters: Vec<(String, Vec<String>)>,
    },
    /// A `paths:` filter matched none of the changed files.
    PathsUnmatched {
        /// How many changed paths were considered.
        changed: usize,
        /// `paths:` patterns.
        filters: Vec<String>,
    },
    /// Every changed file matched `paths-ignore:`.
    PathsAllIgnored {
        /// How many changed paths were considered.
        changed: usize,
        /// `paths-ignore:` patterns.
        filters: Vec<String>,
    },
    /// `on.workflow_run.workflows:` matched no upstream workflow name.
    UpstreamWorkflowUnmatched {
        /// `workflows:` patterns.
        filters: Vec<String>,
    },
}

impl std::fmt::Display for TriggerMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EventNotDeclared { declared } => {
                if declared.is_empty() {
                    write!(f, "the workflow declares no events in `on:`")
                } else {
                    write!(f, "the workflow declares only: {}", declared.join(", "))
                }
            }
            Self::ActivityTypeMissing { accepted } => write!(
                f,
                "this event is filtered by activity type ({}) and none was supplied",
                accepted.join(", ")
            ),
            Self::ActivityTypeRejected { got, accepted } => write!(
                f,
                "activity type `{got}` is not accepted; the workflow accepts: {}",
                accepted.join(", ")
            ),
            Self::RefFiltered {
                branch,
                tag,
                filters,
            } => {
                let is_branch_only = filters
                    .iter()
                    .all(|(label, _)| label.starts_with("branches"));
                let is_tag_only = filters.iter().all(|(label, _)| label.starts_with("tags"));
                if branch.is_none() && tag.is_some() && is_branch_only {
                    let tag_name = tag.as_deref().unwrap_or("tag");
                    return write!(
                        f,
                        "the workflow declares only branch filters; tag `{tag_name}` cannot match `branches`"
                    );
                }
                if tag.is_none() && branch.is_some() && is_tag_only {
                    let branch_name = branch.as_deref().unwrap_or("branch");
                    return write!(
                        f,
                        "the workflow declares only tag filters; branch `{branch_name}` cannot match `tags`"
                    );
                }
                let subject = match (branch, tag) {
                    (Some(branch), _) => format!("branch `{branch}`"),
                    (None, Some(tag)) => format!("tag `{tag}`"),
                    (None, None) => "the run's ref".to_owned(),
                };
                write!(f, "{subject} is excluded by")?;
                for (index, (label, patterns)) in filters.iter().enumerate() {
                    write!(
                        f,
                        "{} {label}: [{}]",
                        if index == 0 { "" } else { "," },
                        patterns.join(", ")
                    )?;
                }
                Ok(())
            }
            Self::PathsUnmatched { changed, filters } => write!(
                f,
                "none of the {changed} changed file(s) match paths: [{}]",
                filters.join(", ")
            ),
            Self::PathsAllIgnored { changed, filters } => write!(
                f,
                "all {changed} changed file(s) match paths-ignore: [{}]",
                filters.join(", ")
            ),
            Self::UpstreamWorkflowUnmatched { filters } => write!(
                f,
                "no upstream workflow name matches workflows: [{}]",
                filters.join(", ")
            ),
        }
    }
}

/// The literal patterns behind a filter value, for reporting.
fn filter_patterns(filter: &Value) -> Vec<String> {
    match filter {
        Value::String(pattern) => vec![pattern.clone()],
        Value::Array(patterns) => patterns
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

fn normalize_activity_type(activity_type: &str) -> &str {
    if activity_type == "synchronized" {
        "synchronize"
    } else {
        activity_type
    }
}

/// Apply an event's default activity-type list.
fn check_activity_type(
    activity_type: Option<&str>,
    defaults: &[&str],
) -> Result<(), TriggerMismatch> {
    let accepted = defaults.iter().map(|t| (*t).to_owned()).collect();
    match activity_type {
        Some(activity_type) => {
            let normalized = normalize_activity_type(activity_type);
            if defaults.contains(&normalized) || defaults.contains(&activity_type) {
                Ok(())
            } else {
                Err(TriggerMismatch::ActivityTypeRejected {
                    got: activity_type.to_owned(),
                    accepted,
                })
            }
        }
        None => Err(TriggerMismatch::ActivityTypeMissing { accepted }),
    }
}

impl Trigger {
    /// Returns true when the workflow should run for an event.
    pub fn matches(&self, event: &str) -> bool {
        match self {
            Trigger::Single(value) => value == event,
            Trigger::Many(values) => values.iter().any(|value| value == event),
            Trigger::Map(values) => values.contains_key(event),
        }
    }

    /// Whether the event configuration contains path-based filters.
    pub fn has_path_filters(&self, event: &str) -> bool {
        matches!(
            self,
            Trigger::Map(values)
                if values.get(event).and_then(Value::as_object).is_some_and(|config| {
                    config.contains_key("paths") || config.contains_key("paths-ignore")
                })
        )
    }

    /// Every event this workflow declares in its `on:` block, in source order.
    ///
    /// Used to tell a caller which `--event` values the workflow would accept
    /// instead of only that the one they passed was wrong.
    pub fn declared_events(&self) -> Vec<String> {
        match self {
            Trigger::Single(value) => vec![value.clone()],
            Trigger::Many(values) => values.clone(),
            Trigger::Map(values) => values.keys().cloned().collect(),
        }
    }

    /// Returns true when the workflow should run for an event with context.
    /// Supports branch/tag/path filtering.
    pub fn matches_with_context(
        &self,
        event: &str,
        branch: Option<&str>,
        tag: Option<&str>,
        paths: &[String],
        activity_type: Option<&str>,
        upstream_workflow_paths: &[String],
    ) -> bool {
        self.match_event(
            event,
            branch,
            tag,
            paths,
            activity_type,
            upstream_workflow_paths,
        )
        .is_ok()
    }

    /// Same decision as [`Trigger::matches_with_context`], but reporting which
    /// filter axis rejected the event.
    ///
    /// The predicate delegates here so the two can never disagree: a caller
    /// that only needs a bool gets `.is_ok()`, and one that has to explain
    /// itself to a user gets the reason.
    pub fn match_event(
        &self,
        event: &str,
        branch: Option<&str>,
        tag: Option<&str>,
        paths: &[String],
        activity_type: Option<&str>,
        upstream_workflow_paths: &[String],
    ) -> Result<(), TriggerMismatch> {
        // Default PR activity types per MessageController.cs:1259-1268.
        const PR_DEFAULT_TYPES: &[&str] = &["opened", "synchronize", "reopened"];

        let not_declared = || TriggerMismatch::EventNotDeclared {
            declared: self.declared_events(),
        };

        let values = match self {
            Trigger::Single(value) => {
                return if value == event {
                    Ok(())
                } else {
                    Err(not_declared())
                };
            }
            Trigger::Many(values) => {
                return if values.iter().any(|value| value == event) {
                    Ok(())
                } else {
                    Err(not_declared())
                };
            }
            Trigger::Map(values) => values,
        };
        if !values.contains_key(event) {
            return Err(not_declared());
        }
        let Some(config) = values.get(event) else {
            return Ok(());
        };

        // Config exists but is null/empty (e.g. `on:\n  pull_request:`).
        let Some(obj) = config.as_object() else {
            if event == "pull_request" || event == "pull_request_target" {
                return check_activity_type(activity_type, PR_DEFAULT_TYPES);
            }
            return Ok(());
        };

        // Activity types.
        if let Some(types) = obj.get("types") {
            let accepted = filter_patterns(types);
            match activity_type {
                Some(activity_type) => {
                    let normalized = normalize_activity_type(activity_type);
                    if !matches_filter(types, activity_type) && !matches_filter(types, normalized) {
                        return Err(TriggerMismatch::ActivityTypeRejected {
                            got: activity_type.to_owned(),
                            accepted,
                        });
                    }
                }
                None => return Err(TriggerMismatch::ActivityTypeMissing { accepted }),
            }
        } else if event == "pull_request" || event == "pull_request_target" {
            check_activity_type(activity_type, PR_DEFAULT_TYPES)?;
        }

        // Branches and tags are OR-filter axes on `push` (GitHub: "The
        // workflow will run for pushes to matching branches or pushes of
        // matching tags"). A branch push is filtered only by the branch axis;
        // a tag push only by the tag axis. When either axis is present and the
        // push does not belong to it, the other axis still decides — `push:
        // {branches: [main], tags: ['v*']}` must run for a push to `main`.
        // An axis is "present" when its positive or ignore filter is defined;
        // with no axis defined the push runs unconditionally.
        let branch_axis = obj.get("branches").is_some() || obj.get("branches-ignore").is_some();
        let tag_axis = obj.get("tags").is_some() || obj.get("tags-ignore").is_some();
        if branch_axis || tag_axis {
            let branch_ok = branch_axis
                && branch.is_some()
                && obj
                    .get("branches")
                    .is_none_or(|filters| matches_filter(filters, branch.unwrap()))
                && obj
                    .get("branches-ignore")
                    .is_none_or(|ignore| !matches_filter(ignore, branch.unwrap()));
            let tag_ok = tag_axis
                && tag.is_some()
                && obj
                    .get("tags")
                    .is_none_or(|filters| matches_filter(filters, tag.unwrap()))
                && obj
                    .get("tags-ignore")
                    .is_none_or(|ignore| !matches_filter(ignore, tag.unwrap()));
            if !branch_ok && !tag_ok {
                return Err(TriggerMismatch::RefFiltered {
                    branch: branch.map(str::to_owned),
                    tag: tag.map(str::to_owned),
                    filters: ["branches", "branches-ignore", "tags", "tags-ignore"]
                        .into_iter()
                        .filter_map(|key| {
                            obj.get(key)
                                .map(|value| (key.to_owned(), filter_patterns(value)))
                        })
                        .collect(),
                });
            }
        }

        // A `paths` filter requires at least one known changed path matching
        // the positive pattern.
        if let Some(path_filters) = obj.get("paths") {
            if paths.is_empty() || !paths.iter().any(|path| matches_filter(path_filters, path)) {
                return Err(TriggerMismatch::PathsUnmatched {
                    changed: paths.len(),
                    filters: filter_patterns(path_filters),
                });
            }
        }
        // `paths-ignore` suppresses only when every changed path is ignored.
        // A mixed change set must still run.
        if let Some(ignore) = obj.get("paths-ignore") {
            if !paths.is_empty() && paths.iter().all(|path| matches_filter(ignore, path)) {
                return Err(TriggerMismatch::PathsAllIgnored {
                    changed: paths.len(),
                    filters: filter_patterns(ignore),
                });
            }
        }
        // `workflow_run.workflows` matches the upstream workflow display name,
        // not its file path.
        if let Some(wf_filter) = obj.get("workflows") {
            if upstream_workflow_paths.is_empty()
                || !upstream_workflow_paths
                    .iter()
                    .any(|name| matches_filter(wf_filter, name))
            {
                return Err(TriggerMismatch::UpstreamWorkflowUnmatched {
                    filters: filter_patterns(wf_filter),
                });
            }
        }
        Ok(())
    }

    /// Returns the set of valid filter keys for a given event name.
    /// Mirrors MessageController.cs:994-1020.
    pub fn valid_filter_keys(event: &str) -> &'static [&'static str] {
        match event {
            "push" => &[
                "branches",
                "branches-ignore",
                "tags",
                "tags-ignore",
                "paths",
                "paths-ignore",
            ],
            "pull_request" | "pull_request_target" => &[
                "types",
                "branches",
                "branches-ignore",
                "paths",
                "paths-ignore",
            ],
            "workflow_run" => &["types", "branches", "branches-ignore", "workflows"],
            "schedule" => &["cron", "timezone"],
            _ => &["types"],
        }
    }

    /// Validate filter keys for an event. Returns Ok(()) or
    /// ParserError::InvalidFilterForKey (a warning — GitHub only warns,
    /// does not reject the workflow).
    pub fn validate_filters(&self, event: &str) -> Result<(), ParserError> {
        if let Trigger::Map(values) = self {
            if let Some(config) = values.get(event) {
                if let Some(obj) = config.as_object() {
                    let valid = Self::valid_filter_keys(event);
                    for key in obj.keys() {
                        if !valid.contains(&key.as_str()) {
                            return Err(ParserError::InvalidFilterForKey {
                                event: event.to_owned(),
                                key: key.clone(),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Check for mutually exclusive filter pairs. Mirrors
    /// MessageController.cs:1236-1250.
    pub fn check_conflicting_filters(&self, event: &str) -> Result<(), ParserError> {
        if let Trigger::Map(values) = self {
            if let Some(config) = values.get(event) {
                if let Some(obj) = config.as_object() {
                    let pairs: &[(&str, &str)] = &[
                        ("branches", "branches-ignore"),
                        ("tags", "tags-ignore"),
                        ("paths", "paths-ignore"),
                    ];
                    for &(a, b) in pairs {
                        if obj.contains_key(a) && obj.contains_key(b) {
                            return Err(ParserError::ConflictingFilters {
                                event: event.to_owned(),
                                a: a.to_owned(),
                                b: b.to_owned(),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Check whether a filter value matches GitHub's ordered pattern semantics.
pub(crate) fn matches_filter(filter: &Value, value: &str) -> bool {
    matches_filter_with_default(filter, value, false)
}

pub(crate) fn matches_filter_with_default(filter: &Value, value: &str, default: bool) -> bool {
    let patterns: Vec<&str> = match filter {
        Value::String(pattern) => vec![pattern.as_str()],
        Value::Array(patterns) => patterns.iter().filter_map(Value::as_str).collect(),
        _ => return false,
    };
    if patterns.is_empty() {
        return default;
    }
    let mut matched = default;
    for pattern in patterns {
        let (negative, pattern) = pattern
            .strip_prefix('!')
            .map_or((false, pattern), |p| (true, p));
        if glob_match(pattern, value) {
            matched = !negative;
        }
    }
    matched
}

/// GitHub-style glob matching anchored to the whole value.
pub(crate) fn glob_match(pattern: &str, value: &str) -> bool {
    fn matches(pattern: &[char], value: &[char], pi: usize, vi: usize) -> bool {
        if pi == pattern.len() {
            return vi == value.len();
        }
        if pattern[pi] == '*' {
            let double_star = pattern.get(pi + 1) == Some(&'*');
            let next_pi = if double_star { pi + 2 } else { pi + 1 };
            if matches(pattern, value, next_pi, vi) {
                return true;
            }
            let mut next_vi = vi;
            while next_vi < value.len() {
                if !double_star && value[next_vi] == '/' {
                    break;
                }
                next_vi += 1;
                if matches(pattern, value, next_pi, next_vi) {
                    return true;
                }
            }
            return false;
        }
        if pattern[pi] == '?' {
            return vi < value.len() && value[vi] != '/' && matches(pattern, value, pi + 1, vi + 1);
        }
        vi < value.len() && pattern[pi] == value[vi] && matches(pattern, value, pi + 1, vi + 1)
    }
    matches(
        &pattern.chars().collect::<Vec<_>>(),
        &value.chars().collect::<Vec<_>>(),
        0,
        0,
    )
}
