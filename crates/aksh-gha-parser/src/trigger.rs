//! Workflow trigger matching.

use serde_json::Value;

use crate::{ParserError, Trigger};

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
        match self {
            Trigger::Single(value) => value == event,
            Trigger::Many(values) => values.iter().any(|value| value == event),
            Trigger::Map(values) => {
                if !values.contains_key(event) {
                    return false;
                }
                // Check branch/tag/path filters
                let config_val = values.get(event);
                if let Some(config) = config_val {
                    if let Some(obj) = config.as_object() {
                        // activity types filter
                        if let Some(types) = obj.get("types") {
                            if let Some(activity_type) = activity_type {
                                if !matches_filter(types, activity_type) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        } else if event == "pull_request" || event == "pull_request_target" {
                            // Default types per MessageController.cs:1259-1268
                            const PR_DEFAULT_TYPES: &[&str] =
                                &["opened", "synchronize", "synchronized", "reopened"];
                            if let Some(activity_type) = activity_type {
                                if !PR_DEFAULT_TYPES.contains(&activity_type) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                        // branches filter
                        if let Some(branches) = obj.get("branches") {
                            if let Some(branch) = branch {
                                if !matches_filter(branches, branch) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                        // branches-ignore
                        if let Some(ignore) = obj.get("branches-ignore") {
                            if let Some(branch) = branch {
                                if matches_filter(ignore, branch) {
                                    return false;
                                }
                            }
                        }
                        // tags filter
                        if let Some(tags) = obj.get("tags") {
                            if let Some(tag) = tag {
                                if !matches_filter(tags, tag) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                        // tags-ignore
                        if let Some(ignore) = obj.get("tags-ignore") {
                            if let Some(tag) = tag {
                                if matches_filter(ignore, tag) {
                                    return false;
                                }
                            }
                        }
                        // A `paths` filter requires at least one known changed
                        // path matching the positive pattern.
                        if let Some(path_filters) = obj.get("paths") {
                            if paths.is_empty()
                                || !paths.iter().any(|path| matches_filter(path_filters, path))
                            {
                                return false;
                            }
                        }
                        // `paths-ignore` suppresses only when every changed
                        // path is ignored. A mixed change set must still run.
                        if let Some(ignore) = obj.get("paths-ignore") {
                            if !paths.is_empty()
                                && paths.iter().all(|path| matches_filter(ignore, path))
                            {
                                return false;
                            }
                        }
                        // `workflow_run.workflows` matches the upstream
                        // workflow display name, not its file path.
                        if let Some(wf_filter) = obj.get("workflows") {
                            if upstream_workflow_paths.is_empty()
                                || !upstream_workflow_paths
                                    .iter()
                                    .any(|name| matches_filter(wf_filter, name))
                            {
                                return false;
                            }
                        }
                    } else if event == "pull_request" || event == "pull_request_target" {
                        // Config exists but is null/empty (e.g. `on:\n  pull_request:`).
                        // Apply default types per MessageController.cs:1259-1268.
                        const PR_DEFAULT_TYPES: &[&str] =
                            &["opened", "synchronize", "synchronized", "reopened"];
                        if let Some(activity_type) = activity_type {
                            if !PR_DEFAULT_TYPES.contains(&activity_type) {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                }
                true
            }
        }
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
