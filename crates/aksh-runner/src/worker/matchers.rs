//! Problem matcher support.
//!
//! Parses `::add-matcher::path.json` / `::remove-matcher owner=name::`
//! and matches log lines against registered patterns to produce annotations.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::debug;

/// A registered problem matcher.
#[derive(Debug, Clone)]
pub struct ProblemMatcher {
    pub owner: String,
    pub patterns: Vec<MatcherPattern>,
    /// F051: Default base directory for resolving relative file paths in annotations.
    pub from_path: String,
    pub state: Vec<Option<PatternMatch>>,
    /// Regexes compiled from `patterns` at registration time; index-parallel to `patterns`.
    /// Avoids re-compiling on every log line (hot path).
    pub compiled_regexes: Vec<regex::Regex>,
}

impl ProblemMatcher {
    pub fn reset(&mut self) {
        if self.patterns.len() > 1 {
            self.state = vec![None; self.patterns.len() - 1];
        } else {
            self.state = Vec::new();
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PatternMatch {
    pub file: Option<String>,
    pub line: Option<String>,
    pub column: Option<String>,
    pub end_line: Option<String>,
    pub end_column: Option<String>,
    pub severity: Option<String>,
    pub code: Option<String>,
    pub message: Option<String>,
    pub from_path: Option<String>,
}

fn get_group_value(captures: &regex::Captures, index: Option<usize>) -> Option<String> {
    let idx = index?;
    captures.get(idx).map(|m| m.as_str().to_string())
}

impl PatternMatch {
    pub fn new(
        running_match: Option<&PatternMatch>,
        pattern: &MatcherPattern,
        captures: &regex::Captures,
        default_severity: &str,
        default_from_path: &str,
    ) -> Self {
        let file = running_match
            .and_then(|r| r.file.clone())
            .or_else(|| get_group_value(captures, pattern.file));
        let line = running_match
            .and_then(|r| r.line.clone())
            .or_else(|| get_group_value(captures, pattern.line));
        let column = running_match
            .and_then(|r| r.column.clone())
            .or_else(|| get_group_value(captures, pattern.column));
        let end_line = running_match
            .and_then(|r| r.end_line.clone())
            .or_else(|| get_group_value(captures, pattern.end_line));
        let end_column = running_match
            .and_then(|r| r.end_column.clone())
            .or_else(|| get_group_value(captures, pattern.end_column));

        let mut severity =
            running_match
                .and_then(|r| r.severity.clone())
                .or_else(|| match &pattern.severity {
                    Some(SeveritySpec::Capture(g)) => {
                        captures.get(*g).map(|m| m.as_str().to_string())
                    }
                    Some(SeveritySpec::Literal(value)) => Some(value.to_string()),
                    None => None,
                });
        if (severity.is_none() || severity.as_deref().is_some_and(|s| s.is_empty()))
            && !default_severity.is_empty()
        {
            severity = Some(default_severity.to_string());
        }

        let code = running_match
            .and_then(|r| r.code.clone())
            .or_else(|| get_group_value(captures, pattern.code));
        let message = running_match
            .and_then(|r| r.message.clone())
            .or_else(|| get_group_value(captures, pattern.message));

        let mut from_path = running_match
            .and_then(|r| r.from_path.clone())
            .or_else(|| get_group_value(captures, pattern.from_path));
        if (from_path.is_none() || from_path.as_deref().is_some_and(|s| s.is_empty()))
            && !default_from_path.is_empty()
        {
            from_path = Some(default_from_path.to_string());
        }

        Self {
            file,
            line,
            column,
            end_line,
            end_column,
            severity,
            code,
            message,
            from_path,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum SeveritySpec {
    Capture(usize),
    Literal(String),
}

/// A single matcher pattern.
#[derive(Debug, Clone, Deserialize)]
pub struct MatcherPattern {
    pub regexp: String,
    #[serde(default)]
    pub file: Option<usize>,
    #[serde(default)]
    pub line: Option<usize>,
    #[serde(default)]
    pub column: Option<usize>,
    #[serde(default, rename = "endLine")]
    pub end_line: Option<usize>,
    #[serde(default, rename = "endColumn")]
    pub end_column: Option<usize>,
    pub severity: Option<SeveritySpec>,
    #[serde(default)]
    pub message: Option<usize>,
    #[serde(default)]
    pub code: Option<usize>,
    /// F051: Capture group index for the fromPath (base directory for relative file resolution).
    #[serde(default, rename = "fromPath")]
    pub from_path: Option<usize>,
    #[serde(rename = "loop", default)]
    pub is_loop: bool,
}

/// Problem matcher file format.
#[derive(Debug, Deserialize)]
struct MatcherFile {
    #[serde(rename = "problemMatcher")]
    problem_matcher: Vec<MatcherDefinition>,
}

#[derive(Debug, Deserialize)]
struct MatcherDefinition {
    owner: String,
    pattern: Vec<MatcherPattern>,
    /// F051: Default fromPath for the matcher (base directory for relative file paths).
    #[serde(default, rename = "fromPath")]
    from_path: Option<String>,
}

/// Registry of active problem matchers.
#[derive(Debug, Default, Clone)]
pub struct MatcherRegistry {
    matchers: HashMap<String, ProblemMatcher>,
}

impl MatcherRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a matcher from a JSON file.
    pub fn add_from_file(&mut self, path: &Path) -> Result<()> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("reading matcher file {}", path.display()))?;
        let file: MatcherFile = serde_json::from_str(&content)
            .with_context(|| format!("parsing matcher file {}", path.display()))?;
        for def in file.problem_matcher {
            validate_matcher_definition(&def)?;
            // Compile once at registration — validated above so unwrap is safe.
            let compiled_regexes: Vec<regex::Regex> = def
                .pattern
                .iter()
                .map(|p| regex::Regex::new(&p.regexp).expect("regex validated above"))
                .collect();
            debug!("Registered problem matcher: {}", def.owner);
            let pattern_len = def.pattern.len();
            let state = if pattern_len > 1 {
                vec![None; pattern_len - 1]
            } else {
                Vec::new()
            };
            self.matchers.insert(
                def.owner.clone(),
                ProblemMatcher {
                    owner: def.owner,
                    patterns: def.pattern,
                    from_path: def.from_path.unwrap_or_default(),
                    state,
                    compiled_regexes,
                },
            );
        }

        Ok(())
    }

    /// Remove a matcher by owner name.
    pub fn remove(&mut self, owner: &str) {
        self.matchers.remove(owner);
    }

    /// Match a log line against all registered matchers.
    pub fn match_line(
        &mut self,
        line: &str,
        workspace: &str,
        repository: &str,
        server_url: &str,
        translate_container_path: bool,
    ) -> Vec<crate::worker::execution_context::Annotation> {
        let mut annotations = Vec::new();
        let stripped_line = strip_ansi_codes(line);
        let match_line = stripped_line.as_deref().unwrap_or(line);

        let failsafe = std::env::var("RUNNER_TEST_GET_REPOSITORY_PATH_FAILSAFE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50);

        let host_work = if !workspace.is_empty() {
            Path::new(workspace)
                .parent()
                .and_then(|p| p.parent())
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default()
        } else {
            "".to_string()
        };

        let mut matched_owner: Option<String> = None;
        let mut matched_match: Option<PatternMatch> = None;

        for matcher in self.matchers.values_mut() {
            if matcher.patterns.is_empty() {
                continue;
            }

            if matcher.patterns.len() == 1 {
                let pattern = &matcher.patterns[0];
                let re = &matcher.compiled_regexes[0];
                if let Some(captures) = re.captures(match_line) {
                    let pm = PatternMatch::new(
                        None,
                        pattern,
                        &captures,
                        "error", // default severity
                        &matcher.from_path,
                    );
                    matched_owner = Some(matcher.owner.clone());
                    matched_match = Some(pm);
                    break;
                }
            } else {
                let num_patterns = matcher.patterns.len();
                for i in (0..num_patterns).rev() {
                    let running_match = if i > 0 {
                        matcher.state[i - 1].as_ref()
                    } else {
                        None
                    };

                    if i == 0 || running_match.is_some() {
                        let pattern = &matcher.patterns[i];
                        let is_last = i == num_patterns - 1;
                        let re = &matcher.compiled_regexes[i];
                        if let Some(captures) = re.captures(match_line) {
                            if is_last {
                                let pm = PatternMatch::new(
                                    running_match,
                                    pattern,
                                    &captures,
                                    "error", // default severity
                                    &matcher.from_path,
                                );
                                if pattern.is_loop {
                                    let saved_run = running_match.cloned();
                                    matcher.reset();
                                    matcher.state[i - 1] = saved_run;
                                } else {
                                    matcher.reset();
                                }
                                matched_owner = Some(matcher.owner.clone());
                                matched_match = Some(pm);
                                break;
                            } else {
                                let pm = PatternMatch::new(
                                    running_match,
                                    pattern,
                                    &captures,
                                    "", // default severity
                                    "", // default fromPath
                                );
                                matcher.state[i] = Some(pm);
                            }
                        } else {
                            if is_last {
                                matcher.state[i - 1] = None;
                            } else {
                                matcher.state[i] = None;
                            }
                        }
                    }
                }
                if matched_match.is_some() {
                    break;
                }
            }
        }

        if let Some(pm) = matched_match {
            let owner = matched_owner.unwrap();
            for m in self.matchers.values_mut() {
                if m.owner != owner {
                    m.reset();
                }
            }

            if let Some(mut ann) = convert_to_annotation(&pm) {
                let file = pm.file.clone().and_then(|f| {
                    let mut resolved = f.clone();

                    if !Path::new(&resolved).is_absolute() {
                        if let Some(from_path) = &pm.from_path {
                            if !from_path.is_empty() {
                                if let Some(dir) = Path::new(from_path).parent() {
                                    if !dir.as_os_str().is_empty() {
                                        resolved =
                                            dir.join(&resolved).to_string_lossy().to_string();
                                    }
                                }
                            }
                        }
                    }

                    if !Path::new(&resolved).is_absolute() {
                        if !workspace.is_empty() {
                            resolved = Path::new(workspace)
                                .join(&resolved)
                                .to_string_lossy()
                                .to_string();
                        }
                    }

                    let mut resolved = normalize_path(Path::new(&resolved))
                        .to_string_lossy()
                        .to_string();

                    if translate_container_path && !host_work.is_empty() {
                        resolved = translate_to_host_path(&resolved, &host_work);
                        resolved = normalize_path(Path::new(&resolved))
                            .to_string_lossy()
                            .to_string();
                    }

                    if workspace.is_empty() {
                        return Some(resolved);
                    }

                    let resolved_path = Path::new(&resolved);
                    if resolved_path.exists() && resolved_path.is_file() {
                        if let Some(repo_path) = get_repository_path(
                            resolved_path,
                            workspace,
                            repository,
                            server_url,
                            failsafe,
                        ) {
                            if let Ok(rel) = resolved_path.strip_prefix(&repo_path) {
                                let rel_str = rel.to_string_lossy().to_string();
                                return Some(rel_str.replace('\\', "/"));
                            }
                        }
                    }
                    None
                });

                ann.file = file;
                annotations.push(ann);
            }
        }

        annotations
    }
}

fn strip_ansi_codes(line: &str) -> Option<String> {
    if !line.as_bytes().contains(&0x1b) {
        return None;
    }

    let mut output = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            output.push(ch);
            continue;
        }

        if chars.peek() == Some(&'[') {
            chars.next();
            for next in chars.by_ref() {
                if ('@'..='~').contains(&next) {
                    break;
                }
            }
        }
    }
    Some(output)
}

fn validate_matcher_definition(def: &MatcherDefinition) -> Result<()> {
    if def.owner.trim().is_empty() {
        anyhow::bail!("Problem matcher owner is required");
    }
    if def.pattern.is_empty() {
        anyhow::bail!("Problem matcher pattern is required");
    }

    let mut file_count = 0;
    let mut line_count = 0;
    let mut col_count = 0;
    let mut sev_count = 0;
    let mut code_count = 0;
    let mut msg_count = 0;
    let mut from_count = 0;

    let mut has_message = false;
    for (idx, pattern) in def.pattern.iter().enumerate() {
        let is_first = idx == 0;
        let is_last = idx == def.pattern.len() - 1;

        if pattern.is_loop {
            if def.pattern.len() == 1 {
                anyhow::bail!("Problem matcher loop may not be set on a single pattern");
            }
            if is_first || !is_last {
                anyhow::bail!("Problem matcher loop is only allowed on the last pattern");
            }
            if pattern.message.is_none() {
                anyhow::bail!("The loop pattern must set 'message'");
            }
        }
        if pattern.message.is_some() {
            has_message = true;
        }

        if pattern.file.is_some() {
            file_count += 1;
        }
        if pattern.line.is_some() {
            line_count += 1;
        }
        if pattern.column.is_some() {
            col_count += 1;
        }
        if pattern.severity.is_some() {
            sev_count += 1;
        }
        if pattern.code.is_some() {
            code_count += 1;
        }
        if pattern.message.is_some() {
            msg_count += 1;
        }
        if pattern.from_path.is_some() {
            from_count += 1;
        }

        // Regex group count check
        let re = regex::Regex::new(&pattern.regexp).map_err(|e| {
            anyhow::anyhow!("Invalid regular expression '{}': {}", pattern.regexp, e)
        })?;
        let groups = re.captures_len();

        let check_range = |name: &str, val: Option<usize>| -> Result<()> {
            if let Some(idx) = val {
                if idx >= groups {
                    anyhow::bail!(
                        "The property '{}' is set to {} which is out of range",
                        name,
                        idx
                    );
                }
            }
            Ok(())
        };

        check_range("file", pattern.file)?;
        check_range("line", pattern.line)?;
        check_range("column", pattern.column)?;
        check_range("endLine", pattern.end_line)?;
        check_range("endColumn", pattern.end_column)?;
        check_range("code", pattern.code)?;
        check_range("message", pattern.message)?;
        check_range("fromPath", pattern.from_path)?;
        if let Some(SeveritySpec::Capture(idx)) = pattern.severity {
            check_range("severity", Some(idx))?;
        }
    }

    if file_count > 1 {
        anyhow::bail!("The property 'file' is set twice");
    }
    if line_count > 1 {
        anyhow::bail!("The property 'line' is set twice");
    }
    if col_count > 1 {
        anyhow::bail!("The property 'column' is set twice");
    }
    if sev_count > 1 {
        anyhow::bail!("The property 'severity' is set twice");
    }
    if code_count > 1 {
        anyhow::bail!("The property 'code' is set twice");
    }
    if msg_count > 1 {
        anyhow::bail!("The property 'message' is set twice");
    }
    if from_count > 1 {
        anyhow::bail!("The property 'fromPath' is set twice");
    }

    if !has_message {
        anyhow::bail!("Problem matcher pattern message is required");
    }
    Ok(())
}

fn convert_to_annotation(
    pm: &PatternMatch,
) -> Option<crate::worker::execution_context::Annotation> {
    let message = pm.message.clone()?;
    if message.trim().is_empty() {
        return None;
    }

    let level = match pm.severity.as_deref().map(|s| s.to_lowercase()) {
        Some(ref s) if s == "warning" => crate::worker::execution_context::AnnotationLevel::Warning,
        Some(ref s) if s == "notice" => crate::worker::execution_context::AnnotationLevel::Notice,
        _ => crate::worker::execution_context::AnnotationLevel::Error,
    };

    let line = pm.line.as_deref().and_then(|s| s.parse().ok());
    let col = pm.column.as_deref().and_then(|s| s.parse().ok());
    let end_line = pm.end_line.as_deref().and_then(|s| s.parse().ok());
    let end_column = pm.end_column.as_deref().and_then(|s| s.parse().ok());

    Some(crate::worker::execution_context::Annotation {
        level,
        message,
        title: None,
        file: None,
        line,
        end_line,
        col,
        end_column,
    })
}

fn normalize_path(path: &Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut components = path.components().peekable();
    let mut ret = std::path::PathBuf::new();
    if let Some(c @ Component::Prefix(..)) = components.peek() {
        ret.push(c.as_os_str());
        components.next();
    }
    if let Some(c @ Component::RootDir) = components.peek() {
        ret.push(c.as_os_str());
        components.next();
    }
    for component in components {
        match component {
            Component::Prefix(..) => {}
            Component::RootDir => {}
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

pub fn translate_to_host_path(container_path: &str, host_work: &str) -> String {
    if let Some(relative) = container_path.strip_prefix("/__w") {
        let mut path = std::path::PathBuf::from(host_work);
        path.push(relative.trim_start_matches('/'));
        path.to_string_lossy().to_string()
    } else {
        container_path.to_string()
    }
}

fn get_url_host(server_url: &str) -> String {
    let mut s = server_url;
    if let Some(rest) = s.strip_prefix("https://") {
        s = rest;
    } else if let Some(rest) = s.strip_prefix("http://") {
        s = rest;
    }
    s.split('/')
        .next()
        .unwrap_or(s)
        .split(':')
        .next()
        .unwrap_or(s)
        .to_string()
}

fn get_repository_path(
    file_path: &Path,
    _workspace: &str,
    repository: &str,
    server_url: &str,
    failsafe: usize,
) -> Option<std::path::PathBuf> {
    let mut current = file_path.parent()?;
    let mut recursion = 0;

    let host = get_url_host(server_url);
    let patterns = vec![
        format!("url = {}/{}", server_url.trim_end_matches('/'), repository),
        format!("url = git@{}:{}.git", host, repository),
    ];

    while recursion <= failsafe {
        if current.as_os_str().is_empty() {
            break;
        }

        let git_config_path = current.join(".git").join("config");
        if git_config_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&git_config_path) {
                for line in content.lines() {
                    let trimmed = line.trim();
                    for pattern in &patterns {
                        if trimmed.eq_ignore_ascii_case(pattern) {
                            return Some(current.to_path_buf());
                        }
                    }
                }
            }
        }

        if let Some(parent) = current.parent() {
            current = parent;
            recursion += 1;
        } else {
            break;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::execution_context::AnnotationLevel;

    #[test]
    fn matcher_accepts_literal_severity() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "mega",
                "pattern": [{
                  "regexp": "^MEGA_ERROR ([^:]+):(\\d+):(\\d+): (.*)$",
                  "file": 1,
                  "line": 2,
                  "column": 3,
                  "message": 4,
                  "severity": "error"
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        registry.add_from_file(&path).unwrap();

        let annotations =
            registry.match_line("MEGA_ERROR sample.rs:12:34: boom", "", "", "", false);
        assert_eq!(annotations.len(), 1);
        assert_eq!(annotations[0].level, AnnotationLevel::Error);
        assert_eq!(annotations[0].file.as_deref(), Some("sample.rs"));
        assert_eq!(annotations[0].line, Some(12));
        assert_eq!(annotations[0].col, Some(34));
        assert_eq!(annotations[0].message, "boom");
    }

    #[test]
    fn matcher_strips_ansi_color_codes_before_matching() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "ansi",
                "pattern": [{
                  "regexp": "^ERR ([^:]+):(\\d+): (.*)$",
                  "file": 1,
                  "line": 2,
                  "message": 3,
                  "severity": "warning"
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        registry.add_from_file(&path).unwrap();

        let annotations = registry.match_line(
            "\u{1b}[31mERR src/lib.rs:7: red boom\u{1b}[0m",
            "",
            "",
            "",
            false,
        );

        assert_eq!(annotations.len(), 1);
        assert_eq!(annotations[0].level, AnnotationLevel::Warning);
        assert_eq!(annotations[0].file.as_deref(), Some("src/lib.rs"));
        assert_eq!(annotations[0].line, Some(7));
        assert_eq!(annotations[0].message, "red boom");
    }

    #[test]
    fn matcher_owner_can_be_removed() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "removable",
                "pattern": [{
                  "regexp": "^ERR (.*)$",
                  "message": 1
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        registry.add_from_file(&path).unwrap();
        assert_eq!(registry.match_line("ERR boom", "", "", "", false).len(), 1);

        registry.remove("removable");

        assert!(registry
            .match_line("ERR boom", "", "", "", false)
            .is_empty());
    }

    #[test]
    fn test_multi_pattern_matching_lifecycle() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "multiline",
                "pattern": [
                  {
                    "regexp": "^Start: (.*)$"
                  },
                  {
                    "regexp": "^Middle: (.*)$"
                  },
                  {
                    "regexp": "^End: (.*)$",
                    "message": 1
                  }
                ]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        registry.add_from_file(&path).unwrap();

        assert!(registry
            .match_line("Start: hello", "", "", "", false)
            .is_empty());
        assert!(registry
            .match_line("Middle: world", "", "", "", false)
            .is_empty());
        let anns = registry.match_line("End: final", "", "", "", false);
        assert_eq!(anns.len(), 1);
        assert_eq!(anns[0].message, "final");

        assert!(registry
            .match_line("Start: hello", "", "", "", false)
            .is_empty());
        assert!(registry
            .match_line("Other line", "", "", "", false)
            .is_empty());
        assert!(registry
            .match_line("Middle: world", "", "", "", false)
            .is_empty());
        assert!(registry
            .match_line("End: final", "", "", "", false)
            .is_empty());
    }

    #[test]
    fn test_multi_pattern_matching_with_loop() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "looping",
                "pattern": [
                  {
                    "regexp": "^Start: (.*)$"
                  },
                  {
                    "regexp": "^End: (.*)$",
                    "message": 1,
                    "loop": true
                  }
                ]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        registry.add_from_file(&path).unwrap();

        assert!(registry
            .match_line("Start: hello", "", "", "", false)
            .is_empty());
        let anns1 = registry.match_line("End: first", "", "", "", false);
        assert_eq!(anns1.len(), 1);
        assert_eq!(anns1[0].message, "first");

        let anns2 = registry.match_line("End: second", "", "", "", false);
        assert_eq!(anns2.len(), 1);
        assert_eq!(anns2[0].message, "second");

        assert!(registry
            .match_line("Other line", "", "", "", false)
            .is_empty());
        assert!(registry
            .match_line("End: third", "", "", "", false)
            .is_empty());
    }

    #[test]
    fn test_repository_path_resolution() {
        let dir = tempfile::TempDir::new().unwrap();
        let workspace = dir.path().join("workspace");
        let repo_dir = workspace.join("my-repo");
        std::fs::create_dir_all(&repo_dir).unwrap();

        let status = std::process::Command::new("git")
            .arg("init")
            .current_dir(&repo_dir)
            .status();
        if status.is_ok() {
            let config_content = "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = false\n\tlogallrefupdates = true\n[remote \"origin\"]\n\turl = https://github.com/my-org/my-repo\n";
            std::fs::write(repo_dir.join(".git").join("config"), config_content).unwrap();
        }

        let file_path = repo_dir.join("subdir").join("test-file.txt");
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        std::fs::write(&file_path, "boom").unwrap();

        let matcher_path = dir.path().join("matcher.json");
        std::fs::write(
            &matcher_path,
            r#"{
              "problemMatcher": [{
                "owner": "path-test",
                "pattern": [{
                  "regexp": "^ERROR: (.*)$",
                  "file": 1,
                  "message": 1
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        registry.add_from_file(&matcher_path).unwrap();

        let line = format!("ERROR: {}", file_path.to_string_lossy());
        let anns = registry.match_line(
            &line,
            &workspace.to_string_lossy(),
            "my-org/my-repo",
            "https://github.com",
            false,
        );

        assert_eq!(anns.len(), 1);
        if status.is_ok() {
            assert_eq!(anns[0].file.as_deref(), Some("subdir/test-file.txt"));
        }
    }
    #[test]
    fn matcher_validation_requires_message() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "bad",
                "pattern": [{
                  "regexp": "^ERR (.*)$"
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        let err = registry.add_from_file(&path).unwrap_err();
        assert!(err.to_string().contains("message is required"));
    }

    #[test]
    fn matcher_validation_rejects_loop_on_single_pattern() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "bad",
                "pattern": [{
                  "regexp": "^ERR (.*)$",
                  "message": 1,
                  "loop": true
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        let err = registry.add_from_file(&path).unwrap_err();
        assert!(err.to_string().contains("loop may not be set"));
    }

    #[test]
    fn matcher_validation_rejects_loop_before_last_pattern() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "bad",
                "pattern": [
                  {
                    "regexp": "^ERR (.*)$",
                    "message": 1,
                    "loop": true
                  },
                  {
                    "regexp": "^(.*)$",
                    "message": 1
                  }
                ]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        let err = registry.add_from_file(&path).unwrap_err();
        assert!(err.to_string().contains("only allowed on the last"));
    }
    #[test]
    fn matcher_validation_rejects_property_set_twice() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "bad",
                "pattern": [
                  {
                    "regexp": "^(file1): (.*)$",
                    "file": 1
                  },
                  {
                    "regexp": "^(file2): (.*)$",
                    "file": 1,
                    "message": 2
                  }
                ]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        let err = registry.add_from_file(&path).unwrap_err();
        assert!(err.to_string().contains("property 'file' is set twice"));
    }

    #[test]
    fn matcher_validation_rejects_property_out_of_range() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "bad",
                "pattern": [{
                  "regexp": "^(.+)$",
                  "message": 2
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        let err = registry.add_from_file(&path).unwrap_err();
        assert!(err
            .to_string()
            .contains("property 'message' is set to 2 which is out of range"));
    }

    #[test]
    fn matcher_validation_requires_owner() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "  ",
                "pattern": [{
                  "regexp": "^ERR (.*)$",
                  "message": 1
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        let err = registry.add_from_file(&path).unwrap_err();
        assert!(err.to_string().contains("owner is required"));
    }

    #[test]
    fn matcher_validation_requires_pattern() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "bad",
                "pattern": []
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        let err = registry.add_from_file(&path).unwrap_err();
        assert!(err.to_string().contains("pattern is required"));
    }

    // --- P0 matcher gap coverage ---

    #[test]
    fn matcher_owner_clobber_replaces_old() {
        let dir = tempfile::TempDir::new().unwrap();
        let path1 = dir.path().join("matcher1.json");
        std::fs::write(
            &path1,
            r#"{
              "problemMatcher": [{
                "owner": "same-owner",
                "pattern": [{
                  "regexp": "^OLD (.*)$",
                  "message": 1
                }]
              }]
            }"#,
        )
        .unwrap();

        let path2 = dir.path().join("matcher2.json");
        std::fs::write(
            &path2,
            r#"{
              "problemMatcher": [{
                "owner": "same-owner",
                "pattern": [{
                  "regexp": "^NEW (.*)$",
                  "message": 1
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        registry.add_from_file(&path1).unwrap();
        assert_eq!(registry.match_line("OLD first", "", "", "", false).len(), 1);

        // Adding same owner replaces
        registry.add_from_file(&path2).unwrap();
        assert!(registry
            .match_line("OLD first", "", "", "", false)
            .is_empty());
        assert_eq!(
            registry.match_line("NEW second", "", "", "", false).len(),
            1
        );
    }

    #[test]
    fn matcher_dynamic_severity_from_regex_group() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "dyn-sev",
                "pattern": [{
                  "regexp": "^(error|warning|notice): (.*)$",
                  "severity": 1,
                  "message": 2
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        registry.add_from_file(&path).unwrap();

        let warns = registry.match_line("warning: deprecated API", "", "", "", false);
        assert_eq!(warns.len(), 1);
        assert_eq!(warns[0].level, AnnotationLevel::Warning);
        assert_eq!(warns[0].message, "deprecated API");

        let errors = registry.match_line("error: compilation failed", "", "", "", false);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].level, AnnotationLevel::Error);

        let notices = registry.match_line("notice: FYI", "", "", "", false);
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].level, AnnotationLevel::Notice);
    }

    #[test]
    fn matcher_captures_end_line_and_end_column() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "range",
                "pattern": [{
                  "regexp": "^(.+):(\\d+):(\\d+)-(\\d+):(\\d+): (.*)$",
                  "file": 1,
                  "line": 2,
                  "column": 3,
                  "endLine": 4,
                  "endColumn": 5,
                  "message": 6,
                  "severity": "warning"
                }]
              }]
            }"#,
        )
        .unwrap();

        let mut registry = MatcherRegistry::new();
        registry.add_from_file(&path).unwrap();

        let anns = registry.match_line("src/lib.rs:10:5-12:20: unused variable", "", "", "", false);
        assert_eq!(anns.len(), 1);
        assert_eq!(anns[0].file.as_deref(), Some("src/lib.rs"));
        assert_eq!(anns[0].line, Some(10));
        assert_eq!(anns[0].col, Some(5));
        assert_eq!(anns[0].end_line, Some(12));
        assert_eq!(anns[0].end_column, Some(20));
        assert_eq!(anns[0].message, "unused variable");
        assert_eq!(anns[0].level, AnnotationLevel::Warning);
    }
}
