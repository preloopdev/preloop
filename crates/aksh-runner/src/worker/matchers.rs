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
            debug!("Registered problem matcher: {}", def.owner);
            self.matchers.insert(
                def.owner.clone(),
                ProblemMatcher {
                    owner: def.owner,
                    patterns: def.pattern,
                    from_path: def.from_path.unwrap_or_default(),
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
    pub fn match_line(&self, line: &str) -> Vec<crate::worker::execution_context::Annotation> {
        let mut annotations = Vec::new();

        for matcher in self.matchers.values() {
            // Only handle single-pattern matchers for now
            if let Some(pattern) = matcher.patterns.first() {
                if let Ok(re) = regex::Regex::new(&pattern.regexp) {
                    if let Some(captures) = re.captures(line) {
                        let message = pattern
                            .message
                            .and_then(|g| captures.get(g))
                            .map(|m| m.as_str().to_string())
                            .unwrap_or_else(|| line.to_string());

                        // F051: Extract file path and fromPath from captures
                        let raw_file = pattern
                            .file
                            .and_then(|g| captures.get(g))
                            .map(|m| m.as_str().to_string());

                        // F051: Resolve relative file paths using fromPath
                        let file = raw_file.map(|f| {
                            // If file is already absolute, use as-is
                            if Path::new(&f).is_absolute() {
                                return f;
                            }
                            // Try pattern-level fromPath capture group first
                            let from_path = pattern
                                .from_path
                                .and_then(|g| captures.get(g))
                                .map(|m| m.as_str().to_string())
                                .unwrap_or_else(|| matcher.from_path.clone());
                            // Resolve relative file against fromPath directory
                            if !from_path.is_empty() {
                                if let Some(dir) = Path::new(&from_path).parent() {
                                    if !dir.as_os_str().is_empty() {
                                        return dir.join(&f).to_string_lossy().to_string();
                                    }
                                }
                            }
                            f
                        });

                        let line_num = pattern
                            .line
                            .and_then(|g| captures.get(g))
                            .and_then(|m| m.as_str().parse().ok());

                        let col = pattern
                            .column
                            .and_then(|g| captures.get(g))
                            .and_then(|m| m.as_str().parse().ok());

                        let severity = match &pattern.severity {
                            Some(SeveritySpec::Capture(g)) => {
                                captures.get(*g).map(|m| m.as_str().to_string())
                            }
                            Some(SeveritySpec::Literal(value)) => Some(value.to_string()),
                            None => None,
                        };

                        let level = match severity.as_deref() {
                            Some("warning") => {
                                crate::worker::execution_context::AnnotationLevel::Warning
                            }
                            Some("notice") => {
                                crate::worker::execution_context::AnnotationLevel::Notice
                            }
                            _ => crate::worker::execution_context::AnnotationLevel::Error,
                        };

                        annotations.push(crate::worker::execution_context::Annotation {
                            level,
                            message,
                            title: None,
                            file,
                            line: line_num,
                            end_line: None,
                            col,
                            end_column: None,
                        });
                    }
                }
            }
        }

        annotations
    }
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

        let annotations = registry.match_line("MEGA_ERROR sample.rs:12:34: boom");
        assert_eq!(annotations.len(), 1);
        assert_eq!(annotations[0].level, AnnotationLevel::Error);
        assert_eq!(annotations[0].file.as_deref(), Some("sample.rs"));
        assert_eq!(annotations[0].line, Some(12));
        assert_eq!(annotations[0].col, Some(34));
        assert_eq!(annotations[0].message, "boom");
    }
}
