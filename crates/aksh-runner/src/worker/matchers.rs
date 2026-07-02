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
    #[serde(default)]
    pub severity: Option<usize>,
    #[serde(default)]
    pub message: Option<usize>,
    #[serde(default)]
    pub code: Option<usize>,
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
}

/// Registry of active problem matchers.
#[derive(Debug, Default)]
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

                        let file = pattern
                            .file
                            .and_then(|g| captures.get(g))
                            .map(|m| m.as_str().to_string());

                        let line_num = pattern
                            .line
                            .and_then(|g| captures.get(g))
                            .and_then(|m| m.as_str().parse().ok());

                        let col = pattern
                            .column
                            .and_then(|g| captures.get(g))
                            .and_then(|m| m.as_str().parse().ok());

                        let severity = pattern
                            .severity
                            .and_then(|g| captures.get(g))
                            .map(|m| m.as_str());

                        let level = match severity {
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
