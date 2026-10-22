#![allow(missing_docs)]

//! Lenient GitHub pull-request mirrors used by the Stack view.

use serde::{Deserialize, Serialize};

/// Pull-request check counts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckSummary {
    pub total: u32,
    pub passing: u32,
    pub failing: u32,
    pub pending: u32,
}

impl CheckSummary {
    /// A compact status suitable for a table cell.
    pub fn label(&self) -> String {
        if self.total == 0 {
            return "not loaded".to_owned();
        }
        format!("{}/{} checks", self.passing, self.total)
    }

    /// Status glyph and tone source for the renderer.
    pub fn glyph(&self) -> &'static str {
        if self.failing > 0 {
            "✗"
        } else if self.pending > 0 {
            "◌"
        } else if self.total > 0 && self.passing == self.total {
            "✓"
        } else {
            "?"
        }
    }
}

/// One pull request returned by GitHub.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackPullRequest {
    pub repository: String,
    pub number: u64,
    pub title: String,
    pub url: String,
    pub author: Option<String>,
    pub base: String,
    pub head: String,
    pub base_sha: Option<String>,
    pub head_sha: Option<String>,
    pub draft: bool,
    pub state: String,
    pub mergeable: Option<String>,
    pub checks: CheckSummary,
}

impl StackPullRequest {
    /// Stable reference accepted by `extra_edges` (`#42` or `owner/repo#42`).
    pub fn reference(&self) -> String {
        format!("{}#{}", self.repository, self.number)
    }

    /// Whether a config reference identifies this pull request.
    pub fn matches_reference(&self, reference: &str) -> bool {
        let trimmed = reference.trim();
        trimmed == format!("#{}", self.number)
            || trimmed.eq_ignore_ascii_case(&self.reference())
            || trimmed.eq_ignore_ascii_case(&format!("{}#{}", self.repository, self.number))
    }
}

/// Pull requests loaded for one configured repository.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackRepository {
    pub name: String,
    pub default_branch: Option<String>,
    pub pull_requests: Vec<StackPullRequest>,
    /// A fetch or parse failure is shown verbatim instead of fabricating rows.
    pub error: Option<String>,
}

/// The complete loaded stack snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackSnapshot {
    pub repositories: Vec<StackRepository>,
}
