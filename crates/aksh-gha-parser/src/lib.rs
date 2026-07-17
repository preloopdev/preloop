//! Typed GitHub Actions workflow parser and job expander.

use std::collections::BTreeMap;

use aksh_gha_protocol::JobId;
pub use aksh_gha_protocol::{JobPlan, StepPlan};
use serde_json::Value;

/// Workflow dependency graph validation.
pub mod dag;
/// Expression evaluation for workflow fields.
pub mod eval;
/// Build `AgentJobRequestMessage` from parsed workflow data.
pub mod job_builder;

mod expand;
mod matrix_expand;
mod models;
mod trigger;
mod yaml;

pub(crate) use expand::coerce_value;
pub use expand::{expand_jobs, expand_jobs_with_reusables};
pub use models::*;
pub(crate) use trigger::{glob_match, matches_filter, matches_filter_with_default};
pub use yaml::{parse_action_metadata, parse_workflow};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
