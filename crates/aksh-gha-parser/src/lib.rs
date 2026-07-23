//! Typed GitHub Actions workflow parser and job expander.

pub use aksh_gha_protocol::{JobPlan, StepPlan};

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

pub use expand::{expand_jobs, expand_jobs_with_reusables, expand_jobs_with_reusables_and_shas};
pub use models::*;
pub use yaml::{parse_action_metadata, parse_workflow};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
