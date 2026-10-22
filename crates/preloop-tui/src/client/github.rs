//! GitHub access through the `gh` CLI, for the stack view.
//!
//! Using `gh api graphql` keeps credentials in the user's existing `gh` login.
//! The TUI never sees a GitHub token.

use anyhow::{Context as _, bail};
use serde_json::Value;

/// Run a GraphQL query with `gh api graphql` and return the parsed response.
///
/// `variables` are sent as string fields (`-f name=value`).
pub async fn graphql(query: &str, variables: &[(&str, &str)]) -> anyhow::Result<Value> {
    let mut command = tokio::process::Command::new("gh");
    command
        .arg("api")
        .arg("graphql")
        .arg("-f")
        .arg(format!("query={query}"));
    for (name, value) in variables {
        command.arg("-f").arg(format!("{name}={value}"));
    }
    let output = command
        .output()
        .await
        .context("running `gh api graphql` (is the GitHub CLI installed?)")?;
    if !output.status.success() {
        bail!(
            "`gh api graphql` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    serde_json::from_slice(&output.stdout).context("parsing `gh api graphql` output")
}
