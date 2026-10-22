//! `tui.toml`: the optional file of named contexts and stack settings.
//!
//! The file only names things. Reading token files and resolving environment
//! variables is the CLI's job, so this module never touches a secret.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, bail};
use serde::Deserialize;

/// Parsed `tui.toml`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct TuiConfig {
    /// The `[[context]]` tables, in file order.
    #[serde(rename = "context")]
    pub contexts: Vec<ContextConfig>,
    /// The `[stack]` table.
    pub stack: StackConfig,
}

/// One `[[context]]` table. Only names and locations, never secret values.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ContextConfig {
    /// Unique display name.
    pub name: String,
    /// Engine base URL. Defaults to `PRELOOP_URL` or `http://127.0.0.1:9090` when absent.
    pub engine_url: Option<String>,
    /// Environment variable holding the engine token.
    pub engine_token_env: Option<String>,
    /// File holding the engine token.
    pub engine_token_file: Option<PathBuf>,
    /// Sandbox Service URL. Absent means no sandbox target for this context.
    pub sandbox_url: Option<String>,
    /// File holding this context's sandbox token. Required for remote sandboxes.
    pub sandbox_token_file: Option<PathBuf>,
    /// File holding this context's sandbox CA PEM.
    pub sandbox_ca_file: Option<PathBuf>,
}

/// The `[stack]` table, used by the stack view.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StackConfig {
    /// Repositories to scan, as `owner/name`.
    pub repos: Vec<String>,
    /// Only show pull requests opened by this login.
    pub author: Option<String>,
    /// Extra mirror edges as `(parent, child)` pull request references.
    pub extra_edges: Vec<(String, String)>,
}

/// Parse and validate the text of a `tui.toml`.
pub fn parse(text: &str) -> anyhow::Result<TuiConfig> {
    let config: TuiConfig = toml::from_str(text).context("parsing tui.toml")?;
    let mut names = BTreeSet::new();
    for context in &config.contexts {
        if context.name.trim().is_empty() {
            bail!("a [[context]] table has no name");
        }
        if !names.insert(context.name.as_str()) {
            bail!("duplicate context name {:?} in tui.toml", context.name);
        }
        if context.engine_token_env.is_some() && context.engine_token_file.is_some() {
            bail!(
                "context {:?} sets both engine_token_env and engine_token_file",
                context.name
            );
        }
    }
    Ok(config)
}

/// Load `path`. `Ok(None)` when the file does not exist.
pub fn load(path: &Path) -> anyhow::Result<Option<TuiConfig>> {
    match std::fs::read_to_string(path) {
        Ok(text) => parse(&text)
            .with_context(|| format!("reading {}", path.display()))
            .map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_contexts_and_stack() {
        let config = parse(
            r##"
            [[context]]
            name = "cpane"
            engine_url = "https://preloop.example"
            engine_token_env = "CPANE_TOKEN"
            sandbox_url = "https://10.0.0.5:9191"
            sandbox_token_file = "/secrets/sbx"

            [[context]]
            name = "local"

            [stack]
            repos = ["preloopdev/preloop"]
            author = "octocat"
            extra_edges = [["#619", "#685"]]
            "##,
        )
        .expect("valid config");

        assert_eq!(config.contexts.len(), 2);
        assert_eq!(config.contexts[0].name, "cpane");
        assert_eq!(config.contexts[0].engine_token_env.as_deref(), Some("CPANE_TOKEN"));
        assert_eq!(config.contexts[1].engine_url, None);
        assert_eq!(config.stack.repos, vec!["preloopdev/preloop".to_owned()]);
        assert_eq!(
            config.stack.extra_edges,
            vec![("#619".to_owned(), "#685".to_owned())]
        );
    }

    #[test]
    fn rejects_duplicate_names() {
        let error = parse("[[context]]\nname = \"a\"\n[[context]]\nname = \"a\"\n")
            .expect_err("duplicate names must fail");
        assert!(error.to_string().contains("duplicate context name"), "{error}");
    }

    #[test]
    fn rejects_two_token_sources() {
        let error = parse(
            "[[context]]\nname = \"a\"\nengine_token_env = \"X\"\nengine_token_file = \"/x\"\n",
        )
        .expect_err("two token sources must fail");
        assert!(error.to_string().contains("both engine_token_env"), "{error}");
    }

    #[test]
    fn missing_file_is_none() {
        let path = std::env::temp_dir().join("preloop-tui-missing-config-test.toml");
        let _ = std::fs::remove_file(&path);
        assert_eq!(load(&path).expect("missing file is not an error"), None);
    }
}
