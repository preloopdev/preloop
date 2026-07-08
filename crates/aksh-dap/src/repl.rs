//! DAP REPL parser and executor.
//!
//! 1:1 port of `src/Runner.Worker/Dap/DapReplParser.cs` and
//! `src/Runner.Worker/Dap/DapReplExecutor.cs`.
//!
//! The runner's debugger exposes a tiny DSL the user can `evaluate`
//! against the current step context:
//!
//! - `help` / `help("run")` — return human-readable help text.
//! - `run("echo hello")` — execute a shell command in the job's
//!   runtime context. Output is streamed back to the editor via
//!   DAP `output` events with secrets masked.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// All REPL DSL commands. Mirrors `DapReplCommand` upstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum DapReplCommand {
    /// `help` or `help("run")`.
    Help { topic: Option<String> },
    /// `run("echo hello")` or
    /// `run("echo hello", shell: "bash", env: { FOO: "bar" }, working_directory: "/tmp")`.
    Run(RunCommand),
}

/// `help` or `help("run")`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HelpCommand {
    /// Optional topic — `"run"`, `"help"`, or `None` for the index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

/// `run("echo hello")` or
/// `run("echo hello", shell: "bash", env: { FOO: "bar" }, working_directory: "/tmp")`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCommand {
    /// The script to execute.
    pub script: String,
    /// Optional shell override (defaults to the runner's configured shell).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell: Option<String>,
    /// Optional additional environment variables.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Optional working directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_directory: Option<String>,
}

impl RunCommand {
    /// Total env-var count, including the job's runtime context. Used
    /// by the executor to decide whether to log a "large env" warning.
    pub fn env_size(&self) -> usize {
        self.env.len()
    }
}

/// Errors from the parser.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    /// Empty input.
    #[error("empty input")]
    Empty,
    /// Unknown command.
    #[error("unknown command: {0}")]
    UnknownCommand(String),
    /// Missing required argument.
    #[error("missing argument: {0}")]
    MissingArgument(&'static str),
    /// Unparseable argument list.
    #[error("malformed arguments: {0}")]
    MalformedArguments(String),
}

/// The REPL parser. Mirrors `DapReplParser.cs::Parse`.
///
/// Accepts the textual command (without the surrounding quotes
/// that the `evaluate` request body wraps it in) and returns a
/// [`DapReplCommand`].
pub struct DapReplParser;

impl DapReplParser {
    /// Parse the input. Whitespace-trimmed; empty input is an error.
    pub fn parse(input: &str) -> Result<DapReplCommand, ParseError> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(ParseError::Empty);
        }

        // `help` / `help("topic")` / `help("topic", key: val)`
        if trimmed == "help" {
            return Ok(DapReplCommand::Help { topic: None });
        }
        if let Some(rest) = trimmed.strip_prefix("help(") {
            let inner = rest.trim_end_matches(')').trim();
            return Ok(DapReplCommand::Help {
                topic: parse_string_literal(inner),
            });
        }

        // `run("script", ...)` — only `run` is supported in the
        // first argument position; everything else is rejected.
        if let Some(rest) = trimmed.strip_prefix("run(") {
            let args = rest.trim_end_matches(')').trim();
            return parse_run(args).map(DapReplCommand::Run);
        }

        Err(ParseError::UnknownCommand(trimmed.split_whitespace().next().unwrap_or("").to_string()))
    }
}

fn parse_string_literal(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Accept double-quoted, single-quoted, or bareword.
    if (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
        || (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
    {
        Some(s[1..s.len() - 1].to_string())
    } else {
        Some(s.to_string())
    }
}

fn parse_run(args: &str) -> Result<RunCommand, ParseError> {
    // Very small ad-hoc parser: first positional is the script,
    // then optional `key: value` pairs separated by commas.
    let mut iter = split_args(args).into_iter();
    let first = iter.next().ok_or(ParseError::MissingArgument("script"))?;
    let script = parse_string_literal(&first)
        .ok_or_else(|| ParseError::MalformedArguments("first argument must be a string".into()))?;

    let mut command = RunCommand {
        script,
        shell: None,
        env: BTreeMap::new(),
        working_directory: None,
    };

    for pair in iter {
        let (key, value) = pair
            .split_once(':')
            .ok_or_else(|| ParseError::MalformedArguments(format!("expected key: value, got {pair}")))?;
        let key = key.trim();
        let value = value.trim();
        match key {
            "shell" => {
                command.shell = parse_string_literal(value);
            }
            "working_directory" | "workingDirectory" | "cwd" => {
                command.working_directory = parse_string_literal(value);
            }
            "env" => {
                command.env = parse_env_map(value)?;
            }
            other => {
                return Err(ParseError::MalformedArguments(format!("unknown key: {other}")));
            }
        }
    }

    Ok(command)
}

fn parse_env_map(s: &str) -> Result<BTreeMap<String, String>, ParseError> {
    // Expect `{ FOO: "bar", BAZ: 'qux' }` or `{ FOO: bar }`.
    let trimmed = s.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|x| x.strip_suffix('}'))
        .ok_or_else(|| ParseError::MalformedArguments("env must be a brace-enclosed map".into()))?;
    let mut map = BTreeMap::new();
    for entry in split_args(inner) {
        let (k, v) = entry
            .split_once(':')
            .ok_or_else(|| ParseError::MalformedArguments(format!("env entry must be key: value, got {entry}")))?;
        let key = k.trim().to_string();
        let value = parse_string_literal(v.trim())
            .ok_or_else(|| ParseError::MalformedArguments(format!("env value for {key} must be a string")))?;
        map.insert(key, value);
    }
    Ok(map)
}

/// Split a comma-separated argument list, respecting quoted strings
/// and brace nesting. This is a small, deliberately limited splitter
/// — it does not need to be a full expression parser.
fn split_args(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_str: Option<char> = None;
    let mut brace_depth: i32 = 0;
    for c in input.chars() {
        match c {
            '"' | '\'' if in_str.is_none() => {
                in_str = Some(c);
                buf.push(c);
            }
            '"' | '\'' if in_str == Some(c) => {
                in_str = None;
                buf.push(c);
            }
            '{' if in_str.is_none() => {
                brace_depth += 1;
                buf.push(c);
            }
            '}' if in_str.is_none() => {
                brace_depth -= 1;
                buf.push(c);
            }
            ',' if in_str.is_none() && brace_depth == 0 => {
                out.push(buf.trim().to_string());
                buf.clear();
            }
            other => buf.push(other),
        }
    }
    let tail = buf.trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

/// The REPL executor. The runner wires this up to a real
/// `ISecretMasker` and a process launcher. The default impl in
/// this crate is a no-op shell (it does not actually execute the
/// command) so the parser and the rest of the DAP surface can be
/// tested in isolation. See [`DapReplExecutor::with_launcher`] to
/// plug in a real launcher.
pub struct DapReplExecutor {
    masker: Box<dyn Fn(&str) -> String + Send + Sync>,
    launcher: Box<dyn Fn(&RunCommand) -> Result<String, std::io::Error> + Send + Sync>,
}

impl Default for DapReplExecutor {
    fn default() -> Self {
        Self {
            masker: Box::new(|s| s.to_string()),
            launcher: Box::new(|_| Ok(String::new())),
        }
    }
}

impl DapReplExecutor {
    /// Build an executor with a custom secret masker.
    pub fn with_masker(masker: Box<dyn Fn(&str) -> String + Send + Sync>) -> Self {
        Self {
            masker,
            launcher: Box::new(|_| Ok(String::new())),
        }
    }

    /// Replace the process launcher. The closure receives the parsed
    /// `run` command and returns the captured stdout (already
    /// masked by the caller's masker if they wish). Returning
    /// `Err` surfaces as an `output` event with `category=stderr`.
    pub fn with_launcher(
        mut self,
        launcher: Box<dyn Fn(&RunCommand) -> Result<String, std::io::Error> + Send + Sync>,
    ) -> Self {
        self.launcher = launcher;
        self
    }

    /// Execute a parsed REPL command. Returns the (already-masked)
    /// output to be sent back to the editor as one or more `output`
    /// events. The `help` branch never invokes the launcher.
    pub fn execute(&self, command: &DapReplCommand) -> String {
        match command {
            DapReplCommand::Help { topic } => {
                let topic = topic.as_deref().unwrap_or("");
                match topic {
                    "" => "Available commands:\n  help [topic]\n  run(\"script\" [, shell: \"bash\"] [, env: { ... }] [, working_directory: \"/path\"])\n".to_string(),
                    "run" => "run(\"script\"): execute a shell command in the job's runtime context. Output is streamed back via output events.".to_string(),
                    "help" => "help [topic]: list commands, or describe a single command.".to_string(),
                    other => format!("No help available for '{other}'."),
                }
            }
            DapReplCommand::Run(cmd) => {
                let result = (self.launcher)(cmd);
                match result {
                    Ok(out) => (self.masker)(&out),
                    Err(e) => format!("error: {e}"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_help_with_no_args() {
        let cmd = DapReplParser::parse("help").unwrap();
        assert!(matches!(cmd, DapReplCommand::Help { topic: None }));
    }

    #[test]
    fn parses_help_with_topic() {
        let cmd = DapReplParser::parse("help(\"run\")").unwrap();
        assert_eq!(cmd, DapReplCommand::Help { topic: Some("run".into()) });
    }

    #[test]
    fn parses_run_with_just_script() {
        let cmd = DapReplParser::parse("run(\"echo hello\")").unwrap();
        assert_eq!(
            cmd,
            DapReplCommand::Run(RunCommand {
                script: "echo hello".into(),
                shell: None,
                env: BTreeMap::new(),
                working_directory: None,
            })
        );
    }

    #[test]
    fn parses_run_with_named_args() {
        let cmd = DapReplParser::parse(
            "run(\"echo $FOO\", shell: \"bash\", env: { FOO: \"bar\" }, working_directory: \"/tmp\")",
        )
        .unwrap();
        let run = match cmd {
            DapReplCommand::Run(r) => r,
            _ => unreachable!(),
        };
        assert_eq!(run.script, "echo $FOO");
        assert_eq!(run.shell.as_deref(), Some("bash"));
        assert_eq!(run.working_directory.as_deref(), Some("/tmp"));
        assert_eq!(run.env.get("FOO").map(|s| s.as_str()), Some("bar"));
    }

    #[test]
    fn rejects_unknown_command() {
        let err = DapReplParser::parse("nope").unwrap_err();
        assert!(matches!(err, ParseError::UnknownCommand(_)));
    }

    #[test]
    fn rejects_empty_input() {
        let err = DapReplParser::parse("   ").unwrap_err();
        assert!(matches!(err, ParseError::Empty));
    }

    #[test]
    fn rejects_run_with_no_args() {
        let err = DapReplParser::parse("run()").unwrap_err();
        assert!(matches!(err, ParseError::MissingArgument("script")));
    }

    #[test]
    fn executor_help_routes_through_topic() {
        let exec = DapReplExecutor::default();
        let out = exec.execute(&DapReplCommand::Help { topic: None });
        assert!(out.contains("Available commands"));
        let out = exec.execute(&DapReplCommand::Help {
            topic: Some("run".into()),
        });
        assert!(out.contains("execute a shell command"));
    }

    #[test]
    fn executor_run_uses_launcher() {
        let exec = DapReplExecutor::default().with_launcher(Box::new(|cmd| {
            Ok(format!("ran: {}", cmd.script))
        }));
        let out = exec.execute(&DapReplCommand::Run(RunCommand {
            script: "echo hi".into(),
            shell: None,
            env: BTreeMap::new(),
            working_directory: None,
        }));
        assert_eq!(out, "ran: echo hi");
    }

    #[test]
    fn executor_masks_launcher_output() {
        let exec = DapReplExecutor::with_masker(Box::new(|_| "<masked>".into()))
            .with_launcher(Box::new(|_| Ok("super-secret".into())));
        let out = exec.execute(&DapReplCommand::Run(RunCommand {
            script: "true".into(),
            shell: None,
            env: BTreeMap::new(),
            working_directory: None,
        }));
        assert_eq!(out, "<masked>");
    }

    #[test]
    fn split_args_respects_quotes_and_braces() {
        let parts = split_args(r#""a, b", c, { d: e }"#);
        assert_eq!(parts, vec![r#""a, b""#.to_string(), "c".to_string(), "{ d: e }".to_string()]);
    }
}
