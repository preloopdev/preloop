//! Per-step execution context.
//!
//! Wraps the job context with step-specific state: env stack,
//! secret masking, issue/annotation collection, debug flag.

use std::collections::HashMap;

use super::contexts::JobContext;

/// Per-step execution context.
pub struct StepContext<'a> {
    pub job: &'a mut JobContext,
    pub step_id: String,
    pub step_name: String,
    /// Step-level env overrides.
    pub env: HashMap<String, String>,
    /// Annotations collected during step execution.
    pub annotations: Vec<Annotation>,
    /// Whether debug output is enabled.
    pub debug: bool,
    /// Whether command echoing is enabled.
    pub echo: bool,
    /// Log lines collected during step execution.
    pub log_lines: Vec<String>,
    /// Whether the step was cancelled.
    pub cancelled: bool,
    /// stop-commands token: when set, all commands are suspended until `::{token}::` is seen.
    pub stop_commands_token: Option<String>,
}

/// A workflow annotation (error/warning/notice).
#[derive(Debug, Clone)]
pub struct Annotation {
    pub level: AnnotationLevel,
    pub message: String,
    pub title: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub end_line: Option<u32>,
    pub col: Option<u32>,
    pub end_column: Option<u32>,
}

/// Annotation severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnotationLevel {
    Notice,
    Warning,
    Error,
}

impl<'a> StepContext<'a> {
    /// Create a new step context.
    pub fn new(job: &'a mut JobContext, step_id: String, step_name: String) -> Self {
        let is_debug = |v: &str| v == "true" || v == "1";
        let debug = std::env::var("ACTIONS_STEP_DEBUG").is_ok_and(|v| is_debug(&v))
            || std::env::var("RUNNER_DEBUG").is_ok_and(|v| is_debug(&v))
            || job.env.get("ACTIONS_STEP_DEBUG").is_some_and(|v| is_debug(v))
            || job.env.get("RUNNER_DEBUG").is_some_and(|v| is_debug(v));
        Self {
            job,
            step_id,
            step_name,
            env: HashMap::new(),
            annotations: Vec::new(),
            debug,
            echo: false,
            log_lines: Vec::new(),
            cancelled: false,
            stop_commands_token: None,
        }
    }

    /// Add a log line: parse workflow commands, apply masking, feed problem matchers.
    pub fn log(&mut self, line: &str) {
        // stop-commands: if a token is set, only look for the resume command
        if let Some(ref token) = self.stop_commands_token.clone() {
            if line.trim() == format!("::{token}::") {
                self.stop_commands_token = None;
                return;
            }
            // All commands suspended — just log the line
            let masked = self.job.mask_secrets(line);
            let ts = crate::worker::job_runner::iso_now();
            self.log_lines.push(format!("{ts} {masked}"));
            return;
        }

        // Parse and handle workflow commands (::add-matcher::, ::error::, etc.)
        if let Some(cmd) = super::commands::parse_command(line) {
            // stop-commands: record the token and consume the line
            if cmd.name == "stop-commands" && !cmd.data.is_empty() {
                self.stop_commands_token = Some(cmd.data.clone());
                return;
            }

            super::commands::handle_command(&cmd, self);

            // Consumed commands: don't log them unless echo is enabled
            if matches!(
                cmd.name.as_str(),
                "add-matcher" | "remove-matcher" | "add-mask" | "save-state" | "set-output"
            ) {
                if self.echo {
                    self.log_raw(line);
                }
                return;
            }
            // group/endgroup/debug/error/warning/notice are already logged
            // by handle_command via log_raw(), so don't double-log
            if matches!(
                cmd.name.as_str(),
                "group" | "endgroup" | "debug" | "error" | "warning" | "notice"
            ) {
                return;
            }
        }

        self.log_raw(line);
    }

    /// Log a line directly (no command parsing). Applies masking and problem matchers.
    pub fn log_raw(&mut self, line: &str) {
        let masked = self.job.mask_secrets(line);
        // P1.6: Feed through job-level problem matchers to produce annotations
        let matched_annotations = self.job.matchers.match_line(&masked);
        for ann in matched_annotations {
            self.annotations.push(ann);
        }
        let ts = crate::worker::job_runner::iso_now();
        self.log_lines.push(format!("{ts} {masked}"));
    }

    /// Add an annotation.
    pub fn annotate(&mut self, annotation: Annotation) {
        self.annotations.push(annotation);
    }

    /// Update the debug flag based on step environment overrides.
    pub fn update_debug_flag(&mut self) {
        let is_debug = |v: &str| v == "true" || v == "1";
        self.debug = self.debug
            || std::env::var("ACTIONS_STEP_DEBUG").is_ok_and(|v| is_debug(&v))
            || std::env::var("RUNNER_DEBUG").is_ok_and(|v| is_debug(&v))
            || self.job.env.get("ACTIONS_STEP_DEBUG").is_some_and(|v| is_debug(v))
            || self.job.env.get("RUNNER_DEBUG").is_some_and(|v| is_debug(v))
            || self.env.get("ACTIONS_STEP_DEBUG").is_some_and(|v| is_debug(v))
            || self.env.get("RUNNER_DEBUG").is_some_and(|v| is_debug(v));
    }

    /// Log a debug line if debug mode is active.
    pub fn debug(&mut self, message: &str) {
        if self.debug {
            self.log_raw(&format!("##[debug]{}", message));
        }
    }

    /// Build the full environment for process execution.
    pub fn build_env(&self) -> HashMap<String, String> {
        let mut env = self.job.env.clone();
        // Step env overrides job env
        for (k, v) in &self.env {
            env.insert(k.clone(), v.clone());
        }
        // Post action steps receive state saved by their paired main step via
        // GITHUB_STATE. A post step is named `__post_<main-step-id>`.
        let state_step_id = self
            .step_id
            .strip_prefix("__post_")
            .unwrap_or(self.step_id.as_str());
        if let Some(state) = self.job.state.get(state_step_id) {
            for (k, v) in state {
                env.insert(format!("STATE_{k}"), v.clone());
            }
        }
        // Add PATH extensions
        if !self.job.extra_path.is_empty() {
            let current_path = std::env::var("PATH").unwrap_or_default();
            let mut parts: Vec<&str> = self.job.extra_path.iter().map(|s| s.as_str()).collect();
            parts.push(&current_path);
            env.insert("PATH".to_string(), parts.join(":"));
        }
        env
    }

    /// Get all collected log content as a single string.
    pub fn log_content(&self) -> String {
        self.log_lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::contexts::JobContext;

    fn make_job() -> JobContext {
        let mut job = JobContext::new(
            "j1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        job.env.insert("JOB_VAR".into(), "from_job".into());
        job.add_mask("secret-value");
        job
    }

    #[test]
    fn build_env_merges_job_and_step() {
        let mut job = make_job();
        let mut ctx = StepContext::new(&mut job, "s1".into(), "Step 1".into());
        ctx.env.insert("STEP_VAR".into(), "from_step".into());
        ctx.env.insert("JOB_VAR".into(), "overridden".into());

        let env = ctx.build_env();
        assert_eq!(env.get("STEP_VAR").unwrap(), "from_step");
        assert_eq!(env.get("JOB_VAR").unwrap(), "overridden");
    }

    #[test]
    fn build_env_includes_extra_path() {
        let mut job = make_job();
        job.extra_path.push("/custom/bin".into());
        let ctx = StepContext::new(&mut job, "s1".into(), "Step".into());

        let env = ctx.build_env();
        let path = env.get("PATH").unwrap();
        assert!(path.starts_with("/custom/bin:"));
    }

    #[test]
    fn log_masks_secrets() {
        let mut job = make_job();
        let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
        ctx.log("token is secret-value here");
        assert!(ctx.log_lines[0].ends_with("token is *** here"));
    }

    #[test]
    fn log_content_joins_lines() {
        let mut job = make_job();
        let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
        ctx.log("line1");
        ctx.log("line2");
        let lines: Vec<&str> = ctx.log_lines.iter().map(|l| l.splitn(2, ' ').nth(1).unwrap_or("")).collect();
        assert_eq!(lines, vec!["line1", "line2"]);
    }

    #[test]
    fn annotations_collected() {
        let mut job = make_job();
        let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
        ctx.annotate(Annotation {
            level: AnnotationLevel::Error,
            message: "test error".into(),
            title: Some("Title".into()),
            file: Some("src/main.rs".into()),
            line: Some(42),
            end_line: None,
            col: None,
            end_column: None,
        });
        assert_eq!(ctx.annotations.len(), 1);
        assert_eq!(ctx.annotations[0].message, "test error");
    }

    #[test]
    fn post_step_env_exposes_saved_state_from_main_step() {
        let mut job = make_job();
        job.state
            .entry("checkout".into())
            .or_default()
            .insert("repository".into(), "owner/repo".into());

        let ctx = StepContext::new(&mut job, "__post_checkout".into(), "Post checkout".into());
        let env = ctx.build_env();

        assert_eq!(
            env.get("STATE_repository").map(String::as_str),
            Some("owner/repo")
        );
    }
}
