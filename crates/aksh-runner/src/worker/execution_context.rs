//! Per-step execution context.
//!
//! Wraps the job context with step-specific state: env stack,
//! secret masking, issue/annotation collection, debug flag.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::sync::Arc;

use super::contexts::JobContext;
pub use super::execution_types::{Annotation, AnnotationLevel};
use super::live_logs::LiveLogQueue;

const DISABLE_STDOUT_MULTILINE_LOG_PREFIXING: &str =
    "ACTIONS_RUNNER_DISABLE_STDOUT_MULTILINE_LOG_PREFIXING";

/// Parse the runner's multiline stdout prefix toggle.
///
/// `StdoutTraceListener` snapshots this process environment variable when it
/// is constructed and uses `StringUtil.ConvertToBoolean`, whose accepted true
/// values are `1`, `true`, and `$true` (case-insensitive). Invalid and unset
/// values retain the default of false.
fn disable_stdout_multiline_log_prefixing() -> bool {
    std::env::var(DISABLE_STDOUT_MULTILINE_LOG_PREFIXING)
        .is_ok_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "$true"))
}

/// Format one line from a stdout trace message.
///
/// The official listener writes a header before every line by default. With
/// multiline prefixing disabled, only the first line gets that header and
/// continuation lines are written verbatim. The timestamp is the local
/// equivalent of that listener header; preserving the line itself is
/// important because workflow-command prefixes (for example `##[error]`) are
/// unrelated to the trace header and must not be altered.
fn format_stdout_line(timestamp: &str, line: &str, prefix: bool) -> String {
    if prefix {
        format!("{timestamp} {line}")
    } else {
        line.to_string()
    }
}

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
    /// Whether to translate container path to host path.
    pub translate_container_path: bool,
    /// Telemetry error messages collected during step execution.
    pub telemetry_errors: Vec<String>,
    /// Log temp file for durable storage.
    pub log_file: Arc<Mutex<BufWriter<std::fs::File>>>,
    /// Line buffer for accumulating partial lines from process output chunks.
    line_buffer: Arc<Mutex<Vec<u8>>>,
    /// Whether to also accumulate log lines in memory (for tests).
    pub keep_in_memory: bool,
    /// Whether continuation lines in a multiline stdout chunk omit the
    /// timestamp prefix, matching StdoutTraceListener.
    disable_stdout_multiline_log_prefixing: bool,
    /// Whether the current buffered partial line follows a complete line in
    /// the same stdout chunk and should therefore omit its prefix on flush.
    stdout_partial_is_continuation: std::sync::atomic::AtomicBool,
    /// Live log queue for WebSocket streaming (best-effort, None when not connected).
    pub live_logs: Option<Arc<LiveLogQueue>>,
    /// Monotonic line counter for live log feed (per step).
    live_line_counter: std::sync::atomic::AtomicU64,
}

impl<'a> StepContext<'a> {
    /// Create a new step context.
    pub fn new(job: &'a mut JobContext, step_id: String, step_name: String) -> Self {
        let is_debug = |v: &str| v == "true" || v == "1";
        let debug = std::env::var("ACTIONS_STEP_DEBUG").is_ok_and(|v| is_debug(&v))
            || std::env::var("RUNNER_DEBUG").is_ok_and(|v| is_debug(&v))
            || job
                .env
                .get("ACTIONS_STEP_DEBUG")
                .is_some_and(|v| is_debug(v))
            || job.env.get("RUNNER_DEBUG").is_some_and(|v| is_debug(v));
        let translate_container_path = job.container_state.is_some();
        let temp_file = tempfile::tempfile().expect("failed to create log temp file");
        let log_file = Arc::new(Mutex::new(BufWriter::new(temp_file)));
        let keep_in_memory = cfg!(test);
        let live_logs = job.live_logs.clone();
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
            translate_container_path,
            telemetry_errors: Vec::new(),
            log_file,
            line_buffer: Arc::new(Mutex::new(Vec::new())),
            keep_in_memory,
            disable_stdout_multiline_log_prefixing: disable_stdout_multiline_log_prefixing(),
            stdout_partial_is_continuation: std::sync::atomic::AtomicBool::new(false),
            live_logs,
            live_line_counter: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Write a raw byte chunk from process output.
    ///
    /// Buffers partial lines and processes each complete line with a UTC
    /// timestamp prefix and secret masking, matching the official runner's
    /// per-line log formatting.
    pub fn write_chunk(&self, chunk: &[u8]) {
        let mut buf = self.line_buffer.lock();
        buf.extend_from_slice(chunk);
        let mut prefix = true;
        self.stdout_partial_is_continuation
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Process all complete lines (ending with \n)
        while let Some(newline_pos) = buf.iter().position(|&b| b == b'\n') {
            // Extract the line (without the newline)
            let line_bytes: Vec<u8> = buf.drain(..=newline_pos).collect();
            let line = String::from_utf8_lossy(&line_bytes[..line_bytes.len() - 1]);

            let masked = self.job.mask_secrets(&line);
            let ts = crate::worker::helpers::iso_now();
            let fmt = format_stdout_line(
                &ts,
                &masked,
                prefix || !self.disable_stdout_multiline_log_prefixing,
            );
            prefix = false;
            {
                let mut lock = self.log_file.lock();
                let _ = writeln!(lock, "{}", fmt);
            }
            // Feed to live log WebSocket (best-effort, matching official runner
            // OutputManager per-line callback behavior).
            if let Some(ref live) = self.live_logs {
                let line_num = self
                    .live_line_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                live.enqueue(&self.step_id, &masked, line_num);
            }
        }

        self.stdout_partial_is_continuation.store(
            self.disable_stdout_multiline_log_prefixing && !prefix && !buf.is_empty(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Flush any remaining partial line in the buffer (call at step end).
    pub fn flush_line_buffer(&self) {
        let mut buf = self.line_buffer.lock();
        if buf.is_empty() {
            return;
        }
        let line = String::from_utf8_lossy(&buf);
        let masked = self.job.mask_secrets(&line);
        let ts = crate::worker::helpers::iso_now();
        let continuation = self
            .stdout_partial_is_continuation
            .swap(false, std::sync::atomic::Ordering::Relaxed);
        let fmt = format_stdout_line(
            &ts,
            &masked,
            !self.disable_stdout_multiline_log_prefixing || !continuation,
        );
        {
            let mut lock = self.log_file.lock();
            let _ = writeln!(lock, "{}", fmt);
        }
        // Feed final partial line to live log WebSocket.
        if let Some(ref live) = self.live_logs {
            let line_num = self
                .live_line_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            live.enqueue(&self.step_id, &masked, line_num);
        }
        buf.clear();
    }

    /// Add a log line: parse workflow commands, apply masking, feed problem matchers.
    pub fn log(&mut self, line: &str) {
        // stop-commands: if a token is set, only look for the resume command
        if let Some(token) = &self.stop_commands_token.clone() {
            if line.trim() == format!("::{token}::") {
                self.stop_commands_token = None;
                return;
            }
            // All commands suspended — just log the line
            let masked = self.job.mask_secrets(line);
            let ts = crate::worker::helpers::iso_now();
            let fmt = format!("{ts} {masked}");
            {
                let mut lock = self.log_file.lock();
                let _ = writeln!(lock, "{}", fmt);
            }
            if self.keep_in_memory {
                self.log_lines.push(fmt);
            }
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
        // Strip runner-controlled markers from user output to prevent injection
        let line = if line.contains("##[start-action") || line.contains("##[end-action") {
            line.replace("##[start-action", r##"##[\start-action"##)
                .replace("##[end-action", r##"##[\end-action"##)
        } else {
            line.to_string()
        };

        // Capture git unsafe repository error messages to telemetry
        if line.contains("fatal: unsafe repository") {
            self.telemetry_errors.push(line.clone());
        }

        let masked = self.job.mask_secrets(&line);

        let workspace = self.job.workspace.clone().unwrap_or_default();
        let repository = self
            .job
            .github_context_value("repository")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();
        let server_url = self
            .job
            .github_context_value("server_url")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();

        // P1.6: Feed through job-level problem matchers to produce annotations
        let matched_annotations = self.job.matchers.match_line(
            &masked,
            &workspace,
            &repository,
            &server_url,
            self.translate_container_path,
        );

        let fmt = if !matched_annotations.is_empty() {
            // Find the highest severity level to prefix the log line
            let mut prefix = "##[error]";
            for ann in &matched_annotations {
                match ann.level {
                    AnnotationLevel::Error => {
                        prefix = "##[error]";
                        break;
                    }
                    AnnotationLevel::Warning => {
                        prefix = "##[warning]";
                    }
                    AnnotationLevel::Notice => {
                        if prefix != "##[error]" && prefix != "##[warning]" {
                            prefix = "##[notice]";
                        }
                    }
                }
            }

            for ann in matched_annotations {
                self.annotate(ann);
            }

            let ts = crate::worker::helpers::iso_now();
            format!("{ts} {prefix}{masked}")
        } else {
            let ts = crate::worker::helpers::iso_now();
            format!("{ts} {masked}")
        };

        {
            let mut lock = self.log_file.lock();
            let _ = writeln!(lock, "{}", fmt);
        }
        if self.keep_in_memory {
            self.log_lines.push(fmt);
        }
    }

    /// Add an annotation.
    ///
    /// Official runner caps annotation messages at 4096 characters and limits
    /// to 10 annotations per step.
    pub fn annotate(&mut self, mut annotation: Annotation) {
        if self.annotations.len() < 10 {
            if annotation.message.len() > 4096 {
                annotation.message.truncate(4096);
            }
            self.annotations.push(annotation);
        }
    }

    /// Update the debug flag based on step environment overrides.
    pub fn update_debug_flag(&mut self) {
        let is_debug = |v: &str| v == "true" || v == "1";
        self.debug = self.debug
            || std::env::var("ACTIONS_STEP_DEBUG").is_ok_and(|v| is_debug(&v))
            || std::env::var("RUNNER_DEBUG").is_ok_and(|v| is_debug(&v))
            || self
                .job
                .env
                .get("ACTIONS_STEP_DEBUG")
                .is_some_and(|v| is_debug(v))
            || self
                .job
                .env
                .get("RUNNER_DEBUG")
                .is_some_and(|v| is_debug(v))
            || self
                .env
                .get("ACTIONS_STEP_DEBUG")
                .is_some_and(|v| is_debug(v))
            || self.env.get("RUNNER_DEBUG").is_some_and(|v| is_debug(v));
    }

    /// Log a debug line if debug mode is active.
    ///
    /// Official runner splits multiline debug messages into separate log
    /// entries, one per line.
    pub fn debug(&mut self, message: &str) {
        if self.debug {
            for line in message.split('\n') {
                self.log_raw(&format!("##[debug]{}", line));
            }
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
        // Prepend GITHUB_PATH additions to the step environment PATH.
        // Match official runner: use the job/step env PATH as base (which
        // includes workflow-level env.PATH), falling back to system PATH.
        if !self.job.extra_path.is_empty() {
            let base_path = env
                .get("PATH")
                .cloned()
                .unwrap_or_else(|| std::env::var("PATH").unwrap_or_default());
            let mut parts: Vec<&str> = self.job.extra_path.iter().map(|s| s.as_str()).collect();
            parts.push(&base_path);
            env.insert("PATH".to_string(), parts.join(":"));
        }
        env
    }

    /// Get all collected log content as a single string.
    pub fn log_content(&self) -> String {
        let mut lock = self.log_file.lock();
        let _ = lock.flush();
        let file = lock.get_ref();
        if let Ok(mut cloned) = file.try_clone() {
            use std::io::{Read, Seek, SeekFrom};
            let mut content = String::new();
            let _ = cloned.seek(SeekFrom::Start(0));
            let _ = cloned.read_to_string(&mut content);
            content
        } else {
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::contexts::JobContext;

    static STDOUT_ENV_LOCK: std::sync::LazyLock<Mutex<()>> =
        std::sync::LazyLock::new(|| Mutex::new(()));

    fn with_stdout_toggle<T>(value: Option<&str>, test: impl FnOnce() -> T) -> T {
        let _guard = STDOUT_ENV_LOCK.lock();
        let previous = std::env::var(DISABLE_STDOUT_MULTILINE_LOG_PREFIXING).ok();
        match value {
            Some(value) => std::env::set_var(DISABLE_STDOUT_MULTILINE_LOG_PREFIXING, value),
            None => std::env::remove_var(DISABLE_STDOUT_MULTILINE_LOG_PREFIXING),
        }
        let result = test();
        match previous {
            Some(previous) => std::env::set_var(DISABLE_STDOUT_MULTILINE_LOG_PREFIXING, previous),
            None => std::env::remove_var(DISABLE_STDOUT_MULTILINE_LOG_PREFIXING),
        }
        result
    }

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
    fn build_env_extra_path_prepends_to_env_path_not_system() {
        let mut job = make_job();
        // Simulate workflow-level env.PATH override
        job.env
            .insert("PATH".into(), "/workflow/bin:/usr/bin".into());
        // Simulate GITHUB_PATH addition
        job.extra_path.push("/github/path/bin".into());
        let ctx = StepContext::new(&mut job, "s1".into(), "Step".into());

        let env = ctx.build_env();
        let path = env.get("PATH").unwrap();
        // GITHUB_PATH should prepend to the workflow env.PATH, not system PATH
        assert!(
            path.starts_with("/github/path/bin:"),
            "expected prepend, got: {path}"
        );
        assert!(
            path.contains("/workflow/bin"),
            "expected workflow PATH base, got: {path}"
        );
        // Should NOT contain the test runner's system path as the base
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
        let lines: Vec<&str> = ctx
            .log_lines
            .iter()
            .map(|l| l.split_once(' ').map(|x| x.1).unwrap_or(""))
            .collect();
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
    fn annotations_cap_enforced() {
        let mut job = make_job();
        let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
        for i in 0..15 {
            ctx.annotate(Annotation {
                level: AnnotationLevel::Warning,
                message: format!("warning {i}"),
                title: None,
                file: None,
                line: None,
                end_line: None,
                col: None,
                end_column: None,
            });
        }
        assert_eq!(ctx.annotations.len(), 10);
        assert_eq!(ctx.annotations[9].message, "warning 9");
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
    #[test]
    fn log_raw_problem_matching_and_telemetry() {
        let mut job = make_job();
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("matcher.json");
        std::fs::write(
            &path,
            r#"{
              "problemMatcher": [{
                "owner": "test-owner",
                "pattern": [{
                  "regexp": "^ERROR: (.*)$",
                  "message": 1
                }]
              }]
            }"#,
        )
        .unwrap();
        job.matchers.add_from_file(&path).unwrap();

        let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());

        // 1. Unsafe repository telemetry check
        ctx.log("fatal: unsafe repository ('/github/workspace' is owned by someone else)");
        assert_eq!(ctx.telemetry_errors.len(), 1);
        assert!(ctx.telemetry_errors[0].contains("fatal: unsafe repository"));

        // 2. Composite action marker stripping check
        ctx.log("Some text ##[start-action display=fake;id=fake] more text");
        let last_log = ctx.log_lines.last().unwrap();
        assert!(last_log.contains("##[\\start-action"));

        // 3. Problem matcher check
        ctx.log("ERROR: compilation failed");
        assert_eq!(ctx.annotations.len(), 1);
        assert_eq!(ctx.annotations[0].message, "compilation failed");
        let last_log = ctx.log_lines.last().unwrap();
        assert!(last_log.contains("##[error]ERROR: compilation failed"));
    }

    // --- ExecutionContextL0 gap coverage ---

    #[test]
    fn annotation_message_trimmed_to_max_length() {
        let mut job = make_job();
        let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
        let long_msg = "x".repeat(5000);
        ctx.annotate(Annotation {
            level: AnnotationLevel::Error,
            message: long_msg,
            title: None,
            file: None,
            line: None,
            end_line: None,
            col: None,
            end_column: None,
        });
        assert_eq!(ctx.annotations.len(), 1);
        assert_eq!(ctx.annotations[0].message.len(), 4096);
    }

    #[test]
    fn debug_splits_multiline_messages() {
        let mut job = make_job();
        let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
        ctx.debug = true;
        ctx.debug("line1\nline2\nline3");
        // Should produce 3 separate log entries
        let debug_lines: Vec<_> = ctx
            .log_lines
            .iter()
            .filter(|l| l.contains("##[debug]"))
            .collect();
        assert_eq!(debug_lines.len(), 3);
        assert!(debug_lines[0].contains("##[debug]line1"));
        assert!(debug_lines[1].contains("##[debug]line2"));
        assert!(debug_lines[2].contains("##[debug]line3"));
    }

    #[test]
    fn debug_single_line_unchanged() {
        let mut job = make_job();
        let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
        ctx.debug = true;
        ctx.debug("single line");
        let debug_lines: Vec<_> = ctx
            .log_lines
            .iter()
            .filter(|l| l.contains("##[debug]"))
            .collect();
        assert_eq!(debug_lines.len(), 1);
        assert!(debug_lines[0].contains("##[debug]single line"));
    }

    #[test]
    fn debug_noop_when_disabled() {
        let mut job = make_job();
        let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
        ctx.debug = false;
        ctx.debug("should not appear");
        assert!(ctx.log_lines.is_empty());
    }

    #[test]
    fn stdout_formatter_prefixes_each_line_by_default() {
        assert_eq!(
            format_stdout_line("2026-01-02T03:04:05Z", "first", true),
            "2026-01-02T03:04:05Z first"
        );
        assert_eq!(
            format_stdout_line("2026-01-02T03:04:05Z", "second", true),
            "2026-01-02T03:04:05Z second"
        );
    }

    #[test]
    fn stdout_formatter_suppresses_only_continuation_prefix() {
        assert_eq!(
            format_stdout_line("2026-01-02T03:04:05Z", "first", true),
            "2026-01-02T03:04:05Z first"
        );
        assert_eq!(
            format_stdout_line("2026-01-02T03:04:05Z", "second", false),
            "second"
        );
    }

    #[test]
    fn write_chunk_multiline_stdout_honors_unset_and_false_toggle() {
        for value in [None, Some("false")] {
            with_stdout_toggle(value, || {
                let mut job = make_job();
                let ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
                ctx.write_chunk(b"first\nsecond\n");
                let lines: Vec<_> = ctx.log_content().lines().map(str::to_string).collect();
                assert_eq!(lines.len(), 2);
                assert!(
                    lines[0].ends_with(" first"),
                    "unexpected first line: {}",
                    lines[0]
                );
                assert!(
                    lines[1].ends_with(" second"),
                    "unexpected second line: {}",
                    lines[1]
                );
            });
        }
    }

    #[test]
    fn write_chunk_multiline_stdout_suppresses_true_continuation_prefix() {
        with_stdout_toggle(Some("true"), || {
            let mut job = make_job();
            let ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
            ctx.write_chunk(b"first\nsecond\n");
            let lines: Vec<_> = ctx.log_content().lines().map(str::to_string).collect();
            assert_eq!(lines.len(), 2);
            assert!(
                lines[0].ends_with(" first"),
                "unexpected first line: {}",
                lines[0]
            );
            assert_eq!(lines[1], "second");
        });
    }

    #[test]
    fn write_chunk_flushes_true_partial_continuation_without_prefix() {
        with_stdout_toggle(Some("true"), || {
            let mut job = make_job();
            let ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
            ctx.write_chunk(b"first\nsecond");
            ctx.flush_line_buffer();
            let lines: Vec<_> = ctx.log_content().lines().map(str::to_string).collect();
            assert_eq!(lines.len(), 2);
            assert!(lines[0].ends_with(" first"));
            assert_eq!(lines[1], "second");
        });
    }

    #[test]
    fn write_chunk_multiline_stdout_preserves_unrelated_command_prefixes() {
        with_stdout_toggle(Some("true"), || {
            let mut job = make_job();
            let ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
            ctx.write_chunk(b"##[error]first\n##[warning]second\n");
            let lines: Vec<_> = ctx.log_content().lines().map(str::to_string).collect();
            assert_eq!(lines.len(), 2);
            assert!(lines[0].ends_with(" ##[error]first"));
            assert_eq!(lines[1], "##[warning]second");
        });
    }
}
