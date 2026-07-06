//! Workflow command parser.
//!
//! Parses `::name key=val,key2=val2::data` and legacy `##[name]data` lines
//! from step output, with the official unescaping rules.

use std::collections::HashMap;

/// A parsed workflow command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowCommand {
    pub name: String,
    pub properties: HashMap<String, String>,
    pub data: String,
}

/// Parse a single line for workflow commands.
///
/// Returns `Some(command)` if the line is a workflow command, `None` otherwise.
pub fn parse_command(line: &str) -> Option<WorkflowCommand> {
    // Try new format: ::name key=val::data
    if let Some(cmd) = parse_double_colon(line) {
        return Some(cmd);
    }
    // Try legacy format: ##[name]data
    if let Some(cmd) = parse_legacy(line) {
        return Some(cmd);
    }
    None
}

/// Parse `::name key=val,key2=val2::data` format.
fn parse_double_colon(line: &str) -> Option<WorkflowCommand> {
    let line = line.trim();
    if !line.starts_with("::") {
        return None;
    }

    let rest = &line[2..];
    let end_pos = rest.find("::")?;
    let command_part = &rest[..end_pos];
    let data = if end_pos + 2 < rest.len() {
        &rest[end_pos + 2..]
    } else {
        ""
    };

    // Split command_part into name and properties
    let (name, props_str) = if let Some(space_pos) = command_part.find(' ') {
        (
            &command_part[..space_pos],
            Some(&command_part[space_pos + 1..]),
        )
    } else {
        (command_part, None)
    };

    if name.is_empty() {
        return None;
    }

    let mut properties = HashMap::new();
    if let Some(props) = props_str {
        for pair in props.split(',') {
            if let Some(eq_pos) = pair.find('=') {
                let key = &pair[..eq_pos];
                let val = &pair[eq_pos + 1..];
                properties.insert(key.to_string(), unescape_property(val));
            }
        }
    }

    Some(WorkflowCommand {
        name: name.to_lowercase(),
        properties,
        data: unescape_data(data),
    })
}

/// Parse legacy `##[name]data` format.
fn parse_legacy(line: &str) -> Option<WorkflowCommand> {
    let line = line.trim();
    if !line.starts_with("##[") {
        return None;
    }
    let close = line.find(']')?;
    let name = &line[3..close];
    let data = &line[close + 1..];

    Some(WorkflowCommand {
        name: name.to_lowercase(),
        properties: HashMap::new(),
        data: data.to_string(),
    })
}

/// Unescape data values per official runner rules.
/// `%25` → `%`, `%0D` → `\r`, `%0A` → `\n`
fn unescape_data(s: &str) -> String {
    s.replace("%0A", "\n")
        .replace("%0D", "\r")
        .replace("%25", "%")
}

/// Unescape property values per official runner rules.
/// Same as data, plus: `%3A` → `:`, `%2C` → `,`
fn unescape_property(s: &str) -> String {
    s.replace("%3A", ":")
        .replace("%2C", ",")
        .replace("%0A", "\n")
        .replace("%0D", "\r")
        .replace("%25", "%")
}

/// Process a workflow command against the step/job context.
pub fn handle_command(
    cmd: &WorkflowCommand,
    ctx: &mut crate::worker::execution_context::StepContext<'_>,
) {
    match cmd.name.as_str() {
        "add-mask" => {
            ctx.job.add_mask(&cmd.data);
        }
        "add-path" => {
            ctx.job.extra_path.insert(0, cmd.data.clone());
        }
        "debug" => {
            ctx.debug(&cmd.data);
        }
        "error" => {
            let masked_data = ctx.job.mask_secrets(&cmd.data);
            let annotation = build_annotation(
                crate::worker::execution_context::AnnotationLevel::Error,
                cmd,
                &masked_data,
            );
            ctx.annotate(annotation);
            ctx.log_raw(&format!("##[error]{masked_data}"));
        }
        "warning" => {
            let masked_data = ctx.job.mask_secrets(&cmd.data);
            let annotation = build_annotation(
                crate::worker::execution_context::AnnotationLevel::Warning,
                cmd,
                &masked_data,
            );
            ctx.annotate(annotation);
            ctx.log_raw(&format!("##[warning]{masked_data}"));
        }
        "notice" => {
            let masked_data = ctx.job.mask_secrets(&cmd.data);
            let annotation = build_annotation(
                crate::worker::execution_context::AnnotationLevel::Notice,
                cmd,
                &masked_data,
            );
            ctx.annotate(annotation);
            ctx.log_raw(&format!("##[notice]{masked_data}"));
        }
        "group" => {
            ctx.log_raw(&format!("##[group]{}", cmd.data));
        }
        "endgroup" => {
            ctx.log_raw("##[endgroup]");
        }
        "echo" => {
            // echo on/off controls command echoing
            match cmd.data.as_str() {
                "on" => ctx.echo = true,
                "off" => ctx.echo = false,
                _ => {}
            }
        }
        "set-output" => {
            // Legacy: ::set-output name=key::value
            // Emit deprecation warning
            ctx.log("##[warning]The `set-output` command is deprecated and will be disabled soon. Please upgrade to using Environment Files. For more information see: https://github.blog/changelog/2022-10-11-github-actions-deprecating-save-state-and-set-output-commands/");
            if let Some(name) = cmd.properties.get("name") {
                ctx.env.insert(format!("OUTPUT_{name}"), cmd.data.clone());
            }
        }
        "save-state" => {
            // Legacy: ::save-state name=key::value
            ctx.log("##[warning]The `save-state` command is deprecated and will be disabled soon. Please upgrade to using Environment Files.");
            if let Some(name) = cmd.properties.get("name") {
                let state = ctx.job.state.entry(ctx.step_id.clone()).or_default();
                state.insert(name.clone(), cmd.data.clone());
            }
        }
        "stop-commands" => {
            // stop-commands: handled at a higher level (M10)
        }
        "add-matcher" => {
            // P1.6: Register problem matcher from JSON file
            let path = std::path::Path::new(&cmd.data);
            if let Err(e) = ctx.job.matchers.add_from_file(path) {
                tracing::warn!("Failed to add problem matcher from {}: {e:#}", cmd.data);
            }
        }
        "remove-matcher" => {
            // P1.6: Unregister problem matcher by owner name
            let owner = cmd.properties.get("owner").unwrap_or(&cmd.data);
            ctx.job.matchers.remove(owner);
        }
        _ => {
            tracing::debug!("Unknown workflow command: {}", cmd.name);
        }
    }
}

fn build_annotation(
    level: crate::worker::execution_context::AnnotationLevel,
    cmd: &WorkflowCommand,
    masked_message: &str,
) -> crate::worker::execution_context::Annotation {
    crate::worker::execution_context::Annotation {
        level,
        message: masked_message.to_string(),
        title: cmd.properties.get("title").cloned(),
        file: cmd.properties.get("file").cloned(),
        line: cmd.properties.get("line").and_then(|v| v.parse().ok()),
        end_line: cmd.properties.get("endLine").and_then(|v| v.parse().ok()),
        col: cmd.properties.get("col").and_then(|v| v.parse().ok()),
        end_column: cmd.properties.get("endColumn").and_then(|v| v.parse().ok()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_command() {
        let cmd = parse_command("::debug::some message").unwrap();
        assert_eq!(cmd.name, "debug");
        assert_eq!(cmd.data, "some message");
        assert!(cmd.properties.is_empty());
    }

    #[test]
    fn parse_command_with_properties() {
        let cmd = parse_command("::error file=app.js,line=10,col=5::Something went wrong").unwrap();
        assert_eq!(cmd.name, "error");
        assert_eq!(cmd.data, "Something went wrong");
        assert_eq!(cmd.properties.get("file").unwrap(), "app.js");
        assert_eq!(cmd.properties.get("line").unwrap(), "10");
        assert_eq!(cmd.properties.get("col").unwrap(), "5");
    }

    #[test]
    fn parse_add_mask() {
        let cmd = parse_command("::add-mask::my secret value").unwrap();
        assert_eq!(cmd.name, "add-mask");
        assert_eq!(cmd.data, "my secret value");
    }

    #[test]
    fn parse_legacy_format() {
        let cmd = parse_command("##[error]Something failed").unwrap();
        assert_eq!(cmd.name, "error");
        assert_eq!(cmd.data, "Something failed");
    }
    #[test]
    fn parse_case_insensitive() {
        let cmd1 = parse_command("::SET-OUTPUT name=foo::bar").unwrap();
        assert_eq!(cmd1.name, "set-output");
        let cmd2 = parse_command("##[ERROR]failed").unwrap();
        assert_eq!(cmd2.name, "error");
    }

    #[test]
    fn unescape_data_values() {
        let cmd = parse_command("::debug::line1%0Aline2%0Dcarriage%25percent").unwrap();
        assert_eq!(cmd.data, "line1\nline2\rcarriage%percent");
    }

    #[test]
    fn unescape_property_values() {
        let cmd = parse_command("::error file=path%3Ato%2Cfile::msg").unwrap();
        assert_eq!(cmd.properties.get("file").unwrap(), "path:to,file");
    }

    #[test]
    fn not_a_command() {
        assert!(parse_command("just a normal line").is_none());
        assert!(parse_command("").is_none());
    }

    #[test]
    fn set_output_legacy() {
        let cmd = parse_command("::set-output name=result::hello world").unwrap();
        assert_eq!(cmd.name, "set-output");
        assert_eq!(cmd.properties.get("name").unwrap(), "result");
        assert_eq!(cmd.data, "hello world");
    }

    // --- P0 command handler integration tests ---

    fn make_ctx<'a>(
        job: &'a mut crate::worker::contexts::JobContext,
    ) -> crate::worker::execution_context::StepContext<'a> {
        crate::worker::execution_context::StepContext::new(job, "s1".into(), "Step".into())
    }

    fn make_job() -> crate::worker::contexts::JobContext {
        crate::worker::contexts::JobContext::new(
            "j1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        )
    }

    #[test]
    fn handle_add_mask_adds_to_masks() {
        let mut job = make_job();
        let mut ctx = make_ctx(&mut job);
        let cmd = parse_command("::add-mask::my-secret-token").unwrap();
        handle_command(&cmd, &mut ctx);
        assert_eq!(
            ctx.job.mask_secrets("my-secret-token is here"),
            "*** is here"
        );
    }

    #[test]
    fn handle_error_creates_annotation() {
        let mut job = make_job();
        let mut ctx = make_ctx(&mut job);
        let cmd = parse_command("::error file=src/main.rs,line=42,endLine=44,col=5,endColumn=10,title=Build Error::compilation failed").unwrap();
        handle_command(&cmd, &mut ctx);
        assert_eq!(ctx.annotations.len(), 1);
        assert_eq!(
            ctx.annotations[0].level,
            crate::worker::execution_context::AnnotationLevel::Error
        );
        assert_eq!(ctx.annotations[0].message, "compilation failed");
        assert_eq!(ctx.annotations[0].file.as_deref(), Some("src/main.rs"));
        assert_eq!(ctx.annotations[0].line, Some(42));
        assert_eq!(ctx.annotations[0].end_line, Some(44));
        assert_eq!(ctx.annotations[0].col, Some(5));
        assert_eq!(ctx.annotations[0].end_column, Some(10));
        assert_eq!(ctx.annotations[0].title.as_deref(), Some("Build Error"));
    }

    #[test]
    fn handle_warning_creates_annotation() {
        let mut job = make_job();
        let mut ctx = make_ctx(&mut job);
        let cmd = parse_command("::warning::this is a warning").unwrap();
        handle_command(&cmd, &mut ctx);
        assert_eq!(ctx.annotations.len(), 1);
        assert_eq!(
            ctx.annotations[0].level,
            crate::worker::execution_context::AnnotationLevel::Warning
        );
        assert_eq!(ctx.annotations[0].message, "this is a warning");
    }

    #[test]
    fn handle_notice_creates_annotation() {
        let mut job = make_job();
        let mut ctx = make_ctx(&mut job);
        let cmd = parse_command("::notice::just an FYI").unwrap();
        handle_command(&cmd, &mut ctx);
        assert_eq!(ctx.annotations.len(), 1);
        assert_eq!(
            ctx.annotations[0].level,
            crate::worker::execution_context::AnnotationLevel::Notice
        );
    }

    #[test]
    fn handle_group_endgroup_logging() {
        let mut job = make_job();
        let mut ctx = make_ctx(&mut job);
        let group = parse_command("::group::Build step").unwrap();
        handle_command(&group, &mut ctx);
        assert!(ctx
            .log_lines
            .iter()
            .any(|l| l.contains("##[group]Build step")));

        let endgroup = parse_command("::endgroup::").unwrap();
        handle_command(&endgroup, &mut ctx);
        assert!(ctx.log_lines.iter().any(|l| l.contains("##[endgroup]")));
    }

    #[test]
    fn handle_echo_on_off() {
        let mut job = make_job();
        let mut ctx = make_ctx(&mut job);
        assert!(!ctx.echo);

        let on = parse_command("::echo::on").unwrap();
        handle_command(&on, &mut ctx);
        assert!(ctx.echo);

        let off = parse_command("::echo::off").unwrap();
        handle_command(&off, &mut ctx);
        assert!(!ctx.echo);
    }

    #[test]
    fn handle_stop_commands_via_log() {
        let mut job = make_job();
        let mut ctx = make_ctx(&mut job);

        // stop-commands sets a token
        ctx.log("::stop-commands::my_token");
        assert_eq!(ctx.stop_commands_token, Some("my_token".to_string()));

        // Commands are suspended while token is active
        ctx.log("::error::this should not create an annotation");
        assert!(ctx.annotations.is_empty());

        // Resume with the token
        ctx.log("::my_token::");
        assert!(ctx.stop_commands_token.is_none());

        // Commands work again
        ctx.log("::error::this should create an annotation");
        assert_eq!(ctx.annotations.len(), 1);
    }
}
