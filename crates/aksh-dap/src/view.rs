//! Synthetic `execution.yml` source view.
//!
//! 1:1 port of `src/Runner.Worker/Dap/JobExecutionView.cs`.
//!
//! When VS Code asks for the source text behind a stack frame
//! (`source` request), the runner returns a YAML it generated at
//! startup listing every `pre`/`main`/`post` step. Each line in the
//! YAML corresponds to one step. The line number from `stackTrace`
//! (which maps to step index + a constant) is what the editor
//! displays as the "current line" of the synthetic source.
//!
//! `/` and `\` in job IDs are replaced with `_` to keep the source
//! path a valid filename. See
//! `DapDebuggerL0.cs::StackTraceSanitizesSyntheticSourcePath`.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// Synthetic source filename. Constant for the duration of the
/// session, returned in the DAP `Source` field of every stack frame.
pub const SOURCE_FILE_NAME: &str = "execution.yml";

/// One entry in the synthetic YAML source view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceEntry {
    /// Display name shown in the editor.
    pub display_name: String,
    /// True if this is a `pre:` step (synthetic "Set up job" group).
    pub is_pre: bool,
    /// True if this is a `post:` step (synthetic "Complete job" group).
    pub is_post: bool,
}

/// One line in the synthetic source. Mirrors `StepLine` upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepLine {
    /// Index into the synthetic source lines (1-based, for editor display).
    pub line: i64,
    /// Frame ID the editor uses to identify the frame in
    /// `scopes`/`variables` requests. Mirrors upstream `FrameId`.
    pub frame_id: i64,
    /// Display name shown for the line.
    pub display_name: String,
    /// The kind of step (pre/main/post). Used to choose which
    /// synthetic group label to render in the YAML.
    pub is_pre: bool,
    pub is_post: bool,
}

/// A post-step that was *predicted* by the runner before it was
/// actually registered. Mirrors `PredictedPostStep` upstream. The
/// runner pre-populates the source view with predicted post steps
/// (e.g. the implicit "Complete job" step) so the editor shows them
/// before they reach the registered state. See
/// `DapDebuggerL0.cs::PredictedPostStepIsServedAtInitializationAndClaimedAtRegistration`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictedPostStep {
    /// Display name of the predicted step.
    pub display_name: String,
    /// Stable frame ID assigned at prediction time. When the actual
    /// post step is later registered it claims this same ID so the
    /// editor's view doesn't shift.
    pub frame_id: i64,
}

/// A single job's synthetic source view.
///
/// Mutated as steps register (predicted post steps first, then the
/// resolved step list, then dynamic post steps). Concurrency model
/// mirrors upstream: a single `lock` guards the three lists and the
/// materialized source string.
pub struct JobExecutionView {
    /// Sanitized job ID used in the synthetic source path.
    pub job_id: String,

    pre_entries: Mutex<Vec<SourceEntry>>,
    main_entries: Mutex<Vec<SourceEntry>>,
    post_entries: Mutex<Vec<SourceEntry>>,
    line_by_step: Mutex<Vec<StepLine>>,
    content: Mutex<Option<String>>,
    complete_job_line: Mutex<i64>,
}

impl JobExecutionView {
    /// Build the view for a job. `steps` are the resolved main
    /// steps; `initial_post_steps` are the post steps that exist
    /// before the job starts; `predicted_post_steps` are post steps
    /// the runner speculates about so they can be served at
    /// `initialize` time.
    pub fn new(
        job_id: &str,
        steps: &[SourceEntry],
        initial_post_steps: &[SourceEntry],
        predicted_post_steps: &[PredictedPostStep],
    ) -> Self {
        let job_id = if job_id.is_empty() {
            "job".to_string()
        } else {
            sanitize_path(job_id)
        };
        // "Set up job" occupies line 1; it is the synthetic pre step.
        let line_by_step = vec![StepLine {
            line: 1,
            frame_id: 1,
            display_name: "Set up job".into(),
            is_pre: true,
            is_post: false,
        }];
        let view = Self {
            job_id,
            pre_entries: Mutex::new(vec![SourceEntry {
                display_name: "Set up job".into(),
                is_pre: true,
                is_post: false,
            }]),
            main_entries: Mutex::new(Vec::new()),
            post_entries: Mutex::new(Vec::new()),
            line_by_step: Mutex::new(line_by_step),
            content: Mutex::new(None),
            complete_job_line: Mutex::new(0),
        };
        view.add_steps(steps);
        view.add_post_steps(initial_post_steps);
        view.add_predicted_post_steps(predicted_post_steps);
        view
    }

    /// Add a batch of resolved main steps. Each step becomes one
    /// line in the source view at a stable line number.
    pub fn add_steps(&self, steps: &[SourceEntry]) {
        let mut main = self.main_entries.lock().unwrap();
        let mut lines = self.line_by_step.lock().unwrap();
        let start_line = (lines.len() as i64) + 1;
        for (offset, step) in steps.iter().enumerate() {
            let line = start_line + offset as i64;
            lines.push(StepLine {
                line,
                frame_id: line,
                display_name: step.display_name.clone(),
                is_pre: false,
                is_post: false,
            });
            main.push(step.clone());
        }
        *self.content.lock().unwrap() = None;
    }

    /// Add a batch of post steps (resolved, not predicted).
    pub fn add_post_steps(&self, steps: &[SourceEntry]) {
        let mut post = self.post_entries.lock().unwrap();
        let mut lines = self.line_by_step.lock().unwrap();
        let start_line = (lines.len() as i64) + 1;
        for (offset, step) in steps.iter().enumerate() {
            let line = start_line + offset as i64;
            // If a predicted post step claimed this frame id, reuse it.
            let frame_id = self
                .claim_predicted_frame_id_locked(&step.display_name, &lines)
                .unwrap_or(line);
            lines.push(StepLine {
                line,
                frame_id,
                display_name: step.display_name.clone(),
                is_pre: false,
                is_post: true,
            });
            post.push(step.clone());
        }
        *self.content.lock().unwrap() = None;
    }

    /// Add predicted post steps. They show up in the source view at
    /// `initialize` time, with stable frame IDs that later-resolved
    /// post steps can claim.
    pub fn add_predicted_post_steps(&self, steps: &[PredictedPostStep]) {
        let mut post = self.post_entries.lock().unwrap();
        let mut lines = self.line_by_step.lock().unwrap();
        let start_line = (lines.len() as i64) + 1;
        for (offset, predicted) in steps.iter().enumerate() {
            let line = start_line + offset as i64;
            lines.push(StepLine {
                line,
                frame_id: predicted.frame_id,
                display_name: predicted.display_name.clone(),
                is_pre: false,
                is_post: true,
            });
            post.push(SourceEntry {
                display_name: predicted.display_name.clone(),
                is_pre: false,
                is_post: true,
            });
        }
        *self.content.lock().unwrap() = None;
    }

    /// When a real post step registers and a predicted post step
    /// with the same display name exists, return the predicted
    /// frame ID so the editor view stays stable.
    ///
    /// Caller MUST hold the `line_by_step` lock (this avoids a
    /// reentrant deadlock on the non-reentrant `std::sync::Mutex`).
    fn claim_predicted_frame_id_locked(
        &self,
        display_name: &str,
        lines: &[StepLine],
    ) -> Option<i64> {
        for line in lines.iter() {
            if line.is_post && line.display_name == display_name {
                return Some(line.frame_id);
            }
        }
        None
    }

    /// Snapshot of the materialized YAML source. Computed lazily on
    /// first call and cached until the next mutation.
    pub fn content(&self) -> String {
        if let Some(c) = self.content.lock().unwrap().as_ref() {
            return c.clone();
        }
        let pre = self.pre_entries.lock().unwrap();
        let main = self.main_entries.lock().unwrap();
        let post = self.post_entries.lock().unwrap();
        let mut out = String::new();
        out.push_str("# Synthetic execution view generated by aksh-runner\n");
        out.push_str("pre:\n");
        for entry in pre.iter() {
            out.push_str(&format!("  - {}\n", entry.display_name));
        }
        out.push_str("main:\n");
        for entry in main.iter() {
            out.push_str(&format!("  - {}\n", entry.display_name));
        }
        out.push_str("post:\n");
        for entry in post.iter() {
            out.push_str(&format!("  - {}\n", entry.display_name));
        }
        *self.content.lock().unwrap() = Some(out.clone());
        out
    }

    /// Return the source path (the synthetic YAML filename, rooted
    /// at the sanitized job ID). The DAP `Source.name` returned to
    /// the client uses the `execution.yml` form.
    pub fn source_path(&self) -> String {
        format!("{}/{}", self.job_id, SOURCE_FILE_NAME)
    }

    /// Look up the `StepLine` for a given frame ID.
    pub fn line_for_frame(&self, frame_id: i64) -> Option<StepLine> {
        self.line_by_step
            .lock()
            .unwrap()
            .iter()
            .find(|l| l.frame_id == frame_id)
            .cloned()
    }

    /// Returns the line number for the synthetic "Complete job"
    /// step, or `0` if it has not been added.
    pub fn complete_job_line(&self) -> i64 {
        *self.complete_job_line.lock().unwrap()
    }

    /// Set the line number for "Complete job".
    pub fn set_complete_job_line(&self, line: i64) {
        *self.complete_job_line.lock().unwrap() = line;
    }

    /// Total number of source lines (1-based for the editor).
    pub fn line_count(&self) -> usize {
        self.line_by_step.lock().unwrap().len()
    }
}

/// Replace `/` and `\` with `_` so the synthetic source path is a
/// valid filename on every platform. Mirrors
/// `DapDebuggerL0.cs::StackTraceSanitizesSyntheticSourcePath`.
pub fn sanitize_path(input: &str) -> String {
    input
        .chars()
        .map(|c| if c == '/' || c == '\\' { '_' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn main(name: &str) -> SourceEntry {
        SourceEntry {
            display_name: name.into(),
            is_pre: false,
            is_post: false,
        }
    }

    fn post(name: &str) -> SourceEntry {
        SourceEntry {
            display_name: name.into(),
            is_pre: false,
            is_post: true,
        }
    }

    fn predicted(name: &str, frame_id: i64) -> PredictedPostStep {
        PredictedPostStep {
            display_name: name.into(),
            frame_id,
        }
    }

    #[test]
    fn empty_job_id_falls_back_to_default() {
        let v = JobExecutionView::new("", &[], &[], &[]);
        assert_eq!(v.job_id, "job");
    }

    #[test]
    fn path_sanitization_replaces_slashes_and_backslashes() {
        assert_eq!(sanitize_path("a/b\\c"), "a_b_c");
        assert_eq!(sanitize_path("plain"), "plain");
        assert_eq!(sanitize_path(""), "");
    }

    #[test]
    fn main_steps_get_increasing_frame_ids() {
        let v = JobExecutionView::new(
            "job1",
            &[main("Checkout"), main("Build")],
            &[post("Complete job")],
            &[],
        );
        let l1 = v.line_for_frame(1).unwrap();
        let l2 = v.line_for_frame(2).unwrap();
        assert_eq!(l1.display_name, "Set up job");
        assert_eq!(l2.display_name, "Checkout");
        assert_ne!(l1.frame_id, l2.frame_id);
    }

    #[test]
    fn predicted_post_step_claims_frame_id_when_resolved() {
        let v = JobExecutionView::new(
            "job1",
            &[main("Build")],
            &[],
            &[predicted("Upload artifact", 999)],
        );
        // The predicted post step was assigned frame_id 999 at init time.
        let pred_line = v.line_for_frame(999).unwrap();
        assert_eq!(pred_line.display_name, "Upload artifact");
        // Now the real post step registers. It should claim the same frame id.
        v.add_post_steps(&[post("Upload artifact")]);
        // The new entry still resolves to frame_id 999 because of the claim.
        let resolved = v.line_for_frame(999).unwrap();
        assert_eq!(resolved.display_name, "Upload artifact");
    }

    #[test]
    fn source_content_lists_all_sections() {
        let v = JobExecutionView::new(
            "j",
            &[main("Build")],
            &[post("Notify")],
            &[predicted("Cleanup", 50)],
        );
        let content = v.content();
        assert!(content.contains("# Synthetic execution view"));
        assert!(content.contains("pre:"));
        assert!(content.contains("- Set up job"));
        assert!(content.contains("main:"));
        assert!(content.contains("- Build"));
        assert!(content.contains("post:"));
        assert!(content.contains("- Notify"));
        assert!(content.contains("- Cleanup"));
    }

    #[test]
    fn source_path_includes_sanitized_job_id() {
        let v = JobExecutionView::new("my/job", &[], &[], &[]);
        assert_eq!(v.source_path(), "my_job/execution.yml");
    }
}
