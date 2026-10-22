//! The prompt surface the setup wizards write through.
//!
//! `preloop init` and `preloop setup github` used to read stdin and write
//! stdout directly, which made them impossible to drive from anything but a
//! terminal. Every question and every line of output now goes through
//! [`Prompter`], so a host program (the TUI) can answer the same questions
//! with its own widgets while the CLI keeps its byte-for-byte terminal
//! behaviour in [`StdioPrompter`].
//!
//! Two rules keep the two hosts honest:
//!
//! - a prompt is a *question*, not a layout: [`StdioPrompter`] renders the
//!   terminal's own punctuation, indentation and retry messages, and a host
//!   renders its own;
//! - `text` is emitted verbatim, so a caller that wants the terminal's
//!   indent supplies it. The wizard's own helpers do exactly that.
//!
//! Cancelling (an unanswered prompt, EOF on stdin, the host's Cancel) is not a
//! failure of the wizard: it is [`WizardCancelled`], which the callers turn
//! into "nothing was written" rather than an error report.

use anyhow::Context;
use std::io::{BufRead, IsTerminal, Write};

/// How a note reads. The terminal host prints the text alone; a graphical
/// host is expected to colour it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NoteLevel {
    Info,
    Success,
    Warning,
    Error,
}

/// One selectable answer to a [`Prompter::choice`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChoiceOption {
    pub label: String,
    /// The one line that explains the choice. A choice that cannot run here
    /// carries the reason in the same line, so the host never has to guess
    /// why it is refused.
    pub detail: String,
    pub enabled: bool,
}

impl ChoiceOption {
    /// A choice the operator can pick.
    pub(crate) fn new(label: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            detail: detail.into(),
            enabled: true,
        }
    }

    /// A choice shown but refused, with the reason the operator needs to make
    /// it available (`no Dockerfile at ./Dockerfile`, `docker is not on PATH`).
    /// Shown, never hidden: the reason is the instruction.
    pub(crate) fn unavailable(
        label: impl Into<String>,
        detail: impl Into<String>,
        reason: impl AsRef<str>,
    ) -> Self {
        Self {
            label: label.into(),
            detail: format!("{} [unavailable: {}]", detail.into(), reason.as_ref()),
            enabled: false,
        }
    }
}

/// The operator stopped the wizard (or the input it was reading ended).
///
/// A caller reports this as "nothing was written" and returns; it is not a
/// programming error and not a failed step.
#[derive(Debug)]
pub(crate) struct WizardCancelled {
    message: String,
}

impl WizardCancelled {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for WizardCancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WizardCancelled {}

/// The error a prompt returns when the host cancelled it.
///
/// A host adapter calls this when its user cancels; the terminal host builds
/// the same error with its own wording when stdin closes.
#[allow(dead_code)]
pub(crate) fn cancelled() -> anyhow::Error {
    cancelled_with("cancelled before the wizard finished; nothing was written")
}

/// [`cancelled`] with the host's own reason.
pub(crate) fn cancelled_with(message: impl Into<String>) -> anyhow::Error {
    WizardCancelled::new(message).into()
}

/// Whether `error` is a cancellation rather than a failure: a host that
/// cancelled wants "nothing was written", not an error report.
#[allow(dead_code)]
pub(crate) fn is_cancelled(error: &anyhow::Error) -> bool {
    error.downcast_ref::<WizardCancelled>().is_some()
}

/// Everything the wizard says and asks.
///
/// Implementations are called from the wizard's own thread, one at a time:
/// a prompt blocks until it is answered, and `progress` marks a wait whose end
/// is the next call.
pub(crate) trait Prompter {
    /// A headed step. `step`/`total` are the wizard's numbering (`1` of `5`);
    /// both are `0` for a heading that is not a numbered step.
    fn section(&mut self, step: usize, total: usize, title: &str);

    /// One line of output, emitted verbatim.
    ///
    /// A caller that wants the terminal's four-space indent supplies it (the
    /// wizard's own helpers do); a host that lays text out itself may trim
    /// leading whitespace.
    fn note(&mut self, level: NoteLevel, text: &str);

    /// Pick one of `options`. `default` is the index Enter keeps.
    ///
    /// The implementation refuses a disabled option and an out-of-range
    /// answer itself; the wizard also re-checks, so a host that returns one
    /// cannot configure something unavailable.
    fn choice(
        &mut self,
        prompt: &str,
        options: &[ChoiceOption],
        default: usize,
    ) -> anyhow::Result<usize>;

    /// A yes/no answer. `default` is what Enter means.
    fn confirm(&mut self, prompt: &str, default: bool) -> anyhow::Result<bool>;

    /// One line of free text.
    ///
    /// `default` is what a blank answer means (`None` leaves the blank
    /// alone, for prompts where blank has its own meaning); `help` is the
    /// explanation to show before the prompt.
    fn text(
        &mut self,
        prompt: &str,
        default: Option<&str>,
        help: Option<&str>,
    ) -> anyhow::Result<String>;

    /// One secret value: never echoed back, never logged.
    fn secret(&mut self, prompt: &str) -> anyhow::Result<String>;

    /// A wait with no measurable progress; the next call ends it.
    fn progress(&mut self, label: &str);
}

/// The terminal host: stdin, stdout, and the same prompts `preloop init` has
/// always printed.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StdioPrompter;

impl StdioPrompter {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Print a prompt and read one line, trimmed. EOF is a cancellation: a
    /// wizard half-answered by a closed pipe must not go on to write config.
    fn read_line(&self, prompt: &str) -> anyhow::Result<String> {
        print!("    {prompt}");
        std::io::stdout().flush().context("flushing stdout")?;
        let mut line = String::new();
        let read = std::io::stdin()
            .lock()
            .read_line(&mut line)
            .context("reading stdin")?;
        if read == 0 {
            return Err(cancelled_with(
                "input closed before the wizard finished; nothing was written",
            ));
        }
        Ok(line.trim().to_owned())
    }
}

impl Prompter for StdioPrompter {
    fn section(&mut self, step: usize, total: usize, title: &str) {
        if step > 0 && total > 0 {
            println!("\n{step}/{total} {title}");
        } else {
            println!("\n{title}");
        }
    }

    fn note(&mut self, _level: NoteLevel, text: &str) {
        println!("{text}");
    }

    fn choice(
        &mut self,
        _prompt: &str,
        options: &[ChoiceOption],
        default: usize,
    ) -> anyhow::Result<usize> {
        loop {
            for (index, option) in options.iter().enumerate() {
                // A choice that cannot run here is shown, not hidden: the
                // reason ("no Dockerfile at ./Dockerfile", "docker is not on
                // PATH") is what tells the operator how to get it.
                println!("    {}) {} — {}", index + 1, option.label, option.detail);
            }
            let answer = self.read_line(&format!("choice [{}]: ", default + 1))?;
            let chosen = if answer.is_empty() {
                default
            } else {
                match answer.trim().parse::<usize>() {
                    Ok(value) if value >= 1 && value <= options.len() => value - 1,
                    _ => {
                        println!("    enter a number between 1 and {}", options.len());
                        continue;
                    }
                }
            };
            if !options[chosen].enabled {
                println!("    {} is unavailable: pick another", options[chosen].label);
                continue;
            }
            return Ok(chosen);
        }
    }

    fn confirm(&mut self, prompt: &str, default: bool) -> anyhow::Result<bool> {
        loop {
            let answer = self.read_line(prompt)?;
            match answer.to_ascii_lowercase().as_str() {
                "" => return Ok(default),
                "y" | "yes" => return Ok(true),
                "n" | "no" => return Ok(false),
                _ => println!("    answer y or n"),
            }
        }
    }

    fn text(
        &mut self,
        prompt: &str,
        default: Option<&str>,
        help: Option<&str>,
    ) -> anyhow::Result<String> {
        if let Some(help) = help {
            for line in help.lines() {
                println!("    {line}");
            }
        }
        let answer = self.read_line(prompt)?;
        Ok(match default {
            Some(default) if answer.is_empty() => default.to_owned(),
            _ => answer,
        })
    }

    fn secret(&mut self, prompt: &str) -> anyhow::Result<String> {
        // Echoing a secret leaves it in the scrollback; hide it when a human
        // is typing, but keep plain stdin for pipes so automation still works.
        if std::io::stdin().is_terminal() {
            return Ok(rpassword::prompt_password(prompt)?.trim().to_owned());
        }
        let mut line = String::new();
        let read = std::io::stdin()
            .lock()
            .read_line(&mut line)
            .context("reading stdin")?;
        if read == 0 {
            return Err(cancelled_with(
                "input closed before the secret was read; nothing was written",
            ));
        }
        Ok(line.trim().to_owned())
    }

    fn progress(&mut self, _label: &str) {
        // The terminal already shows the work: a build log streams, and the
        // waits print the line that says what they are waiting for.
    }
}

/// A prompter that records what the wizard said instead of printing it, and
/// refuses to prompt. `preloop init --json` runs the reused setup flow through
/// this so its human output becomes the step's `log` field instead of
/// corrupting the one-object-per-line contract.
#[derive(Debug, Default)]
pub(crate) struct CapturePrompter {
    log: String,
}

impl CapturePrompter {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The text captured so far.
    pub(crate) fn log(self) -> String {
        self.log
    }

    fn record(&mut self, text: &str) {
        self.log.push_str(text);
        self.log.push('\n');
    }
}

impl Prompter for CapturePrompter {
    fn section(&mut self, step: usize, total: usize, title: &str) {
        if step > 0 && total > 0 {
            self.record(&format!("{step}/{total} {title}"));
        } else {
            self.record(title);
        }
    }

    fn note(&mut self, _level: NoteLevel, text: &str) {
        self.record(text);
    }

    fn choice(
        &mut self,
        prompt: &str,
        _options: &[ChoiceOption],
        _default: usize,
    ) -> anyhow::Result<usize> {
        Err(cancelled_with(format!(
            "`{prompt}` needs a terminal, and this run has none"
        )))
    }

    fn confirm(&mut self, prompt: &str, _default: bool) -> anyhow::Result<bool> {
        Err(cancelled_with(format!(
            "`{prompt}` needs a terminal, and this run has none"
        )))
    }

    fn text(
        &mut self,
        prompt: &str,
        _default: Option<&str>,
        _help: Option<&str>,
    ) -> anyhow::Result<String> {
        Err(cancelled_with(format!(
            "`{prompt}` needs a terminal, and this run has none"
        )))
    }

    fn secret(&mut self, prompt: &str) -> anyhow::Result<String> {
        Err(cancelled_with(format!(
            "`{prompt}` needs a terminal, and this run has none"
        )))
    }

    fn progress(&mut self, _label: &str) {}
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::collections::VecDeque;

    /// One scripted answer.
    #[derive(Debug, Clone)]
    pub(crate) enum Answer {
        Choice(usize),
        Confirm(bool),
        Text(&'static str),
        Secret(&'static str),
        Cancel,
    }

    /// Records everything the wizard said and answers its prompts from a
    /// script, so a flow can be driven without a terminal.
    #[derive(Debug, Default)]
    pub(crate) struct TestPrompter {
        sections: Vec<String>,
        notes: Vec<(NoteLevel, String)>,
        progress: Vec<String>,
        answers: VecDeque<Answer>,
        prompts: Vec<String>,
    }

    impl TestPrompter {
        pub(crate) fn new(answers: impl IntoIterator<Item = Answer>) -> Self {
            Self {
                answers: answers.into_iter().collect(),
                ..Self::default()
            }
        }

        /// Every line the wizard emitted, in order, joined by newlines.
        pub(crate) fn log(&self) -> String {
            self.sections
                .iter()
                .chain(self.notes.iter().map(|(_, text)| text))
                .cloned()
                .collect::<Vec<_>>()
                .join("\n")
        }

        pub(crate) fn prompts(&self) -> &[String] {
            &self.prompts
        }

        pub(crate) fn remaining_answers(&self) -> usize {
            self.answers.len()
        }

        fn next(&mut self, prompt: &str) -> anyhow::Result<Answer> {
            self.prompts.push(prompt.to_owned());
            self.answers.pop_front().ok_or_else(|| {
                anyhow::anyhow!("the wizard asked `{prompt}` and the script had no answer left")
            })
        }
    }

    impl Prompter for TestPrompter {
        fn section(&mut self, step: usize, total: usize, title: &str) {
            if step > 0 && total > 0 {
                self.sections.push(format!("{step}/{total} {title}"));
            } else {
                self.sections.push(title.to_owned());
            }
        }

        fn note(&mut self, level: NoteLevel, text: &str) {
            self.notes.push((level, text.to_owned()));
        }

        fn choice(
            &mut self,
            prompt: &str,
            options: &[ChoiceOption],
            _default: usize,
        ) -> anyhow::Result<usize> {
            match self.next(prompt)? {
                Answer::Choice(index) if index < options.len() => Ok(index),
                Answer::Choice(index) => {
                    anyhow::bail!("the script chose {index}, out of {} options", options.len())
                }
                Answer::Cancel => Err(cancelled()),
                _ => anyhow::bail!("the script answered `{prompt}` with the wrong kind of answer"),
            }
        }

        fn confirm(&mut self, prompt: &str, _default: bool) -> anyhow::Result<bool> {
            match self.next(prompt)? {
                Answer::Confirm(value) => Ok(value),
                Answer::Cancel => Err(cancelled()),
                _ => anyhow::bail!("the script answered `{prompt}` with the wrong kind of answer"),
            }
        }

        fn text(
            &mut self,
            prompt: &str,
            default: Option<&str>,
            _help: Option<&str>,
        ) -> anyhow::Result<String> {
            match self.next(prompt)? {
                Answer::Text(value) => Ok(match default {
                    Some(default) if value.is_empty() => default.to_owned(),
                    _ => value.to_owned(),
                }),
                Answer::Cancel => Err(cancelled()),
                _ => anyhow::bail!("the script answered `{prompt}` with the wrong kind of answer"),
            }
        }

        fn secret(&mut self, prompt: &str) -> anyhow::Result<String> {
            match self.next(prompt)? {
                Answer::Secret(value) => Ok(value.to_owned()),
                Answer::Cancel => Err(cancelled()),
                _ => anyhow::bail!("the script answered `{prompt}` with the wrong kind of answer"),
            }
        }

        fn progress(&mut self, label: &str) {
            self.progress.push(label.to_owned());
        }
    }

    #[test]
    fn a_scripted_answer_reaches_the_right_question() {
        let mut prompter = TestPrompter::new([
            Answer::Confirm(true),
            Answer::Choice(1),
            Answer::Text("value"),
        ]);
        assert!(prompter.confirm("change? ", false).unwrap());
        assert_eq!(
            prompter
                .choice(
                    "which?",
                    &[ChoiceOption::new("a", ""), ChoiceOption::new("b", "")],
                    0
                )
                .unwrap(),
            1
        );
        assert_eq!(prompter.text("what? ", None, None).unwrap(), "value");
        assert_eq!(prompter.log(), "", "no note was recorded");
        assert_eq!(prompter.remaining_answers(), 0);
        assert_eq!(prompter.prompts(), ["change? ", "which?", "what? "]);
    }

    #[test]
    fn a_prompt_the_script_did_not_plan_for_fails_loudly() {
        let mut prompter = TestPrompter::new([]);
        let error = prompter.confirm("change? ", false).unwrap_err();
        assert!(error.to_string().contains("change?"), "{error}");
    }

    #[test]
    fn an_unavailable_choice_is_refused_by_the_scripted_host_too() {
        let mut prompter = TestPrompter::new([Answer::Choice(7)]);
        let error = prompter
            .choice("which?", &[ChoiceOption::new("a", "")], 0)
            .unwrap_err();
        assert!(error.to_string().contains("out of"), "{error}");
    }

    #[test]
    fn a_cancelled_secret_is_the_wizard_cancelled_error() {
        let mut prompter = TestPrompter::new([Answer::Cancel]);
        let error = prompter.secret("token: ").unwrap_err();
        assert!(is_cancelled(&error), "{error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The terminal renderer's own contract, without a terminal: what the
    /// caller passes is what is printed, and an unavailable choice says why.
    #[test]
    fn a_choice_option_carries_the_reason_it_is_unavailable() {
        let option = ChoiceOption::unavailable(
            "Dockerfile in this repo",
            "build it here and use the saved tar",
            "no Dockerfile at ./Dockerfile",
        );
        assert!(!option.enabled);
        assert_eq!(
            option.detail,
            "build it here and use the saved tar [unavailable: no Dockerfile at ./Dockerfile]"
        );
        let enabled = ChoiceOption::new("Official", "drop-in parity");
        assert!(enabled.enabled);
        assert_eq!(enabled.detail, "drop-in parity");
    }

    /// A capture host records instead of printing, so `--json` can carry the
    /// reused flow's output as a field.
    #[test]
    fn a_capture_prompter_records_what_the_wizard_says() {
        let mut prompter = CapturePrompter::new();
        prompter.section(1, 5, "Credentials");
        prompter.note(NoteLevel::Info, "    current: GitHub App 42");
        prompter.section(0, 0, "wrote /tmp/config.toml");
        assert_eq!(
            prompter.log(),
            "1/5 Credentials\n    current: GitHub App 42\nwrote /tmp/config.toml\n"
        );
    }

    /// A capture host cannot ask a question: a run that needs one is
    /// cancelled rather than answered with a made-up value.
    #[test]
    fn a_capture_prompter_refuses_to_ask() {
        let mut prompter = CapturePrompter::new();
        let error = prompter.confirm("change? ", false).unwrap_err();
        assert!(is_cancelled(&error), "{error}");
        assert!(error.to_string().contains("change?"), "{error}");
    }

    #[test]
    fn a_cancellation_is_recognised_and_a_failure_is_not() {
        assert!(is_cancelled(&cancelled()));
        assert!(!is_cancelled(&anyhow::anyhow!("boom")));
        assert!(
            cancelled().to_string().contains("nothing was written"),
            "the message has to say the config is untouched"
        );
    }
}
