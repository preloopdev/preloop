//! Preloop CI command-line interface.

use anyhow::Context;
use base64::Engine as _;
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use preloop_gha_protocol::{ExecutionStatus, NdjsonEvent, RunAccepted, RunId, WorkflowSubmission};
use preloop_orchestrator::environment::{is_stock_base_image, DEFAULT_BASE_IMAGE};
use preloop_orchestrator::{artifact_payload, RunnerPool, RunnerPoolConfig};
use preloop_vm::SmolVmProvider;
use rand::RngCore;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

mod app_manifest;
mod dap_client;
mod debug_session;
mod github_auth;
mod github_setup;
mod push;
mod server_install;
mod update;

pub(crate) fn server_url() -> String {
    std::env::var("PRELOOP_URL").unwrap_or_else(|_| "http://127.0.0.1:9090".to_owned())
}

fn should_send_local_workspace_header(url: &str, uses_default_transport: bool) -> bool {
    if uses_default_transport {
        return true;
    }

    // An explicit loopback URL is still a local engine invocation. Do not
    // trust a host path for an arbitrary remote server.
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let Some(host) = parsed.host_str() else {
        return false;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn mounted_control_origin(public_url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(public_url).ok()?;
    let host = parsed.host_str()?;
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    loopback.then(|| public_url.trim_end_matches('/').to_owned())
}

// ── Pre-push hook ──────────────────────────────────────────────────────────
const HOOK_MARKER: &str = "# pre-push hook: soft CI gate for preloop";

/// Marker the pre-push hook greps for to decide fail-open. Emitted only when
/// the engine itself cannot be reached (connection-level failure); a CI step
/// that happens to print a network error must never look like an outage.
pub(crate) const ENGINE_UNREACHABLE_MARKER: &str = "PRELOOP_UNREACHABLE";

fn engine_unreachable_context() -> String {
    format!("preloop engine unreachable ({ENGINE_UNREACHABLE_MARKER})")
}

const PRE_PUSH_HOOK: &str = r#"#!/usr/bin/env bash
# pre-push hook: soft CI gate for preloop.
set -euo pipefail

# Chain a previously installed hook (another tool's), if any: run it first
# with the same stdin refs; its verdict is authoritative.
prev_hook="$(dirname "$0")/pre-push.preloop-prev"
if [[ -x "$prev_hook" ]]; then
  refs="$(cat)"
  if printf '%s\n' "$refs" | "$prev_hook" "$@"; then
    :
  else
    previous_status=$?
    exit "$previous_status"
  fi
else
  refs="$(cat)"
fi

preloop_bin="${PRELOOP_BIN:-preloop}"
endpoint="${PRELOOP_URL:-http://127.0.0.1:9090}"
remote_url="${2:-unknown-remote}"
cache_dir="${XDG_CACHE_HOME:-$HOME/.cache}/preloop-push"
mkdir -p "$cache_dir"
zero_sha=0000000000000000000000000000000000000000

# The gate only validates pushes of the checked-out branch's current commit:
# for any other ref the tree CI would test is not the tree being pushed, and
# a soft gate must not brick legitimate pushes it cannot verify — those pass
# with a loud note. The gate itself never pushes anything (no push-back): it
# holds `git push` open while CI runs and lets git proceed only when CI is
# green. Cached verdicts are scoped to (remote, endpoint, sha) so a result
# from another repository, server, or commit is never reused.
head_branch="$(git symbolic-ref --short HEAD 2>/dev/null || true)"
head_sha="$(git rev-parse HEAD 2>/dev/null || true)"

run_ci_or_resume() {
  local local_sha="$1" branch="$2" remote_sha="$3"
  local log_range
  # [skip ci] in any pushed commit bypasses the gate (mirrors preloop's
  # server-side skip-label handling). A new branch carries an all-zero
  # remote sha — log the single revision, not an invalid range.
  if [[ "$remote_sha" == "$zero_sha" ]]; then
    log_range="$local_sha"
  else
    log_range="${remote_sha}..$local_sha"
  fi
  if git log --format=%B "$log_range" 2>/dev/null | grep -qi '\[skip *ci\]'; then
    echo "preloop: [skip ci] found — pushing ${branch} without the CI gate"
    return 0
  fi

  # Scoped cache: keyed by (remote, endpoint, sha), recording the run id.
  # Only a terminal-success verdict for this exact destination skips CI; a
  # recorded run is validated live (and resumed) instead of trusting a bare
  # "the hook exited 0" marker that an interruption could skip writing.
  local cache_key cache run_id verdict
  get_verdict() {
    local status_err status_rc status_output
    status_err="$(mktemp "${TMPDIR:-/tmp}/preloop-status-XXXXXX")"
    set +e
    status_output="$(PRELOOP_URL="$endpoint" "$preloop_bin" status "$1" 2>"$status_err")"
    status_rc=$?
    set -e
    if [[ $status_rc -eq 0 ]]; then
      printf '%s\n' "$status_output"
    elif grep -q "$ENGINE_UNREACHABLE_MARKER" "$status_err"; then
      printf 'unreachable\n'
    else
      printf 'error\n'
    fi
    rm -f "$status_err"
  }
  cache_key="$(printf '%s\n' "$remote_url" "$endpoint" "$local_sha" | git hash-object --stdin)"
  cache="$cache_dir/$cache_key"
  if [[ -f "$cache" ]]; then
    IFS=$'\t' read -r run_id cached_remote cached_endpoint < "$cache" || true
    if [[ "$cached_remote" == "$remote_url" && "$cached_endpoint" == "$endpoint" ]]; then
      verdict="$(get_verdict "$run_id")"
      case "$verdict" in
        success)
          echo "preloop: cached CI verdict for ${local_sha:0:12}: success"
          return 0 ;;
        failure|cancelled|skipped)
          echo "preloop: cached CI verdict for ${local_sha:0:12}: ${verdict} — fix and re-push (or rm $cache)" >&2
          return 1 ;;
        in_progress|queued|pending)
          echo "preloop: run ${run_id} still in progress — waiting for CI before pushing ${branch}..."
          waited=0
          timeout_secs="${PRELOOP_HOOK_TIMEOUT_SECS:-3600}"
          while (( waited < timeout_secs )); do
            sleep 15
            waited=$(( waited + 15 ))
            verdict="$(get_verdict "$run_id")"
            case "$verdict" in
              success)
                echo "preloop: CI passed for ${branch}"
                return 0 ;;
              failure|cancelled|skipped)
                echo "preloop: cached CI verdict for ${local_sha:0:12}: ${verdict} — fix and re-push (or rm $cache)" >&2
                return 1 ;;
              in_progress|queued|pending) ;;
              unreachable)
                echo "preloop: cannot reach the engine to resume run ${run_id} — pushing ${branch} WITHOUT the CI gate (fail-open)" >&2
                return 0 ;;
              error)
                echo "preloop: cannot validate cached run ${run_id} — push aborted" >&2
                return 1 ;;
            esac
          done
          echo "preloop: CI did not finish within ${timeout_secs}s — push aborted" >&2
          return 1 ;;
        error)
          echo "preloop: cannot resolve run ${run_id} — running CI fresh" >&2 ;;
      esac
    fi
  fi

  # Run CI. Fail open only on the machine-readable engine-unreachable marker
  # emitted by preloop itself — never on CI step output that happens to
  # mention a network error.
  local log error_log run_pid run_status i
  log="$(mktemp "${TMPDIR:-/tmp}/preloop-push-XXXXXX")"
  error_log="$(mktemp "${TMPDIR:-/tmp}/preloop-push-error-XXXXXX")"
  echo "preloop: running CI on preloop before pushing ${branch} (this holds the push)..."
  set +e
  "$preloop_bin" run >"$log" 2>"$error_log" &
  run_pid=$!
  # Record the run id as soon as the engine accepts it, so an interrupted
  # hook (Ctrl-C, crash) resumes the same run on the next push instead of
  # duplicating CI.
  for i in $(seq 1 300); do
    run_id="$(grep -oE 'Run [0-9a-fA-F-]{36} created' "$log" 2>/dev/null | head -1 | awk '{print $2}' || true)"
    [[ -n "$run_id" ]] && break
    sleep 0.2
  done
  if [[ -n "$run_id" ]]; then
    printf '%s\t%s\t%s\n' "$run_id" "$remote_url" "$endpoint" > "$cache"
  fi
  wait "$run_pid"
  run_status=$?
  set -e
  if grep -q "$ENGINE_UNREACHABLE_MARKER" "$error_log"; then
    echo "preloop: engine unreachable — pushing ${branch} WITHOUT the CI gate (fail-open)" >&2
    rm -f "$log" "$error_log"
    return 0
  fi
  if [[ $run_status -ne 0 ]]; then
    echo "preloop: CI failed for ${branch} — push aborted. Details:" >&2
    tail -n 30 "$log" >&2
    tail -n 30 "$error_log" >&2
    rm -f "$log" "$error_log"
    return 1
  fi
  rm -f "$log" "$error_log"
  return 0
}

while read -r local_ref local_sha _remote_ref remote_sha; do
  case "$local_ref" in refs/heads/*) ;; *) continue ;; esac
  [[ "$local_sha" != "$zero_sha" ]] || continue
  branch="${local_ref#refs/heads/}"
  if [[ "$branch" != "$head_branch" || "$local_sha" != "$head_sha" ]]; then
    echo "preloop: not gating push of ${branch} (only the checked-out branch is validated)" >&2
    continue
  fi
  if ! run_ci_or_resume "$local_sha" "$branch" "$remote_sha"; then
    exit 1
  fi
done <<< "$refs"
exit 0
"#;

fn hook_installed() -> bool {
    resolve_hooks_dir()
        .ok()
        .and_then(|dir| std::fs::read_to_string(dir.join("pre-push")).ok())
        .is_some_and(|content| content.contains(HOOK_MARKER))
}

fn hook_decided() -> bool {
    fn git_config_flag(key: &str) -> bool {
        std::process::Command::new("git")
            .args(["config", "--local", "--get", key])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .is_some_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "true")
    }

    git_config_flag("preloop.hook-installed") || git_config_flag("preloop.hook-declined")
}

fn set_hook_decision(key: &str) {
    let _ = std::process::Command::new("git")
        .args(["config", "--local", key, "true"])
        .status();
}

fn mark_hook_installed() {
    set_hook_decision("preloop.hook-installed");
}

fn mark_hook_declined() {
    set_hook_decision("preloop.hook-declined");
}

fn install_hook() -> anyhow::Result<()> {
    // `core.hooksPath` moves every hook out of `.git/hooks` (a global
    // `core.hooksPath` like `~/.config/git/hooks` is common); writing to
    // `.git/hooks` would leave the gate unused. Resolve the directory git
    // actually consults, matching its own precedence.
    let hooks_dir = resolve_hooks_dir()?;
    std::fs::create_dir_all(&hooks_dir).context("create hooks directory")?;
    let hook_path = hooks_dir.join("pre-push");
    // Preserve a pre-existing hook (installed by another tool): chain it
    // instead of clobbering repository checks. Reinstalling over our own
    // hook backs nothing up (idempotent).
    let existing_hook = std::fs::read(&hook_path).ok();
    if existing_hook
        .as_deref()
        .is_some_and(|bytes| !String::from_utf8_lossy(bytes).contains(HOOK_MARKER))
    {
        let backup = hooks_dir.join("pre-push.preloop-prev");
        std::fs::copy(&hook_path, &backup).context("back up previous pre-push hook")?;
    }
    std::fs::write(&hook_path, PRE_PUSH_HOOK).context("write pre-push hook")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&hook_path)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&hook_path, perms)?;
    }
    mark_hook_installed();
    Ok(())
}

/// The directory git runs hooks from, resolved by Git itself.
fn resolve_hooks_dir() -> anyhow::Result<std::path::PathBuf> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--git-path", "hooks"])
        .output()
        .context("resolve Git hooks directory")?;
    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if !path.is_empty() {
            return Ok(std::path::PathBuf::from(path));
        }
    }
    anyhow::bail!(
        "git rev-parse --git-path hooks failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

fn maybe_offer_hook() -> anyhow::Result<()> {
    if hook_installed() || hook_decided() {
        return Ok(());
    }
    if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        return Ok(());
    }
    eprintln!(
        "preloop: add preloop CI as a pre-push hook to run CI on your working tree before\n\
         \x20 committing? This will run CI on preloop before every `git push`."
    );
    eprint!("         [y/N] ");
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    match answer.trim().to_ascii_lowercase().as_str() {
        "y" | "yes" => {
            install_hook()?;
            eprintln!("preloop: installed .git/hooks/pre-push");
        }
        _ => {
            mark_hook_declined();
            eprintln!("preloop: not installing (won't ask again for this repo)");
        }
    }
    Ok(())
}

pub(crate) fn api_token() -> Option<String> {
    std::env::var("PRELOOP_TOKEN")
        .or_else(|_| std::env::var("PRELOOP_SYSTEM_TOKEN"))
        .ok()
        .or_else(|| {
            std::fs::read_to_string(preloop_home().join("engine.token"))
                .ok()
                .map(|token| token.trim().to_owned())
                .filter(|token| !token.is_empty())
        })
}

pub(crate) fn build_client() -> reqwest::Client {
    // The CLI talks to the native management surface, which the engine
    // serves on TCP (`server_url`, default 127.0.0.1:9090). The unix socket
    // is the runner/guest surface: it deliberately refuses native APIs
    // (`/api/v1/runs` and friends 404 there), so it must never be the
    // default transport for CLI commands.
    reqwest::Client::builder()
        .build()
        .expect("valid HTTP client configuration")
}

pub(crate) fn preloop_home() -> PathBuf {
    std::env::var_os("PRELOOP_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".preloop")))
        .unwrap_or_else(|| PathBuf::from(".preloop"))
}

/// A `smolvm` command carrying Preloop's Linux VM-sandbox environment.
///
/// Direct `machine exec`/`cp`/`shell` calls can implicitly boot or restart a
/// stopped machine — upstream's exec path connects to the machine, starting
/// it when needed, and `machine shell` does the same — so these spawns must
/// carry the exact seccomp/Landlock/cgroup environment the VM provider
/// applies to its own boots. This is the single chokepoint for them; the
/// policy lives in [`preloop_vm::smolvm_sandbox_env`] and is a no-op on
/// macOS, matching the provider.
pub(crate) fn smolvm_command() -> anyhow::Result<std::process::Command> {
    let mut command = std::process::Command::new("smolvm");
    // The service unit pins `SMOLVM_DATA_DIR=<PRELOOP_HOME>/smolvm`
    // (see crates/preloop-cli/src/server_install.rs), so the engine records
    // its machines in that registry. A separately invoked `preloop shell` /
    // `preloop debug` must consult the SAME registry or it cannot find the
    // paused, service-owned machine. `apply_smolvm_runtime_env` fills the
    // same gap from the effective Preloop home (and isolates `HOME` on macOS,
    // where SmolVM ignores `SMOLVM_DATA_DIR`); an operator value still wins.
    preloop_vm::apply_smolvm_runtime_env(&mut command, None)?;
    preloop_vm::apply_smolvm_sandbox_env(&mut command)?;
    Ok(command)
}

#[cfg(test)]
pub(crate) static SMOLVM_PATH_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A fake `smolvm` on PATH that records the sandbox environment it was given
/// and answers the debug-session byte-count probe. Serialized with
/// [`SMOLVM_PATH_LOCK`]: `PATH` is process-global and the debug-session and
/// shell tests run in the same binary.
#[cfg(test)]
pub(crate) fn fake_smolvm_on_path() -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("smolvm");
    std::fs::write(
        &executable,
        r##"#!/bin/sh
printf 'SMOLVM_SECCOMP=%s\n' "${SMOLVM_SECCOMP-}" > "$0.seccomp"
printf 'SMOLVM_LANDLOCK=%s\n' "${SMOLVM_LANDLOCK-}" > "$0.landlock"
printf 'SMOLVM_CGROUP_ROOT=%s\n' "${SMOLVM_CGROUP_ROOT-}" > "$0.cgroup"
printf 'SMOLVM_DATA_DIR=%s\n' "${SMOLVM_DATA_DIR-}" > "$0.datadir"
case "$*" in
  *"wc -c"*) printf '12345\n' ;;
esac
exit 0
"##,
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
    use std::os::unix::fs::PermissionsExt;
    permissions.set_mode(0o755);
    std::fs::set_permissions(&executable, permissions).unwrap();
    (directory, executable)
}

/// Run `test` with a fake `smolvm` first on PATH, restoring PATH afterwards.
#[cfg(test)]
pub(crate) fn with_fake_smolvm_path<T>(test: impl FnOnce(&PathBuf) -> T) -> T {
    let _guard = SMOLVM_PATH_LOCK.blocking_lock();
    let (directory, executable) = fake_smolvm_on_path();
    let previous = std::env::var_os("PATH");
    let mut path = directory.path().as_os_str().to_owned();
    path.push(":");
    if let Some(previous) = &previous {
        path.push(previous);
    }
    std::env::set_var("PATH", path);
    let result = test(&executable);
    match previous {
        Some(previous) => std::env::set_var("PATH", previous),
        None => std::env::remove_var("PATH"),
    }
    result
}

#[derive(Debug, Parser)]
#[command(
    name = "preloop",
    about = "Local CI with hardware isolation",
    version = env!("CARGO_PKG_VERSION")
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the installed Preloop version.
    Version,

    Run(RunArgs),

    /// Show the expanded job DAG without executing.
    Plan(PlanArgs),

    /// Show operational status, queue health, and recent runs.
    Status(StatusArgs),

    Logs(LogsArgs),

    /// Cancel the current run.
    Cancel(CancelArgs),

    /// Manage the local secret store.
    Secret(github_setup::SecretArgs),

    /// Configure GitHub credentials (App or fine-grained PAT).
    Setup(github_setup::SetupArgs),

    /// Verify the GitHub credential configuration.
    Doctor(github_setup::DoctorArgs),

    /// Install or remove the control plane as a supervised service.
    ///
    /// `install` scaffolds hardened systemd units (Linux) or a LaunchDaemon
    /// (macOS), a private environment file, and an optional self-update
    /// timer. `--user` installs rootless per-user services (systemd user
    /// units / a LaunchAgent) with state in ~/.preloop. `uninstall` removes
    /// them without touching PRELOOP_HOME data unless asked.
    Server(server_install::ServerArgs),

    /// Open a shell in a preserved VM.
    Shell(ShellArgs),

    /// Attach to a job paused at a failed step: inspect, fix, retry.
    Debug(debug_session::DebugArgs),

    /// Attach an interactive DAP client to a debugger-enabled run.
    Dap(dap_client::DapArgs),

    /// Publish a completed run's result to GitHub: push the tested commit,
    /// create or update the pull request, and report check runs.
    ///
    /// Defaults to the most recent run. Re-running is safe — every step is
    /// idempotent.
    Push(PushArgs),

    /// Poll GitHub Releases and atomically install the matching binary.
    Update(update::UpdateArgs),

    /// Run the control plane and microVM runner pool in the foreground.
    ///
    /// This is the self-hosting entry point: it serves the GitHub webhook and
    /// Checks endpoints and provisions a microVM per queued job. Point a
    /// tunnel or reverse proxy at `--listen` to receive events from GitHub.
    Serve(ServeArgs),

    /// Former name for `serve`. Retained because `ensure_engine_running`
    /// spawns it by name, and to keep existing supervisor units working.
    #[command(hide = true)]
    Engine,

    /// Build a fresh packed microVM artifact for release automation.
    #[command(hide = true)]
    BuildGolden(BuildGoldenArgs),
}

#[derive(Debug, Parser)]
struct BuildGoldenArgs {
    /// Directory containing the Linux preloop-runner binary.
    #[arg(long)]
    runner_bundle: PathBuf,

    /// Workspace to detect toolchains from (rust-toolchain.toml, .nvmrc, …).
    /// Defaults to the current directory, so the release-golden workflow —
    /// which runs from the repo checkout — bakes the project's toolchains
    /// into the artifact. Without them every fork of the packed golden
    /// reinstalls rust per job.
    #[arg(long)]
    workspace: Option<PathBuf>,

    /// Destination path for the packed artifact.
    #[arg(long)]
    output: PathBuf,

    /// OCI base image or packed artifact to use. Explicit CLI input overrides
    /// PRELOOP_RUNNER_BASE_IMAGE.
    #[arg(
        long,
        env = "PRELOOP_RUNNER_BASE_IMAGE",
        default_value = DEFAULT_BASE_IMAGE
    )]
    base_image: String,

    /// Persistent guest storage in GiB. Large official runner snapshots may
    /// need 80 GiB or more.
    #[arg(long = "storage-gb", env = "PRELOOP_RUNNER_STORAGE_GB")]
    storage_gib: Option<u32>,
}

#[derive(Debug, Default, clap::Args)]
struct ServeArgs {
    /// Address to bind. Overrides PRELOOP_LISTEN.
    #[arg(long, value_name = "ADDR")]
    listen: Option<String>,

    /// Externally reachable base URL. Overrides PRELOOP_PUBLIC_URL.
    ///
    /// Must be the address GitHub and any remote runners can reach — a
    /// loopback URL here is only correct when everything is on this host.
    #[arg(long, value_name = "URL")]
    public_url: Option<String>,

    /// GitHub App id.
    #[arg(long, value_name = "ID")]
    github_app_id: Option<String>,

    /// Path to the GitHub App private key PEM.
    #[arg(long, value_name = "PATH")]
    github_app_key: Option<PathBuf>,

    /// Installation id. Skips installation discovery when supplied.
    #[arg(long, value_name = "ID")]
    github_app_installation_id: Option<u64>,

    /// Shared secret for verifying `X-Hub-Signature-256`.
    #[arg(long, value_name = "SECRET")]
    webhook_secret: Option<String>,

    /// Persist the supplied GitHub credentials so later runs reuse them.
    #[arg(long)]
    save: bool,

    /// Durable-state backend: `sqlite://<path>`, a bare path, or
    /// `postgres://…` (with optional `?sslmode=require|verify-full`).
    /// Defaults to `PRELOOP_STORE_URL`, then to SQLite in the state dir.
    #[arg(long, value_name = "URL")]
    store: Option<String>,
}

#[derive(Debug, Parser)]
struct RunArgs {
    /// Workflow file path. Bare filenames resolve inside .github/workflows/.
    #[arg(short = 'f', long = "file")]
    file: Option<PathBuf>,

    /// Run a single job by its YAML key. Includes the needs: dependency closure.
    #[arg(long)]
    job: Option<String>,

    /// Simulate a trigger event (push, pull_request, merge_group, …).
    #[arg(long)]
    event: Option<String>,

    /// Event payload JSON file (webhook body) for the simulated trigger.
    #[arg(long, value_name = "PATH")]
    payload: Option<PathBuf>,

    /// Base ref for pull_request or merge_group events.
    #[arg(long)]
    base: Option<String>,

    /// Tear down on failure instead of pausing for debugging.
    ///
    /// Pausing is the default in a terminal: a failed step holds its microVM
    /// open so you can fix and retry from that step. Non-interactive runs
    /// (`--detach`, pipes, CI) never pause, so nothing hangs.
    #[arg(long)]
    no_debug: bool,

    /// Keep the failed job VM alive even when nothing can attach interactively.
    ///
    /// Pausing already implies this for an interactive run. Pass it to hold a
    /// VM open for a later `preloop shell` from a detached or piped run, which
    /// otherwise tears down because there is nobody to answer the pause.
    #[arg(long)]
    preserve_on_failure: bool,

    /// Inline secret as NAME=VALUE. Repeatable.
    #[arg(long = "secret", value_name = "NAME=VALUE")]
    secrets: Vec<String>,

    /// Submit and return immediately without streaming events.
    #[arg(short = 'd', long)]
    detach: bool,

    /// After the run completes, push the tested commit to GitHub and
    /// publish the result: create or update the pull request for the branch
    /// and report check runs for the commit. A dirty working tree is
    /// allowed: CI runs on a snapshot of the uncommitted state, and the
    /// pushed commit carries exactly that tested tree. Requires a GitHub
    /// origin.
    #[arg(long)]
    push: bool,

    /// Create a pull request for the branch when none is open. Implies
    /// `--push`.
    #[arg(long)]
    create_pr: bool,

    /// Create newly-created pull requests as drafts, so reviewers are not
    /// notified until you mark them ready. Only affects PR creation.
    ///
    /// A bare `--pr-draft` means draft; `--pr-draft=false` opens new pull
    /// requests ready for review.
    #[arg(
        long,
        num_args = 0..=1,
        default_value_t = true,
        default_missing_value = "true",
        action = clap::ArgAction::Set
    )]
    pr_draft: bool,
}

#[derive(Debug, Parser)]
struct PushArgs {
    /// Run ID. Defaults to the most recent run.
    run_id: Option<String>,
}

#[derive(Debug, Parser)]
struct PlanArgs {
    /// Workflow file path.
    #[arg(short = 'f', long = "file")]
    file: Option<PathBuf>,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Parser)]
struct StatusArgs {
    /// Print the raw status JSON (no prose) for jq/scripting.
    #[arg(long)]
    json: bool,

    /// Number of recent runs to show in the table.
    #[arg(long, default_value = "20")]
    limit: usize,
}

#[derive(Debug, Parser)]
struct LogsArgs {
    /// Run ID. Defaults to the most recent run.
    run_id: Option<String>,

    /// Filter by job ID.
    #[arg(long)]
    job: Option<String>,

    /// Filter by step number.
    #[arg(long)]
    step: Option<u32>,
}

#[derive(Debug, Parser)]
struct CancelArgs {
    /// Run ID. Defaults to the most recent active run.
    run_id: Option<String>,
}

#[derive(Debug, Parser)]
struct ShellArgs {
    /// Run reference (e.g. "last-failed"). Defaults to last failed run.
    run_ref: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Unified observability init (Step 2): one handle for the process, shared
    // with `AppState` and `RunnerPoolConfig` later. `PRELOOP_LOG_FORMAT` now
    // controls pretty/json/auto (auto = pretty on TTY, JSON when piped), and
    // `RUST_LOG` defaults to `info` (the old `fmt::init()` default of ERROR hid
    // pool provisioning faults). The runtime is held for the life of `main`
    // and flushed with a bounded 2s shutdown on exit.
    let obs_config = preloop_observability::ObservabilityConfig::from_env();
    let (observability, observability_runtime) =
        preloop_observability::Observability::from_config(obs_config);
    preloop_observability::ObservabilityRuntime::install_fmt_subscriber(observability.config());
    // Keep the handle alive; the pool/server will clone it later. Suppress
    // unused warning until the wiring lands in Step 3.
    let _observability = observability;
    let _observability_runtime = observability_runtime;

    let cli = Cli::parse();
    // One config path for the whole process. `setup`/`doctor`/`secret` return
    // before `cmd_engine` runs, so pinning this only inside the engine let a
    // custom PRELOOP_HOME split the CLI's writes ($HOME/.preloop/config.toml)
    // from the engine's reads ($PRELOOP_HOME/config.toml) — setup silently had
    // no effect. An explicit operator override still wins.
    if std::env::var_os(preloop_runner_server::config::CONFIG_PATH_ENV).is_none() {
        std::env::set_var(
            preloop_runner_server::config::CONFIG_PATH_ENV,
            github_setup::config_path_for_home(),
        );
    }
    // Both run the daemon in this process, so neither may bootstrap another
    // one underneath itself.
    match cli.command {
        Command::Version => {
            println!("preloop {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Command::Serve(args) => return cmd_engine(args).await,
        Command::Engine => return cmd_engine(ServeArgs::default()).await,
        Command::BuildGolden(args) => return cmd_build_golden(args).await,
        Command::Update(args) => return update::run(args).await,
        // Local configuration commands must not spawn the engine.
        Command::Setup(args) => return github_setup::cmd_setup(args).await,
        Command::Doctor(args) => return github_setup::cmd_doctor(args).await,
        Command::Secret(args) => return github_setup::cmd_secret(args).await,
        Command::Server(args) => return server_install::run(args),
        // Planning parses local workflow files only; do not bootstrap the
        // control-plane engine for a command that never contacts it.
        Command::Plan(args) => return cmd_plan(args).await,
        _ => {}
    }
    ensure_engine_running().await?;

    match cli.command {
        Command::Run(args) => cmd_run(args).await,
        Command::Plan(_) => unreachable!("plan is handled before engine bootstrap"),
        Command::Status(args) => cmd_status(args).await,
        Command::Logs(args) => cmd_logs(args).await,
        Command::Cancel(args) => cmd_cancel(args).await,
        Command::Shell(args) => cmd_shell(args).await,
        Command::Debug(args) => {
            debug_session::run(args, build_client(), server_url(), api_token()).await
        }
        Command::Dap(args) => dap_client::run(args, server_url(), api_token()).await,
        Command::Push(args) => cmd_push(args).await,
        Command::Update(_)
        | Command::Serve(_)
        | Command::Engine
        | Command::BuildGolden(_)
        | Command::Version
        | Command::Setup(_)
        | Command::Doctor(_)
        | Command::Secret(_)
        | Command::Server(_) => {
            unreachable!("daemon commands handled before client startup")
        }
    }
}

fn systemd_socket_activation_requested() -> bool {
    cfg!(target_os = "linux") && std::env::var_os("LISTEN_FDS").is_some()
}

async fn cmd_build_golden(args: BuildGoldenArgs) -> anyhow::Result<()> {
    const TOKEN_ENV: &str = "PRELOOP_GOLDEN_BUILD_TOKEN";
    std::env::set_var(TOKEN_ENV, "artifact-build-only");
    let runner_bundle = std::fs::canonicalize(&args.runner_bundle).with_context(|| {
        format!(
            "runner bundle does not exist: {}",
            args.runner_bundle.display()
        )
    })?;
    let output = if args.output.is_absolute() {
        args.output
    } else {
        std::env::current_dir()?.join(args.output)
    };
    // Enterprise option: verify the base image's provenance before baking.
    // Dump-style base images carry a GitHub-signed SLSA attestation and a
    // cosign keyless signature from the publishing workflow; a golden should
    // only be built from an attested base. Opt in with:
    //   PRELOOP_VERIFY_BASE_IMAGE=1 PRELOOP_VERIFY_BASE_IMAGE_REPO=<owner/repo>
    if std::env::var_os("PRELOOP_VERIFY_BASE_IMAGE")
        .is_some_and(|value| value != "0" && value != "false")
    {
        verify_base_image(&args.base_image).await?;
    }
    if env_flag("PRELOOP_REQUIRE_BASE_DIGEST", false) {
        require_digest_pinned_base(&args.base_image)?;
    }
    let config = RunnerPoolConfig {
        size: 1,
        use_fork: false,
        use_packed_artifact: false,
        // Unique per bake: smolvm keys a machine's data dir by a hash of its
        // name and reuses a dir left behind by a failed/interrupted run at its
        // old on-disk size (smolvm#956). A stale dir then boots with the
        // previous, smaller storage disk, so a large bake runs out of space
        // mid-extraction with a confusing "Resource temporarily unavailable".
        // A fresh name per run forces a fresh disk at the requested size.
        name_prefix: std::env::var("PRELOOP_GOLDEN_NAME_PREFIX")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| {
                // A constant fallback would make repeated local bakes reuse
                // the same machine name (and its hash dir), defeating the
                // fresh-disk guarantee above. Per-invocation suffix instead.
                format!("preloop-release-golden-{}", std::process::id())
            }),
        base_image: args.base_image,
        workspace: args.workspace.or_else(|| std::env::current_dir().ok()),
        artifact_stem: output.clone(),
        release_version: env!("CARGO_PKG_VERSION").to_owned(),
        runner_bundle,
        externals_dir: std::env::var_os("PRELOOP_RUNNER_EXTERNALS")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("preloop-externals")),
        runner_binary_name: "preloop-runner".into(),
        server_url: "http://127.0.0.1:1".into(),
        control_origin: None,
        control_socket: None,
        control_upstream: None,
        dns: std::env::var("PRELOOP_RUNNER_DNS").ok(),
        registration_token_env: TOKEN_ENV.into(),
        labels: vec![
            "self-hosted".into(),
            "Linux".into(),
            std::env::consts::ARCH.into(),
        ],
        cpus: RUNNER_CPUS,
        memory_mib: runner_memory_mib(),
        storage_gib: args.storage_gib.unwrap_or_else(runner_storage_gib),
        overlay_gib: std::env::var("PRELOOP_RUNNER_OVERLAY_GB")
            .ok()
            .and_then(|v| v.parse().ok()),
        debug_dir: None,
        runner_key_dir: None,
        pending_jobs: None,
        preload_images: Vec::new(),
        // The golden bake provisions the image (runner install, apt, service
        // files); it must run as root. The official runner image declares
        // USER=runner, so the machine's default exec user is not root.
        runner_user: std::env::var("PRELOOP_RUNNER_USER")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| Some("root".to_owned())),
        runner_uid: None,
        next_job_runs_on: None,
        pending_registrations: None,
        preparing_signal: None,
        pool_status: None,
    };
    let payload = artifact_payload(&output, &config.base_image);
    RunnerPool::new(std::sync::Arc::new(SmolVmProvider::default()), config)?
        .rebuild_artifact()
        .await?;
    if payload != output {
        let temporary = output.with_file_name(format!(
            ".{}.tmp-{}",
            output
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("golden"),
            std::process::id()
        ));
        let copy_result = (|| -> anyhow::Result<()> {
            std::fs::copy(&payload, &temporary).with_context(|| {
                format!(
                    "copy generated golden {} to temporary output {}",
                    payload.display(),
                    temporary.display()
                )
            })?;
            std::fs::rename(&temporary, &output).with_context(|| {
                format!("atomically replace requested output {}", output.display())
            })?;
            Ok(())
        })();
        if copy_result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        copy_result?;
    }
    anyhow::ensure!(
        output.is_file(),
        "golden build did not create {}",
        output.display()
    );
    println!("{}", output.display());
    Ok(())
}

/// Verify a registry base image's provenance before baking a golden from it.
///
/// The dump pipeline's images carry cosign keyless signatures and in-toto
/// attestations (SLSA provenance + SPDX SBOM), signed by the publishing
/// workflow's OIDC identity. A mirror can instead publish signatures under a
/// long-lived key and point `PRELOOP_BASE_IMAGE_PUBKEY` at the public key
/// file; `gh attestation verify` only understands GitHub-API attestations
/// (attest-build-provenance), which the dump does not produce, so verification
/// is cosign-based.
///
/// `cosign` must be installed on the build host. `PRELOOP_VERIFY_BASE_IMAGE`
/// enables the check; `PRELOOP_VERIFY_BASE_IMAGE_REPO` names the repository
/// that publishes the base image; `PRELOOP_BASE_IMAGE_IDENTITY_REGEXP`
/// overrides the default certificate identity match.
async fn verify_base_image(base_image: &str) -> anyhow::Result<()> {
    let repo = std::env::var("PRELOOP_VERIFY_BASE_IMAGE_REPO").context(
        "PRELOOP_VERIFY_BASE_IMAGE=1 requires PRELOOP_VERIFY_BASE_IMAGE_REPO=<owner/repo>",
    )?;
    if !base_image.contains('/') || base_image.starts_with('.') || base_image.starts_with('/') {
        anyhow::bail!("base image `{base_image}` is not a registry reference; nothing to verify");
    }
    require_digest_pinned_base(base_image)?;
    verify_base_image_with(&repo, base_image)
}

fn verify_base_image_with(repo: &str, base_image: &str) -> anyhow::Result<()> {
    let identity = std::env::var("PRELOOP_BASE_IMAGE_IDENTITY_REGEXP").unwrap_or_else(|_| {
        format!(
            "^https://github.com/{repo}/.github/workflows/(dump|attest-local)\\.yml@refs/heads/"
        )
    });
    // The dump pipeline's images carry cosign signatures and in-toto
    // attestations (SLSA provenance + SPDX SBOM). They are keyless-signed by
    // the publishing workflow's OIDC identity; a mirror can instead publish
    // signatures under a long-lived key and point PRELOOP_BASE_IMAGE_PUBKEY
    // at the public key file. `gh attestation verify` only understands
    // GitHub-API attestations (attest-build-provenance), which the dump does
    // not produce, so verification is cosign-based.
    let key = std::env::var("PRELOOP_BASE_IMAGE_PUBKEY").ok();
    let identity_args: Vec<&str> = match &key {
        Some(_) => vec![],
        None => vec![
            "--certificate-identity-regexp",
            &identity,
            "--certificate-oidc-issuer",
            "https://token.actions.githubusercontent.com",
        ],
    };
    let mut verify_args: Vec<&str> = vec!["verify", base_image];
    if let Some(key) = &key {
        verify_args.extend(["--key", key]);
    }
    verify_args.extend(identity_args.iter().copied());
    run_verifier("cosign", &verify_args, "cosign signature")?;
    let mut attest_args: Vec<&str> = vec!["verify-attestation", base_image, "--type", "spdx"];
    if let Some(key) = &key {
        attest_args.extend(["--key", key]);
    }
    attest_args.extend(identity_args.iter().copied());
    run_verifier("cosign", &attest_args, "cosign SPDX SBOM attestation")
}

fn require_digest_pinned_base(base_image: &str) -> anyhow::Result<()> {
    // Packed artifacts and local paths are already immutable inputs; this
    // policy applies to registry references only. Release workflows enable
    // this before baking so a mutable tag cannot silently become the golden's
    // input after the provenance sidecars were captured.
    if base_image.starts_with('.')
        || base_image.starts_with('/')
        || std::path::Path::new(base_image).exists()
    {
        return Ok(());
    }
    let digest = base_image
        .split_once("@sha256:")
        .map(|(_, digest)| digest)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "base image `{base_image}` must use an immutable @sha256:<digest> reference"
            )
        })?;
    anyhow::ensure!(
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "base image `{base_image}` has an invalid sha256 digest"
    );
    Ok(())
}

fn run_verifier(binary: &str, args: &[&str], what: &str) -> anyhow::Result<()> {
    let status = std::process::Command::new(binary)
        .args(args)
        .status()
        .with_context(|| format!("failed to run `{binary}`; is it installed?"))?;
    if !status.success() {
        anyhow::bail!("{what} verification failed for the base image (exit {status})");
    }
    Ok(())
}

async fn ensure_engine_running() -> anyhow::Result<()> {
    if std::env::var("PRELOOP_URL").is_ok() {
        return Ok(());
    }

    let client = build_client();
    let url = server_url();

    let mut health_req = client
        .get(format!("{url}/healthz"))
        .timeout(Duration::from_millis(500));
    if let Some(token) = api_token() {
        health_req = health_req.bearer_auth(token);
    }
    if health_req.send().await.is_ok() {
        return Ok(());
    }

    eprintln!("[preloop] Starting local background engine...");

    let preloop_dir = preloop_home();

    let state_dir = preloop_dir.join("state");
    let pid_path = preloop_dir.join("preloop.pid");
    let token_path = preloop_dir.join("engine.token");

    std::fs::create_dir_all(&state_dir)?;
    set_private_directory_permissions(&preloop_dir)?;

    let token = if let Ok(existing) = std::fs::read_to_string(&token_path) {
        existing.trim().to_owned()
    } else {
        let mut bytes = [0_u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        let token = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        write_private_file(&token_path, token.as_bytes())?;
        token
    };

    let engine_bin = std::env::current_exe().context("resolve preloop executable")?;

    let mut cmd = std::process::Command::new(&engine_bin);
    cmd.arg("engine");
    cmd.env("PRELOOP_SYSTEM_TOKEN", token);
    cmd.env("PRELOOP_HOME", &preloop_dir);
    if std::env::var_os("RUST_LOG").is_none() {
        cmd.env("RUST_LOG", "info,preloop=debug");
    }

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(preloop_dir.join("engine.log"))?;

    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(log_file.try_clone()?);
    cmd.stderr(log_file);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn engine at {}", engine_bin.display()))?;

    let _ = std::fs::write(&pid_path, child.id().to_string());

    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("engine exited before becoming ready: {status}");
        }
        if client
            .get(format!("{url}/healthz"))
            .timeout(Duration::from_millis(300))
            .send()
            .await
            .is_ok()
        {
            eprintln!("[preloop] Engine ready.");
            return Ok(());
        }
    }

    anyhow::bail!("engine auto-boot timed out after 30 seconds");
}

#[cfg(unix)]
pub(crate) fn set_private_directory_permissions(path: &std::path::Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn set_private_directory_permissions(_path: &std::path::Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(unix)]
pub(crate) fn set_private_file_permissions(path: &std::path::Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn set_private_file_permissions(_path: &std::path::Path) -> anyhow::Result<()> {
    Ok(())
}

pub(crate) fn write_private_file(path: &std::path::Path, contents: &[u8]) -> anyhow::Result<()> {
    use std::io::Write as _;

    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("create private file {}", path.display()))?;
    set_private_file_permissions(path)?;
    file.write_all(contents)
        .with_context(|| format!("write private file {}", path.display()))?;
    Ok(())
}

fn prepare_engine_token(
    home: &std::path::Path,
    configured: Option<String>,
) -> anyhow::Result<String> {
    std::fs::create_dir_all(home)
        .with_context(|| format!("create PRELOOP_HOME {}", home.display()))?;
    set_private_directory_permissions(home)?;

    let token_path = home.join("engine.token");
    if let Some(token) = configured {
        anyhow::ensure!(!token.trim().is_empty(), "PRELOOP_SYSTEM_TOKEN is empty");
        write_private_file(&token_path, token.as_bytes())?;
        return Ok(token);
    }
    if let Ok(existing) = std::fs::read_to_string(&token_path) {
        let token = existing.trim().to_owned();
        anyhow::ensure!(!token.is_empty(), "{} is empty", token_path.display());
        set_private_file_permissions(&token_path)?;
        return Ok(token);
    }

    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let token = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    write_private_file(&token_path, token.as_bytes())?;
    Ok(token)
}

/// Merge stored and command-line GitHub credentials into the environment the
/// server reads, optionally persisting them, and report the effective state.
///
/// Precedence, widest to narrowest: existing environment, then `--flags`,
/// then `github-app.json` (written by `--save`), then `config.toml`'s
/// `[github]` section (written by `preloop setup` and read by the server at
/// startup). The environment stays authoritative so a container that
/// injects secrets is never overridden by a file left behind by `--save`.
fn resolve_github_auth(args: &ServeArgs, state_dir: &std::path::Path) -> anyhow::Result<()> {
    let mut auth = github_auth::StoredAuth::load(state_dir)?;

    let private_key_pem = args
        .github_app_key
        .as_ref()
        .map(|path| {
            std::fs::read_to_string(path)
                .with_context(|| format!("reading GitHub App key {}", path.display()))
        })
        .transpose()?;

    let from_flags = github_auth::StoredAuth {
        app_id: args.github_app_id.clone(),
        installation_id: args.github_app_installation_id,
        private_key_pem,
        webhook_secret: args.webhook_secret.clone(),
    };
    let supplied = from_flags != github_auth::StoredAuth::default();
    auth.overlay(from_flags);

    // `preloop setup` stores credentials in config.toml's [github] section,
    // which is what the server itself loads at startup. Fill any gaps from
    // it so the startup report matches what the server will actually see.
    let file_config = preloop_runner_server::config::load_config()?;
    auth.fill_gaps(github_auth::StoredAuth {
        app_id: file_config.github.app_id,
        installation_id: None,
        private_key_pem: file_config.github.app_pem,
        webhook_secret: file_config.github.webhook_secret,
    });

    if args.save {
        if !supplied {
            anyhow::bail!("--save needs at least one GitHub credential flag to save");
        }
        let path = auth.save(state_dir)?;
        eprintln!("[preloop] saved GitHub credentials to {}", path.display());
    }

    auth.apply();
    eprintln!("[preloop] {}", github_auth::StoredAuth::report());
    if github_auth::StoredAuth::is_unconfigured() {
        eprintln!("[preloop] connect GitHub with `preloop setup` — until then, jobs get local tokens and webhooks are unverified");
    }
    Ok(())
}

async fn cmd_engine(args: ServeArgs) -> anyhow::Result<()> {
    let home = preloop_home();
    let state_dir = home.join("state");
    let socket = home.join("preloop.sock");

    // Ensure PRELOOP_SYSTEM_TOKEN and engine.token stay synchronized.
    let token = prepare_engine_token(&home, std::env::var("PRELOOP_SYSTEM_TOKEN").ok())?;
    std::env::set_var("PRELOOP_SYSTEM_TOKEN", &token);
    let listen: std::net::SocketAddr = args
        .listen
        .clone()
        .or_else(|| std::env::var("PRELOOP_LISTEN").ok())
        .unwrap_or_else(|| "127.0.0.1:9090".to_owned())
        .parse()
        .context("--listen / PRELOOP_LISTEN must be a socket address")?;
    let public_url = args
        .public_url
        .clone()
        .or_else(|| std::env::var("PRELOOP_PUBLIC_URL").ok())
        .unwrap_or_else(|| format!("http://127.0.0.1:{}", listen.port()));
    std::env::set_var("PRELOOP_PUBLIC_URL", &public_url);

    // Runner-facing origin is always the loopback listen address: in-VM
    // runners reach it over the mounted control socket, and their job-side
    // programs through the in-guest loopback bridge — never over the public
    // network. The public URL stays strictly GitHub-facing (check-run
    // details links). Runners on other machines need PRELOOP_RUNNER_URL to
    // point at a host-reachable address instead.
    // When the control upstream is set, the VM reaches the server over
    // virtio-net TCP at that address. `PRELOOP_RUNNER_URL` stays loopback so
    // the server advertises loopback URLs in job messages — the in-guest
    // bridge makes them work.
    let control_upstream = std::env::var("PRELOOP_CONTROL_UPSTREAM").ok();
    if std::env::var("PRELOOP_RUNNER_URL").is_err() {
        std::env::set_var(
            "PRELOOP_RUNNER_URL",
            format!("http://127.0.0.1:{}", listen.port()),
        );
    }
    let runner_url = std::env::var("PRELOOP_RUNNER_URL").unwrap();
    let control_origin = mounted_control_origin(&runner_url);

    // Resolve GitHub credentials before `AppState::new` reads the environment.
    // Both `github_app::load_from_env` and the webhook-secret lookup happen
    // inside `serve`, so anything published after that call is ignored.
    resolve_github_auth(&args, &state_dir)?;

    // Shared with the runner pool so it can size provisioning to the work
    // actually waiting, not just to whether it has an idle runner left.
    let queue_depth = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let next_job_runs_on = std::sync::Arc::new(std::sync::RwLock::new(Vec::new()));
    // Raised while the pool is warming its machine image (artifact download
    // or build, golden prep); the server pauses the queued-job starvation
    // clock for the whole warm so a first job does not die before the pool
    // can register a runner.
    let pool_preparing = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Shared one-time provision-token map. The pool writes a token for every
    // machine it provisions and forwards it into the guest's `configure`;
    // the control plane trusts only registrations presenting a match, which
    // is what authorizes job → runner assignment binding. Present whenever
    // the pool can provision.
    let pending_registrations =
        std::sync::Arc::new(std::sync::RwLock::new(std::collections::BTreeMap::<
            String,
            std::time::SystemTime,
        >::new()));
    let pool_enabled = env_flag("PRELOOP_RUNNER_POOL_ENABLED", DEFAULT_RUNNER_POOL_ENABLED);
    let pool_config = local_runner_pool_config(
        &home,
        runner_url.clone(),
        control_origin.clone(),
        control_upstream.clone(),
        queue_depth.clone(),
        next_job_runs_on.clone(),
        pool_enabled,
        pool_preparing.clone(),
        pending_registrations.clone(),
    );
    let pool_available = match &pool_config {
        Ok(_) => true,
        Err(error) => {
            // "jobs queue until a runner is available" was the old message,
            // but a missing bundle means no runner will EVER become
            // available — the queue is a dead end the user only discovers by
            // staring at `preloop status`. Name the cause and the fix so the
            // first run fails loudly and instructively instead.
            tracing::warn!(
                %error,
                "no runner pool: jobs will fail after the queue grace window. \
                 Install the Linux guest runner with `preloop update`, or set \
                 PRELOOP_RUNNER_BUNDLE to a directory containing preloop-runner \
                 (see docs/vm-images.md)"
            );
            false
        }
    };
    let mut server = tokio::spawn(preloop_runner_server::serve(
        preloop_runner_server::ServerConfig {
            listen,
            systemd_socket_activation: systemd_socket_activation_requested(),
            unix_socket: Some(socket.clone()),
            queue_depth: Some(queue_depth.clone()),
            next_job_runs_on: Some(next_job_runs_on.clone()),
            pool_preparing: Some(pool_preparing.clone()),
            pending_registrations: pool_available.then_some(pending_registrations),
            pool_status: None,
            observability: None,
            require_job_assignments: env_flag("PRELOOP_REQUIRE_JOB_ASSIGNMENTS", false),
            state_dir,
            store_url: args.store.clone(),
            record_flows: None,
            tls: preloop_runner_server::TlsMode::None,
            enable_test_api: false,
            test_api_token: None,
            oidc_issuer: None,
            enable_scheduler: false,
        },
    ));
    // Keep the server in the race so a bind failure surfaces its own error
    // instead of a generic socket-wait timeout.
    tokio::select! {
        result = &mut server => return result?,
        result = wait_for_engine_socket(&socket) => result?,
    }

    let shutdown = tokio_util::sync::CancellationToken::new();
    let mut pool = match pool_config {
        Ok(config) => {
            if !pool_enabled {
                if config.use_fork {
                    tracing::info!(
                        "warm runner pool disabled; forking one VM per queued job from the golden"
                    );
                } else {
                    tracing::info!("warm runner pool disabled; provisioning one VM per queued job");
                }
            }
            let pool_shutdown = shutdown.clone();
            // Supervisor-side, once, before any VM boots: systemd's
            // `Delegate=cpu memory pids` chowns this unit's cgroup subtree but
            // leaves `cgroup.subtree_control` empty, so a `vm-<pid>` leaf would
            // get no cpu/memory/pids limit files. This performs the same
            // vacate-then-enable dance `smolvm serve` does. It is the only
            // place Preloop writes to the cgroup hierarchy — the CLI paths
            // (`preloop shell`, debug-session `machine exec`/`cp`) resolve the
            // root read-only and never mutate it.
            preloop_vm::init_vm_cgroup_delegation();
            Some(tokio::spawn(async move {
                RunnerPool::new(std::sync::Arc::new(SmolVmProvider::default()), config)?
                    .run(pool_shutdown)
                    .await
            }))
        }
        Err(_) => None,
    };

    if let Some(pool_task) = pool.as_mut() {
        tokio::select! {
            result = &mut server => { result??; return Ok(()); },
            result = pool_task => { result??; return Ok(()); },
            _ = engine_shutdown_signal() => {},
        }
    } else {
        tokio::select! {
            result = &mut server => { result??; return Ok(()); },
            _ = engine_shutdown_signal() => {},
        }
    }
    shutdown.cancel();
    if let Some(pool_task) = pool.as_mut() {
        if tokio::time::timeout(Duration::from_secs(30), &mut *pool_task)
            .await
            .is_err()
        {
            pool_task.abort();
        }
    }
    server.abort();
    let _ = std::fs::remove_file(socket);
    Ok(())
}

async fn engine_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn wait_for_engine_socket(socket: &std::path::Path) -> anyhow::Result<()> {
    // readyz probe: http://localhost/readyz (500ms timeout, 30s window)
    #[cfg(unix)]
    let client = reqwest::Client::builder().unix_socket(socket).build()?;
    #[cfg(not(unix))]
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    let mut last_reason: Option<String> = None;
    while start.elapsed() < Duration::from_secs(30) {
        match client
            .get("http://localhost/readyz")
            .timeout(Duration::from_millis(500))
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    return Ok(());
                }
                // Non-2xx readyz: capture reason for timeout reporting.
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                let reason = extract_readyz_reason(&body)
                    .unwrap_or_else(|| format!("{status}: {}", truncate_reason(&body)));
                last_reason = Some(reason);
            }
            Err(err) => {
                last_reason = Some(err.to_string());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if let Some(reason) = last_reason {
        anyhow::bail!("local control plane did not become ready within 30 seconds: last readyz reason: {reason}")
    }
    anyhow::bail!("local control plane did not become ready within 30 seconds")
}

fn extract_readyz_reason(body: &str) -> Option<String> {
    if body.trim().is_empty() {
        return None;
    }
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
        for key in ["reason", "code", "message", "error", "status"] {
            if let Some(s) = v.get(key).and_then(|x| x.as_str()) {
                if !s.trim().is_empty() {
                    return Some(s.to_owned());
                }
            }
        }
        // Nested { "ready": { "reason": ... } } or similar
        if let Some(obj) = v.as_object() {
            for (_, val) in obj {
                if let Some(s) = val.as_str() {
                    if !s.trim().is_empty() && s.len() < 200 {
                        return Some(s.to_owned());
                    }
                }
                if let Some(inner) = val.as_object() {
                    for k in ["reason", "code"] {
                        if let Some(s) = inner.get(k).and_then(|x| x.as_str()) {
                            return Some(s.to_owned());
                        }
                    }
                }
            }
        }
        // Return truncated JSON if no specific field
        return Some(truncate_reason(body));
    }
    Some(truncate_reason(body))
}

fn truncate_reason(s: &str) -> String {
    let t = s.trim();
    if t.len() > 300 {
        format!("{}…", &t[..300])
    } else {
        t.to_owned()
    }
}

// Configuration assembly, not a public API: the parameter list mirrors the
// inputs the pool genuinely needs.
#[allow(clippy::too_many_arguments)]
fn local_runner_pool_config(
    home: &std::path::Path,
    server_url: String,
    control_origin: Option<String>,
    control_upstream: Option<String>,
    queue_depth: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    next_job_runs_on: std::sync::Arc<std::sync::RwLock<Vec<String>>>,
    pool_enabled: bool,
    preparing_signal: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pending_registrations: std::sync::Arc<
        std::sync::RwLock<std::collections::BTreeMap<String, std::time::SystemTime>>,
    >,
) -> anyhow::Result<RunnerPoolConfig> {
    let control_bridge = home.join("control-bridge");
    std::fs::create_dir_all(&control_bridge)?;
    set_private_directory_permissions(&control_bridge)?;
    let runner_bundle = std::env::var_os("PRELOOP_RUNNER_BUNDLE")
        .map(PathBuf::from)
        .filter(|path| linux_runner_bundle(path))
        .or_else(|| {
            let exe_dir = std::env::current_exe().ok()?.parent()?.to_path_buf();
            let target_dir = exe_dir.parent()?;
            let mut candidates = Vec::new();
            // Released installs (`install.sh`, `preloop update`) place the
            // Linux guest runner under <prefix>/lib/preloop/runner/<triple>/.
            // Prefer the host's own Linux triple, then any installed triple.
            if let Some(prefix) = exe_dir.parent() {
                let runner_dir = prefix.join("lib/preloop/runner");
                candidates.push(runner_dir.join(linux_guest_triple()));
                if let Ok(entries) = std::fs::read_dir(&runner_dir) {
                    let mut installed: Vec<_> = entries
                        .flatten()
                        .map(|entry| entry.path())
                        .filter(|path| path.is_dir())
                        .collect();
                    installed.sort();
                    candidates.extend(installed);
                }
            }
            // Development builds keep the guest binary under target/<triple>/;
            // the runner executes inside Linux VMs.
            candidates.extend([
                target_dir.join("aarch64-unknown-linux-gnu/debug"),
                target_dir.join("aarch64-unknown-linux-musl/debug"),
                target_dir.join("aarch64-unknown-linux-gnu/release"),
                target_dir.join("aarch64-unknown-linux-musl/release"),
                exe_dir.join("preloop-runner"),
                exe_dir.to_path_buf(),
            ]);
            candidates
                .into_iter()
                .find(|directory| directory.join("preloop-runner").is_file())
        })
        .filter(|path| linux_runner_bundle(path))
        .context("Linux runner bundle unavailable; set PRELOOP_RUNNER_BUNDLE to a directory containing a Linux preloop-runner, or build one with `just build-preloop` (docs/vm-images.md)")?;
    let use_packed_artifact = env_flag("PRELOOP_USE_PACKED_GOLDEN", DEFAULT_USE_PACKED_GOLDEN);
    // The workspace is scanned for toolchain version files (rust-toolchain.toml,
    // .nvmrc, etc.) so the golden can be built with the project's toolchains
    // pre-installed instead of installing them per job. PRELOOP_WORKSPACE
    // overrides the current directory for daemon-style deployments.
    let workspace = std::env::var_os("PRELOOP_WORKSPACE")
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));
    // The mounted control socket only makes sense when the runner URL is
    // loopback (the VM reaches the host through the socket relay). With
    // `PRELOOP_RUNNER_URL` pointing at a host-reachable LAN address — the
    // "runners on other machines" mode — there is no relay, and the runner
    // talks to the control plane over plain TCP instead. The orchestrator
    // gates both the socket mount and the guest env on this field, so leaving
    // it `None` is what switches the transport.
    let control_socket = control_origin.as_ref().map(|_| home.join("preloop.sock"));
    // When a TCP upstream is configured, skip the socket mount — the
    // guest control bridge will forward via TCP instead of vsock.
    let control_socket = if control_upstream.is_some() {
        None
    } else {
        control_socket
    };
    let base_image = std::env::var("PRELOOP_RUNNER_BASE_IMAGE")
        .unwrap_or_else(|_| preloop_orchestrator::environment::DEFAULT_BASE_IMAGE.into());
    // A custom base image (`.smolmachine` artifact or any non-stock OCI
    // reference) serves every queued job itself, so environment-based runner
    // replacement has nothing to switch to: the job's implied stock base
    // (`ubuntu:24.04` from `ubuntu-latest` labels) will always differ from
    // the configured image and idle runners would be replaced forever.
    // Compare on the plain `repository:tag`, so the digest-pinned defaults
    // (ubuntu:24.04@sha256:…) still count as stock Ubuntu images.
    let custom_base = !is_stock_base_image(&base_image);
    Ok(RunnerPoolConfig {
        // Size zero is the deliberate low-memory mode: keep the local
        // supervisor alive, but build a runner only when a job is queued.
        size: if pool_enabled {
            std::env::var("PRELOOP_RUNNER_POOL_SIZE")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| host_runner_pool_size(RUNNER_CPUS))
        } else {
            0
        },
        // Forking is safe whenever the packed artifact is enabled. The warm
        // pool switch controls whether idle runners stay registered; size-zero
        // mode still prepares one golden and forks a disposable VM per job.
        use_packed_artifact,
        use_fork: use_packed_artifact && env_flag("PRELOOP_USE_FORK", true),
        // Multiple engines on one host must not share a namespace: smolvm
        // keys machines and persistent overlays by name, and cross-engine
        // reuse boots a runner whose persisted state points at the other
        // engine's control plane.
        name_prefix: std::env::var("PRELOOP_RUNNER_NAME_PREFIX")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "preloop-runner".into()),
        base_image: base_image.clone(),
        // GitHub-hosted parity: guest runners run as a dedicated account
        // instead of root, so steps see the hosted user-session contract.
        // PRELOOP_RUNNER_USER=root restores root; an empty value disables
        // switching (runner keeps the guest's root identity).
        runner_user: match std::env::var("PRELOOP_RUNNER_USER") {
            Ok(value) if value.is_empty() => None,
            Ok(value) => Some(value),
            Err(_) => Some("runner".to_owned()),
        },
        runner_uid: std::env::var("PRELOOP_RUNNER_UID")
            .ok()
            .and_then(|value| value.parse().ok())
            .or(Some(1001)),
        workspace: Some(workspace),
        // The packed artifact cache key includes the resolved base image
        // (tag AND digest): the digest-pinned defaults are a golden's
        // provenance. When the pin moves, a stale packed golden baked from
        // the old digest must not be reused. Custom `.smolmachine` bases are
        // filesystem paths, so separators are normalized out of the key.
        artifact_stem: home.join("vms").join(format!(
            "preloop-{}-{}",
            base_image.replace(['/', ':', '@'], "-"),
            std::env::consts::ARCH
        )),
        release_version: env!("CARGO_PKG_VERSION").to_owned(),
        runner_bundle,
        // Node externals shared with every VM via a read-only mount. The
        // operator may point this anywhere; the default lives next to the
        // control-bridge state.
        externals_dir: std::env::var_os("PRELOOP_RUNNER_EXTERNALS")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("externals")),
        runner_binary_name: "preloop-runner".to_owned(),
        server_url,
        control_origin,
        control_socket,
        control_upstream,
        dns: std::env::var("PRELOOP_RUNNER_DNS").ok(),
        registration_token_env: "PRELOOP_SYSTEM_TOKEN".into(),
        labels: runner_pool_labels(),
        cpus: RUNNER_CPUS,
        memory_mib: runner_memory_mib(),
        storage_gib: runner_storage_gib(),
        overlay_gib: std::env::var("PRELOOP_RUNNER_OVERLAY_GB")
            .ok()
            .and_then(|v| v.parse().ok()),
        debug_dir: Some(home.join("state").join("debug")),
        runner_key_dir: Some(home.join("runner-keys")),
        // Warm the golden with the images this project's workflows declare,
        // so `container:`/`services:` jobs do not re-pull on every run.
        preload_images: preloop_orchestrator::environment::scan_workflow_images(
            &std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        pending_jobs: Some(queue_depth),
        next_job_runs_on: (!custom_base).then_some(next_job_runs_on),
        pending_registrations: Some(pending_registrations),
        preparing_signal: Some(preparing_signal),
        pool_status: None,
    })
}

/// Base runner labels plus any extras from `PRELOOP_RUNNER_LABELS`.
///
/// The scheduler matches `runs-on` labels against what a runner declares, so
/// a non-x86 host can still volunteer for X64-pinned workflows by declaring
/// the extra label explicitly. Comma-separated; empty entries are ignored.
fn runner_pool_labels() -> Vec<String> {
    let mut labels = vec![
        "self-hosted".to_owned(),
        "Linux".to_owned(),
        std::env::consts::ARCH.to_owned(),
    ];
    if let Ok(extra) = std::env::var("PRELOOP_RUNNER_LABELS") {
        for label in extra.split(',').map(str::trim).filter(|l| !l.is_empty()) {
            if !labels
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(label))
            {
                labels.push(label.to_owned());
            }
        }
    }
    labels
}

/// vCPUs given to each runner VM.
const RUNNER_CPUS: u16 = 4;
/// Low-memory on-demand provisioning is the default; opt into idle warm VMs.
const DEFAULT_RUNNER_POOL_ENABLED: bool = false;
/// Published or locally cached packed images avoid cold OCI bootstrap per job.
const DEFAULT_USE_PACKED_GOLDEN: bool = true;
/// Memory given to each runner VM, in MiB. SmolVM balloons this, so an idle
/// runner commits far less than its ceiling.
const RUNNER_MEMORY_MIB: u32 = 4096;
/// Persistent storage given to each runner VM, in GiB.
///
/// The default is enough for the minimal golden. Full snapshots of the
/// official GitHub-hosted image are much larger and should set
/// `PRELOOP_RUNNER_STORAGE_GB` to 80 or more.
const RUNNER_STORAGE_GIB: u32 = 20;

/// Memory ceiling for each runner VM, honouring `PRELOOP_RUNNER_MEMORY_MIB`.
///
/// The 4 GiB default fits ordinary jobs, but a thin-LTO `codegen-units=1`
/// release build of this workspace peaks past it and rustc dies with SIGKILL
/// — which is exactly how the aarch64 release job failed. Pool size is
/// already tunable via `PRELOOP_RUNNER_POOL_SIZE`; memory has to be too, or
/// the only lever on a memory-bound host is running fewer machines.
/// Ballooning means a raised ceiling costs nothing while runners sit idle.
fn runner_memory_mib() -> u32 {
    const MIN_MEMORY_MIB: u32 = 1024;
    std::env::var("PRELOOP_RUNNER_MEMORY_MIB")
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .filter(|value| *value >= MIN_MEMORY_MIB)
        .unwrap_or(RUNNER_MEMORY_MIB)
}

/// Persistent storage for each runner VM, honouring
/// `PRELOOP_RUNNER_STORAGE_GB`.
fn runner_storage_gib() -> u32 {
    std::env::var("PRELOOP_RUNNER_STORAGE_GB")
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(RUNNER_STORAGE_GIB)
}

/// Resident memory an idle runner VM actually holds, in MiB.
///
/// Measured at ~390 MiB for a forked runner sitting on its long poll, against a
/// 4096 MiB ceiling: SmolVM balloons the guest, so the ceiling says nothing
/// about the cost of keeping one warm.
const IDLE_RUNNER_MIB: u64 = 400;

/// Share of host memory this is willing to hold in idle runners.
const IDLE_MEMORY_SHARE: u64 = 8;

/// Most runners to keep warm, however large the host.
const WARM_POOL_CAP: usize = 8;

/// How many runners to keep warm.
///
/// Two different resources set the bounds, and conflating them under-sized the
/// pool. Running jobs are CPU-bound, so `parallelism / cpus_per_runner` is what
/// the host can execute at once. Warm runners are *idle*, consuming memory and
/// almost no CPU, and their job is to absorb a fan-out without anyone waiting
/// on a VM build — which costs ~500 ms under load and shows up as a cliff the
/// moment a matrix is one job wider than the pool.
///
/// So the warm set is allowed past the CPU budget, bounded by the memory we are
/// willing to leave parked and never below what the host can actually run.
/// Capped at `WARM_POOL_CAP` so a very large machine does not sit on dozens of
/// idle VMs. Set
/// `PRELOOP_RUNNER_POOL_SIZE` to override.
fn host_runner_pool_size(cpus_per_runner: u16) -> usize {
    let parallelism = std::thread::available_parallelism().map_or(2, |value| value.get());
    let by_cpu = (parallelism / usize::from(cpus_per_runner.max(1))).max(1);
    let by_memory = host_memory_mib()
        .map(|total| (total / IDLE_MEMORY_SHARE / IDLE_RUNNER_MIB) as usize)
        .unwrap_or(by_cpu);
    // Not `clamp`: on a host with more CPU budget than the cap its lower bound
    // would exceed its upper bound and panic.
    let target = by_cpu.saturating_mul(2).min(by_memory).min(WARM_POOL_CAP);
    target.max(by_cpu.min(WARM_POOL_CAP)).max(1)
}

/// Total physical memory in MiB, or `None` when it cannot be determined.
#[cfg(target_os = "macos")]
fn host_memory_mib() -> Option<u64> {
    let output = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    let bytes: u64 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .ok()?;
    Some(bytes / (1024 * 1024))
}

/// Total physical memory in MiB, or `None` when it cannot be determined.
#[cfg(target_os = "linux")]
fn host_memory_mib() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kib: u64 = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kib / 1024)
}

/// Total physical memory in MiB, or `None` when it cannot be determined.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn host_memory_mib() -> Option<u64> {
    None
}

fn linux_runner_bundle(path: &std::path::Path) -> bool {
    let binary = path.join("preloop-runner");
    let Ok(mut file) = std::fs::File::open(binary) else {
        return false;
    };
    let mut magic = [0_u8; 4];
    file.read_exact(&mut magic).is_ok() && magic == *b"\x7fELF"
}

/// The Linux guest triple the local microVM pool runs on this host.
pub(crate) fn linux_guest_triple() -> &'static str {
    if cfg!(target_arch = "x86_64") {
        "x86_64-unknown-linux-gnu"
    } else {
        "aarch64-unknown-linux-gnu"
    }
}

fn env_flag(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| parse_flag(&value))
        .unwrap_or(default)
}

fn parse_flag(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn default_local_activity_type(event: &str, payload: &serde_json::Value) -> Option<String> {
    if matches!(event, "pull_request" | "pull_request_target")
        && payload
            .get("action")
            .and_then(serde_json::Value::as_str)
            .is_none()
    {
        // A local run represents testing the current checkout against the PR
        // workflow. `synchronize` is one of GitHub's default PR activity types
        // and avoids requiring a synthetic webhook payload just to pass the
        // trigger gate.
        Some("synchronize".to_owned())
    } else {
        None
    }
}

fn collect_local_reusable_workflows(
    workflow_path: &std::path::Path,
) -> anyhow::Result<BTreeMap<String, String>> {
    let current_dir = std::env::current_dir()?;
    let workflows_dir = current_dir.join(".github").join("workflows");
    let mut reusable_workflows = BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(workflows_dir) else {
        return Ok(reusable_workflows);
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("yml" | "yaml")
        ) || same_file_path(&path, workflow_path)
        {
            continue;
        }
        let Some(relative) = path.strip_prefix(&current_dir).ok() else {
            continue;
        };
        let yaml = std::fs::read_to_string(&path)
            .with_context(|| format!("read reusable workflow {}", path.display()))?;
        reusable_workflows.insert(relative.to_string_lossy().into_owned(), yaml);
    }
    Ok(reusable_workflows)
}

async fn cmd_run(args: RunArgs) -> anyhow::Result<()> {
    // A hook that fails to install after the user accepted is a real
    // failure: report it instead of silently running ungated.
    maybe_offer_hook()?;
    let workflow_path = resolve_workflow_path(args.file.as_deref())?;
    let workflow_yaml = std::fs::read_to_string(&workflow_path)
        .with_context(|| format!("failed to read workflow: {}", workflow_path.display()))?;
    let event = args.event.as_deref().unwrap_or("push");

    // Hand local reusable workflows (`uses: ./.github/workflows/…`) to the
    // server the same way the native client does: everything under the
    // workspace `.github/workflows/`, keyed repository-relative, minus the
    // submitted workflow itself.
    let reusable_workflows = collect_local_reusable_workflows(&workflow_path)?;

    let payload = match args.payload.as_deref() {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read payload: {}", path.display()))?;
            serde_json::from_str(&text)
                .with_context(|| format!("parse payload {}: not valid JSON", path.display()))?
        }
        None => serde_json::json!({}),
    };
    let activity_type = default_local_activity_type(event, &payload);

    let mut secrets = preloop_gha_protocol::SecretMap::default();
    for secret in &args.secrets {
        if let Some((name, value)) = secret.split_once('=') {
            secrets.insert(
                name.to_owned(),
                preloop_gha_protocol::SecretString::new(value.to_owned()),
            );
        } else {
            anyhow::bail!("invalid --secret format `{secret}`: expected NAME=VALUE");
        }
    }

    let push_requested = args.push || args.create_pr;
    // A dirty working tree is allowed with --push: CI runs on the server's
    // snapshot of the uncommitted state, and after it passes the CLI
    // materializes a commit whose tree is exactly what CI tested (see
    // `decide_dirty_push_opts`). A clean tree pins the push to HEAD directly.
    let dirty = if push_requested {
        git_porcelain().context("failed to check the working tree for --push")?
    } else {
        Vec::new()
    };
    let dirty_push = push_requested && !dirty.is_empty();
    let tested_head = if push_requested && !dirty_push {
        Some((git_rev_parse("HEAD")?, git_rev_parse("HEAD^{tree}")?))
    } else {
        None
    };

    let mut submission = WorkflowSubmission {
        workflow_yaml,
        event: event.to_owned(),
        payload,
        repository: detect_repository(),
        git_ref: detect_git_ref(),
        workflow_path: Some(workflow_path.display().to_string()),
        local_workspace: None,
        secrets,
        reusable_workflows,
        selected_jobs: args.job.into_iter().collect(),
        base_ref: args.base,
        activity_type,
        // On by default where it can be acted on: a paused job blocks until a
        // controller answers, so pausing a piped or detached run would hang
        // something with no way to respond. `--preserve-on-failure` is the
        // escape hatch for exactly that case — hold the VM for a later
        // `preloop shell` without anyone attached now.
        preserve_on_failure: !args.no_debug
            && (args.preserve_on_failure
                || (!args.detach && std::io::IsTerminal::is_terminal(&std::io::stdin()))),
        ..Default::default()
    };
    // The terminal gate is silent by design (a piped run must not hang
    // waiting for a controller), but a user who expects the debug shell on
    // failure needs to know it was disabled before the run fails — the
    // alternative is a plain `✗` with no explanation and no way to attach.
    // A piped pipeline never expects a failure shell, so the notice would be
    // noise on every CI run; it targets interactive-ish invocations only.
    if !submission.preserve_on_failure && !args.no_debug && std::env::var("CI").is_err() {
        eprintln!(
            "[preloop] failure shells are off ({}); pass --preserve-on-failure to \
             pause on failure for `preloop debug`/`preloop shell`",
            if args.detach {
                "detached run"
            } else {
                "stdin is not a terminal"
            }
        );
    }
    // Overridden rather than set in the literal so a plain run keeps the
    // protocol's own defaults for `sha` and `actor`.
    if push_requested {
        // The base commit: for a clean tree this IS the tested commit; for a
        // dirty tree it is the parent of the materialized CI-verified commit.
        submission.sha = git_rev_parse("HEAD")?;
        // The server names the requester in the PR body it opens.
        if let Some(name) = git_config_user_name() {
            submission.actor = name;
        }
        submission.push = Some(preloop_gha_protocol::PushRequest {
            create_pr: args.create_pr,
            draft_pr: args.pr_draft,
            // The server verifies the branch head for a dirty-tree push (the
            // materialized commit) instead of the base sha.
            dirty: dirty_push,
        });
        if let Some((_, head_tree)) = tested_head {
            submission.push_tree = Some(head_tree);
        }
    }

    let client = build_client();
    let url = server_url();
    // Secrets redact on plain serialization; sending them is opt-in.
    let mut request = client
        .post(format!("{url}/api/v1/runs"))
        .json(&submission.to_request_json()?);
    if should_send_local_workspace_header(&url, std::env::var("PRELOOP_URL").is_err()) {
        let workspace = std::fs::canonicalize(".")?;
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(workspace.as_os_str().to_string_lossy().as_bytes());
        request = request.header("x-preloop-local-workspace", encoded);
    }
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .with_context(engine_unreachable_context)?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }

    let accepted: RunAccepted = response.json().await?;
    println!(
        "Run {} created ({} jobs queued)",
        accepted.run_id, accepted.queued_jobs
    );

    let mut final_status = None;
    if args.detach {
        println!(
            "Run {} submitted in detached mode. Use `preloop status` to check",
            accepted.run_id
        );
        return Ok(());
    }
    let mut seen_events = HashSet::new();
    // A mid-stream connection drop (EOF, reset) is the same "stream ended
    // early" condition as `None` and deserves a reconnect, not a hard
    // failure. Bounded so a dead engine does not spin forever.
    let mut consecutive_stream_errors = 0u32;
    // A paused session the watcher cannot see is a job hung forever with no
    // explanation, so the first poll failure is reported instead of being
    // folded into "no session".
    let mut poll_warned = false;
    // Warn once (not every backpressure tick) when the control plane is
    // unreachable or no registered runner can claim the queued jobs, so a
    // queued run does not stream `still waiting` forever without the one
    // fact that explains it. The check is repeated while a claimable runner
    // exists (one can die between ticks) and latches only once a warning
    // actually prints.
    let mut runner_warned = false;
    // Set when the run loop leaves through the debug prompt (abort, detach,
    // or a session error). The run is not terminal in those cases — the job
    // is paused and reattachable — so the generic conclusion below must not
    // report a finished status.
    let mut left_via_pause = false;
    // Per-job status seen on the event stream, for the backpressure line.
    let mut job_statuses: std::collections::HashMap<String, ExecutionStatus> =
        std::collections::HashMap::new();
    let mut last_activity = std::time::Instant::now();
    let mut last_backpressure = std::time::Instant::now();
    loop {
        let mut events_request = client.get(format!(
            "{url}/api/v1/runs/{}/events.ndjson",
            accepted.run_id
        ));
        if let Some(token) = api_token() {
            events_request = events_request.bearer_auth(token);
        }
        let events_response = events_request
            .send()
            .await
            .with_context(engine_unreachable_context)?;
        if !events_response.status().is_success() {
            let status = events_response.status();
            let body = events_response.text().await.unwrap_or_default();
            anyhow::bail!("server returned {status}: {body}");
        }

        let mut stream = events_response.bytes_stream();
        let mut pending = String::new();
        let mut paused: Option<preloop_gha_protocol::debug_session::DebugSession> = None;
        let mut stream_error: Option<reqwest::Error> = None;
        loop {
            // The server holds this stream open until the run is terminal, and
            // a job paused at a failed step never gets there. Watching only the
            // stream would sit silently forever with the answer one poll away,
            // so race the next chunk against a check for a paused session.
            let chunk = tokio::select! {
                biased;
                chunk = stream.next() => match chunk {
                    Some(chunk) => match chunk {
                        Ok(chunk) => chunk,
                        Err(error) => {
                            stream_error = Some(error);
                            break;
                        }
                    },
                    None => break,
                },
                () = tokio::time::sleep(Duration::from_millis(750)) => {
                    match debug_session::list_sessions(&client, &url, api_token()).await {
                        Ok(sessions) => {
                            let paused_total = sessions
                                .iter()
                                .filter(|session| {
                                    session.state
                                        == preloop_gha_protocol::debug_session::SessionState::Paused
                                })
                                .count();
                            if let Some(session) =
                                sessions.into_iter().find(|session| {
                                    session.run_id == accepted.run_id
                                        && session.state
                                            == preloop_gha_protocol::debug_session::SessionState::Paused
                                })
                            {
                                paused = Some(session);
                                break;
                            }
                            // A run that produces no events for a while is
                            // usually waiting on pool capacity — often held
                            // by other runs' paused debug sessions. Say so
                            // instead of hanging silently on the last status
                            // line.
                            if !final_status.is_some_and(ExecutionStatus::is_terminal)
                                && last_activity.elapsed() >= Duration::from_secs(15)
                                && last_backpressure.elapsed() >= Duration::from_secs(15)
                            {
                                let queued = job_statuses
                                    .values()
                                    .filter(|status| {
                                        matches!(
                                            status,
                                            ExecutionStatus::Queued | ExecutionStatus::Pending
                                        )
                                    })
                                    .count();
                                // A slow in-progress step is not a stall: only
                                // claim one when jobs or sessions are actually
                                // waiting on capacity.
                                if queued > 0 || paused_total > 0 {
                                    eprintln!(
                                        "[preloop] still waiting: {queued} job(s) queued, \
                                         {paused_total} debug session(s) paused on the server \
                                         (preloop debug <id> to inspect)"
                                    );
                                    if !runner_warned {
                                        match runner_capacity(&client, &url, accepted.run_id).await
                                        {
                                            Ok(Some((queued, claimable)))
                                                if queued > 0 && claimable == 0 =>
                                            {
                                                runner_warned = true;
                                                eprintln!(
                                                    "[preloop] warning: no registered runner on \
                                                     {url} can claim the queued jobs (their \
                                                     `runs-on:` labels match no runner). If \
                                                     `preloop serve` is still provisioning its \
                                                     runner pool this is expected; otherwise the \
                                                     jobs will never start — register a runner \
                                                     with matching labels."
                                                );
                                            }
                                            // A claimable runner exists (or nothing
                                            // runner-bound is queued): keep checking on
                                            // later ticks instead of latching a premature
                                            // "all quiet" state.
                                            Ok(_) => {}
                                            Err(error) => {
                                                runner_warned = true;
                                                eprintln!(
                                                    "[preloop] warning: cannot determine runner \
                                                     availability on {url} ({error:#}); older \
                                                     control plane without /api/v1/runners or a \
                                                     transient error. Queued jobs may still \
                                                     start if a runner is registered."
                                                );
                                            }
                                        }
                                    }
                                    last_backpressure = std::time::Instant::now();
                                }
                            }
                        }
                        Err(error) => {
                            if !poll_warned {
                                eprintln!(
                                    "[preloop] cannot check for paused debug sessions: {error:#}"
                                );
                                poll_warned = true;
                            }
                        }
                    }
                    continue;
                }
            };
            pending.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(newline) = pending.find('\n') {
                let line = pending[..newline].trim().to_owned();
                pending.drain(..=newline);
                if seen_events.insert(line.clone()) {
                    if let Ok(NdjsonEvent::JobStatus { job_id, status, .. }) =
                        serde_json::from_str::<NdjsonEvent>(&line)
                    {
                        job_statuses.insert(job_id.0, status);
                    }
                    last_activity = std::time::Instant::now();
                    update_run_status(&mut final_status, render_event(&line));
                }
            }
            if final_status.is_some_and(ExecutionStatus::is_terminal) {
                break;
            }
        }

        if let Some(error) = stream_error {
            consecutive_stream_errors += 1;
            if consecutive_stream_errors > 5 {
                return Err(anyhow::anyhow!(error).context(
                    "the run's event stream kept dropping; is the engine still running?",
                ));
            }
            tokio::time::sleep(Duration::from_millis(
                250 * u64::from(consecutive_stream_errors),
            ))
            .await;
            continue;
        }
        consecutive_stream_errors = 0;

        if let Some(session) = paused {
            match debug_session::prompt_at_failure(&client, &url, api_token(), session).await {
                // Resumed: reconnect and keep reporting the run.
                Ok(true) => continue,
                Ok(false) => {
                    // Aborted or detached: the prompt already explained the
                    // outcome and printed the reattach command.
                    left_via_pause = true;
                    break;
                }
                Err(error) => {
                    eprintln!("debug session error: {error:#}");
                    return Err(error);
                }
            }
        }

        if !final_status.is_some_and(ExecutionStatus::is_terminal)
            && !pending.trim().is_empty()
            && seen_events.insert(pending.trim().to_owned())
        {
            update_run_status(&mut final_status, render_event(pending.trim()));
        }

        if final_status.is_some_and(ExecutionStatus::is_terminal) {
            break;
        }

        // The server holds `events.ndjson` open until the run is terminal, so
        // reaching here means the stream dropped early. Retry promptly rather
        // than adding a fixed poll interval to every run's wall clock.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    if left_via_pause {
        return Ok(());
    }

    if let Some(status) = final_status {
        let symbol = if status == ExecutionStatus::Success {
            "✓"
        } else {
            "✗"
        };
        println!("{symbol} Run: {status:?}");
        // Push-back runs on any conclusion: a draft PR with red checks is
        // the reviewable state. Sync progress goes to stderr so piped
        // stdout stays clean.
        let push_error = if push_requested {
            if dirty_push {
                // The PR decision happens after CI for a dirty tree: the
                // user saw the result and chooses y/N/d now (or a label /
                // explicit flag decides in non-interactive runs).
                match decide_dirty_push_opts(
                    &dirty,
                    args.push,
                    args.create_pr,
                    args.pr_draft,
                    status == ExecutionStatus::Success,
                )? {
                    Some(opts) => push::push_run(
                        &client,
                        &url,
                        api_token(),
                        &accepted.run_id.to_string(),
                        Some(opts),
                    )
                    .await
                    .err(),
                    None => {
                        eprintln!(
                            "not pushing (declined) — CI result was informational. \
                             Re-run `preloop push {}` to push later.",
                            accepted.run_id
                        );
                        None
                    }
                }
            } else {
                push::push_run(
                    &client,
                    &url,
                    api_token(),
                    &accepted.run_id.to_string(),
                    None,
                )
                .await
                .err()
            }
        } else {
            None
        };
        if let Some(error) = &push_error {
            eprintln!(
                "push failed: {error:#}\n\
                 fix the problem and rerun: `preloop push {}`",
                accepted.run_id
            );
        }
        if status != ExecutionStatus::Success {
            anyhow::bail!("run completed with status {status:?}");
        }
        if let Some(error) = push_error {
            anyhow::bail!("{error:#}");
        }
    }

    Ok(())
}

fn same_file_path(left: &std::path::Path, right: &std::path::Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

/// Registered-runner capacity for a run's queued jobs, when the control
/// plane supports the run-scoped runners query.
///
/// Returns `(queued, claimable)`: the run's ready-queue job count, and how
/// many registered runners could claim at least one of them (the same
/// `job_matches_runner` predicate the scheduler dispatches with). `None`
/// when the server ignores `run_id` and returns the plain list — the caller
/// then falls back to the raw count so a zero-runner control plane still
/// gets the warning.
async fn runner_capacity(
    client: &reqwest::Client,
    url: &str,
    run_id: RunId,
) -> anyhow::Result<Option<(usize, usize)>> {
    let mut request = client.get(format!("{url}/api/v1/runners?run_id={run_id}"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        anyhow::bail!("server returned {}", response.status());
    }
    let body: serde_json::Value = response.json().await?;
    match (
        body.get("queued").and_then(serde_json::Value::as_u64),
        body.get("claimable").and_then(serde_json::Value::as_u64),
    ) {
        (Some(queued), Some(claimable)) => Ok(Some((queued as usize, claimable as usize))),
        // Older server (or one ignoring the query): only the raw count is
        // available. When count is zero, represent that as 0 claimable runners
        // for the queued jobs so the dead-pool warning fires. When count > 0,
        // treat runners as claimable (optimistic fallback).
        _ => {
            let count = body
                .get("count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0) as usize;
            if count == 0 {
                Ok(Some((1, 0)))
            } else {
                Ok(Some((count, count)))
            }
        }
    }
}

fn update_run_status(current: &mut Option<ExecutionStatus>, next: Option<ExecutionStatus>) {
    if let Some(status) = next {
        if status.is_terminal() || current.is_none() {
            *current = Some(status);
        }
    }
}

fn render_event(line: &str) -> Option<ExecutionStatus> {
    if line.is_empty() {
        return None;
    }
    match serde_json::from_str::<NdjsonEvent>(line) {
        Ok(NdjsonEvent::JobStatus { job_id, status, .. }) => {
            let symbol = match status {
                ExecutionStatus::Success => "✓",
                ExecutionStatus::Failure => "✗",
                ExecutionStatus::Cancelled | ExecutionStatus::Skipped => "⊘",
                ExecutionStatus::InProgress => "⠋",
                ExecutionStatus::Queued | ExecutionStatus::Pending => "○",
            };
            println!("  {symbol} {job_id} ({status:?})");
            None
        }
        Ok(NdjsonEvent::RunStatus { status, .. }) => Some(status),
        Ok(_) | Err(_) => None,
    }
}

fn resolve_workflow_path(file: Option<&std::path::Path>) -> anyhow::Result<PathBuf> {
    match file {
        Some(path) => {
            if path.components().count() == 1 {
                let workflow = PathBuf::from(".github/workflows").join(path);
                if workflow.exists() {
                    return Ok(workflow);
                }
            }
            if path.exists() {
                Ok(path.to_owned())
            } else {
                anyhow::bail!("workflow file not found: {}", path.display())
            }
        }
        None => {
            let dir = PathBuf::from(".github/workflows");
            if !dir.is_dir() {
                anyhow::bail!("no .github/workflows directory found")
            }
            let mut workflows: Vec<PathBuf> = std::fs::read_dir(&dir)?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| {
                    path.extension()
                        .is_some_and(|extension| extension == "yml" || extension == "yaml")
                })
                .collect();
            workflows.sort();
            match workflows.len() {
                0 => anyhow::bail!("no workflow files found in .github/workflows/"),
                1 => Ok(workflows.into_iter().next().expect("one workflow")),
                _ => {
                    println!("Multiple workflows found:");
                    for (index, workflow) in workflows.iter().enumerate() {
                        println!("  {}: {}", index + 1, workflow.display());
                    }
                    anyhow::bail!("specify a workflow with -f")
                }
            }
        }
    }
}

fn detect_repository() -> String {
    std::process::Command::new("git")
        .args(["remote", "get-url", "origin"])
        .output()
        .ok()
        .and_then(|output| {
            if !output.status.success() {
                return None;
            }
            let remote = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            let remote = remote.strip_suffix(".git").unwrap_or(&remote);
            let remote = remote.rsplit_once(':').map_or(remote, |(_, path)| path);
            let path = remote.rsplit('/').take(2).collect::<Vec<_>>();
            if path.len() != 2 {
                return None;
            }
            Some(path.into_iter().rev().collect::<Vec<_>>().join("/"))
        })
        .unwrap_or_else(|| {
            let dir_name = std::env::current_dir()
                .ok()
                .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| "repo".to_owned());
            format!("local/{dir_name}")
        })
}

fn detect_git_ref() -> String {
    if let Ok(output) = std::process::Command::new("git")
        .args(["symbolic-ref", "HEAD"])
        .output()
    {
        if output.status.success() {
            let ref_str = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if !ref_str.is_empty() {
                return ref_str;
            }
        }
    }

    if let Ok(output) = std::process::Command::new("git")
        .args(["describe", "--tags", "--exact-match"])
        .output()
    {
        if output.status.success() {
            let tag = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if !tag.is_empty() {
                return format!("refs/tags/{tag}");
            }
        }
    }

    "refs/heads/main".to_owned()
}

/// Uncommitted tracked and untracked files, one per line (`git status
/// --porcelain`). Empty means the working tree is exactly HEAD.
fn git_porcelain() -> anyhow::Result<Vec<String>> {
    let output = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .context("git status")?;
    if !output.status.success() {
        anyhow::bail!(
            "git status failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect())
}

/// Decide, after CI on a dirty tree, whether to materialize the tested tree,
/// push it, and open a PR. Precedence: explicit `--create-pr` flag >
/// head-commit labels (`[pr]` / `[draft]` / `[no-pr]`) > interactive prompt >
/// safe default. An explicit `--push` (without `--create-pr`) still pushes
/// the tested tree non-interactively — the user asked to push; `create_pr`
/// stays false. Returns `None` when the push should not happen at all.
fn decide_dirty_push_opts(
    dirty: &[String],
    explicit_push: bool,
    explicit_pr: bool,
    pr_draft: bool,
    ci_passed: bool,
) -> anyhow::Result<Option<push::PushOpts>> {
    if !ci_passed {
        eprintln!("not pushing a dirty tree because CI failed");
        return Ok(None);
    }
    if explicit_pr {
        return Ok(Some(push::PushOpts {
            create_pr: true,
            draft: pr_draft,
        }));
    }

    let head_message = git_rev_parse("HEAD")
        .ok()
        .and_then(|sha| {
            std::process::Command::new("git")
                .args(["log", "-1", "--format=%B", &sha])
                .output()
                .ok()
        })
        .and_then(|output| {
            output
                .status
                .success()
                .then(|| String::from_utf8_lossy(&output.stdout).to_lowercase())
        })
        .unwrap_or_default();
    if head_message.contains("[no-pr]") {
        return Ok(Some(push::PushOpts {
            create_pr: false,
            draft: pr_draft,
        }));
    }
    if head_message.contains("[pr]") || head_message.contains("[draft]") {
        return Ok(Some(push::PushOpts {
            create_pr: true,
            draft: head_message.contains("[draft]") || pr_draft,
        }));
    }

    if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        // Non-interactive and no label. An explicit `--push` is still an
        // explicit request: push the tested tree without opening a PR. A
        // plain `--create-pr`-less run with a dirty tree stays a no-op.
        return Ok(explicit_push.then_some(push::PushOpts {
            create_pr: false,
            draft: pr_draft,
        }));
    }

    eprintln!(
        "CI {} on your working tree ({} change(s) included):",
        if ci_passed { "passed" } else { "failed" },
        dirty.len()
    );
    for line in dirty {
        eprintln!("  {line}");
    }
    loop {
        eprint!("Commit the tested tree and open a PR? [y/N/d] ");
        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .context("reading prompt answer")?;
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "n" => return Ok(None),
            "y" => {
                return Ok(Some(push::PushOpts {
                    create_pr: true,
                    draft: pr_draft,
                }))
            }
            "d" => {
                return Ok(Some(push::PushOpts {
                    create_pr: true,
                    draft: true,
                }))
            }
            _ => eprintln!("answer y, N, or d"),
        }
    }
}

fn git_rev_parse(rev: &str) -> anyhow::Result<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", rev])
        .output()
        .with_context(|| format!("git rev-parse {rev}"))?;
    if !output.status.success() {
        anyhow::bail!(
            "git rev-parse {rev} failed (are you in a git checkout?): {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_config_user_name() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["config", "user.name"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let name = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!name.is_empty()).then_some(name)
}

async fn cmd_push(args: PushArgs) -> anyhow::Result<()> {
    let client = build_client();
    let url = server_url();
    let run_id = match args.run_id {
        Some(run_id) => run_id,
        None => latest_run_id(&client, &url, None).await?.ok_or_else(|| {
            anyhow::anyhow!("no runs found; pass a run id: `preloop push <run_id>`")
        })?,
    };
    push::push_run(&client, &url, api_token(), &run_id, None).await
}

fn materialize_plan_jobs(
    plans: Vec<preloop_gha_protocol::JobPlan>,
    reusable_workflows: &BTreeMap<String, String>,
) -> anyhow::Result<Vec<preloop_gha_protocol::JobPlan>> {
    let mut materialized = Vec::new();
    for plan in plans {
        let Some(call) = plan.reusable_call.as_ref() else {
            materialized.push(plan);
            continue;
        };
        // Needs-driven matrices are runtime placeholders. Their concrete
        // cells require completed needs outputs, so do not send the empty
        // matrix to `expand_reusable_call` (which correctly rejects it).
        // Keep the placeholder visible and report its deferred expression.
        if plan.deferred_matrix.is_some() {
            materialized.push(plan);
            continue;
        }
        let Some(yaml) = reusable_workflows.get(&call.workflow_file) else {
            // Remote and unresolved calls remain explicit placeholders. The
            // plan must not invent callee jobs without the referenced YAML.
            materialized.push(plan);
            continue;
        };
        let called = preloop_gha_parser::parse_workflow(yaml)
            .map_err(|error| anyhow::anyhow!("parse reusable {}: {error}", call.workflow_file))?;
        let expanded = preloop_gha_parser::expand_reusable_call(
            &called,
            &plan,
            reusable_workflows,
            &BTreeMap::new(),
        )
        .map_err(|error| anyhow::anyhow!("expand reusable {}: {error}", call.workflow_file))?;
        // Keep the caller boundary in the output for downstream `needs`
        // references, then append the concrete callee jobs it gates.
        materialized.push(plan);
        materialized.extend(materialize_plan_jobs(expanded.jobs, reusable_workflows)?);
    }
    Ok(materialized)
}

async fn cmd_plan(args: PlanArgs) -> anyhow::Result<()> {
    let workflow_path = resolve_workflow_path(args.file.as_deref())?;
    let workflow_yaml = std::fs::read_to_string(&workflow_path)
        .with_context(|| format!("failed to read workflow: {}", workflow_path.display()))?;

    let reusable_workflows = collect_local_reusable_workflows(&workflow_path)?;

    let workflow = preloop_gha_parser::parse_workflow(&workflow_yaml)
        .map_err(|error| anyhow::anyhow!("parse {}: {error}", workflow_path.display()))?;
    let expanded =
        preloop_gha_parser::expand_jobs_with_reusables(&workflow, &reusable_workflows)
            .map_err(|error| anyhow::anyhow!("expand {}: {error}", workflow_path.display()))?;
    let plans = materialize_plan_jobs(expanded.jobs, &reusable_workflows)?;

    if args.json {
        let value = serde_json::json!({
            "workflow": workflow_path.display().to_string(),
            "jobs": plans.iter().map(plan_json).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    println!(
        "workflow: {} ({} jobs expanded from {} declared)",
        workflow_path.display(),
        plans.len(),
        workflow.jobs.len()
    );
    let name = workflow.name.as_deref().unwrap_or("(unnamed)");
    println!("name:     {name}");
    println!();
    for plan in &plans {
        let matrix = if plan.matrix.is_empty() {
            String::new()
        } else {
            let cells: Vec<String> = plan
                .matrix
                .iter()
                .map(|(key, value)| match value {
                    serde_json::Value::String(value) => format!("{key}={value}"),
                    other => format!("{key}={other}"),
                })
                .collect();
            format!("  [{}]", cells.join(", "))
        };
        let needs = if plan.needs.is_empty() {
            String::new()
        } else {
            let ids: Vec<&str> = plan.needs.iter().map(|need| need.0.as_str()).collect();
            format!("  needs: {}", ids.join(", "))
        };
        let runner_group = plan
            .runner_group
            .as_deref()
            .map(|group| format!("  runner-group: {group}"))
            .unwrap_or_default();
        let deferred_matrix = plan
            .deferred_matrix
            .as_deref()
            .map(|expression| format!("  deferred-matrix: {expression}"))
            .unwrap_or_default();
        println!(
            "{}{}  runs-on: {}{}{}{}{}",
            plan.id.0,
            matrix,
            plan.runs_on.join(", "),
            needs,
            runner_group,
            deferred_matrix,
            format_args!("  steps: {}", plan.steps.len()),
        );
    }
    Ok(())
}

fn plan_json(plan: &preloop_gha_protocol::JobPlan) -> serde_json::Value {
    serde_json::json!({
        "id": plan.id.0,
        "base_id": plan.base_id,
        "name": plan.name,
        "runner_group": plan.runner_group,
        "runs_on": plan.runs_on,
        "needs": plan.needs.iter().map(|need| need.0.clone()).collect::<Vec<_>>(),
        "matrix": plan.matrix,
        "deferred_matrix": plan.deferred_matrix,
        "steps": plan.steps.len(),
    })
}

async fn cmd_status(args: StatusArgs) -> anyhow::Result<()> {
    let client = build_client();
    let url = server_url();
    // Native bearer required for /api/v1/status (same as other native calls)
    let mut status_req = client.get(format!("{url}/api/v1/status"));
    if let Some(token) = api_token() {
        status_req = status_req.bearer_auth(token);
    }
    let status_resp = status_req
        .send()
        .await
        .with_context(engine_unreachable_context)?;
    if !status_resp.status().is_success() {
        let status = status_resp.status();
        let body = status_resp.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    let status_text = status_resp.text().await?;
    if args.json {
        // Byte-for-byte, no prose: so jq works
        print!("{}", status_text);
        if !status_text.ends_with('\n') {
            println!();
        }
        return Ok(());
    }
    let status: serde_json::Value =
        serde_json::from_str(&status_text).context("parse status json")?;

    // Fetch recent runs for table (preserve existing behavior)
    let mut runs_req = client.get(format!("{url}/api/v1/runs?limit={}", args.limit));
    if let Some(token) = api_token() {
        runs_req = runs_req.bearer_auth(token);
    }
    let runs: Vec<serde_json::Value> = match runs_req.send().await {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or_default(),
        Ok(r) => {
            let st = r.status();
            let body = r.text().await.unwrap_or_default();
            eprintln!("[warn] runs table unavailable: {st}: {body}");
            Vec::new()
        }
        Err(e) => {
            eprintln!("[warn] runs table unavailable: {e}");
            Vec::new()
        }
    };

    // --- Human rendering: 10 sections in order ---
    render_status_human(&status, &runs, args.limit);
    Ok(())
}

fn render_status_human(status: &serde_json::Value, runs: &[serde_json::Value], limit: usize) {
    // Helpers to extract safely
    let get_str = |v: &serde_json::Value, k: &str| -> Option<String> {
        v.get(k).and_then(|x| x.as_str()).map(|s| s.to_owned())
    };
    let get_f64 =
        |v: &serde_json::Value, k: &str| -> Option<f64> { v.get(k).and_then(|x| x.as_f64()) };
    let get_u64 =
        |v: &serde_json::Value, k: &str| -> Option<u64> { v.get(k).and_then(|x| x.as_u64()) };
    let get_bool =
        |v: &serde_json::Value, k: &str| -> Option<bool> { v.get(k).and_then(|x| x.as_bool()) };

    // 1. service + snapshot age
    println!("== service ==");
    let service = status.get("service").unwrap_or(&serde_json::Value::Null);
    let version =
        get_str(service, "version").unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
    let instance = get_str(service, "instance_id")
        .or_else(|| get_str(status, "instance_id"))
        .unwrap_or_else(|| "-".to_owned());
    let uptime = get_f64(service, "uptime_seconds")
        .or_else(|| get_f64(status, "uptime_seconds"))
        .unwrap_or(0.0);
    let shutdown = get_bool(service, "shutdown_requested")
        .or_else(|| get_bool(status, "shutdown_requested"))
        .unwrap_or(false);
    let snapshot_age = get_f64(status, "snapshot_age_seconds").unwrap_or(0.0);
    let observed_at = get_str(status, "observed_at").unwrap_or_else(|| "-".to_owned());
    let overall = get_str(status, "overall").unwrap_or_else(|| "unknown".to_owned());
    println!(
        "  version: {version}  instance: {instance}  uptime: {uptime:.0}s  overall: {overall}"
    );
    println!(
        "  observed_at: {observed_at}  snapshot_age: {snapshot_age:.1}s  shutdown: {shutdown}"
    );
    if status.get("schema_version").is_some() {
        println!("  schema_version: {}", status["schema_version"]);
    }

    // 2. queue (ready/blocked + oldest)
    println!("\n== queue ==");
    let jobs = status.get("jobs").unwrap_or(&serde_json::Value::Null);
    let ready = get_u64(jobs, "ready").unwrap_or(0);
    let dep_blocked = get_u64(jobs, "dependency_blocked").unwrap_or(0);
    let conc_blocked = get_u64(jobs, "concurrency_blocked").unwrap_or(0);
    let pending_exp = get_u64(jobs, "pending_expansion").unwrap_or(0);
    let expanding = get_u64(jobs, "expanding").unwrap_or(0);
    let claimable = get_u64(jobs, "claimable").unwrap_or(0);
    let unclaimable = get_u64(jobs, "unclaimable").unwrap_or(0);
    let oldest = get_f64(jobs, "oldest_ready_seconds");
    println!("  ready: {ready}  claimable: {claimable}  unclaimable: {unclaimable}  dependency_blocked: {dep_blocked}  concurrency_blocked: {conc_blocked}  pending_expansion: {pending_exp}  expanding: {expanding}");
    match oldest {
        Some(v) => println!("  oldest_ready: {v:.1}s"),
        None => println!("  oldest_ready: -"),
    }
    // Also show runs queued/in_progress if present
    if let Some(runs_obj) = status.get("runs") {
        let q = get_u64(runs_obj, "queued").unwrap_or(0);
        let ip = get_u64(runs_obj, "in_progress").unwrap_or(0);
        let completed = get_u64(runs_obj, "completed").unwrap_or(0);
        println!("  runs queued: {q}  in_progress: {ip}  completed: {completed}");
    }

    // 3. concurrency + scheduler
    println!("\n== concurrency & scheduler ==");
    let conc = status
        .get("concurrency")
        .unwrap_or(&serde_json::Value::Null);
    let groups_active = get_u64(conc, "groups_active").unwrap_or(0);
    let groups_contended = get_u64(conc, "groups_contended").unwrap_or(0);
    let pending_holders = get_u64(conc, "pending_holders").unwrap_or(0);
    let deepest = get_u64(conc, "deepest_group_pending").unwrap_or(0);
    let qmax = get_u64(conc, "queue_max_pending").unwrap_or(0);
    let overflow = get_u64(conc, "overflow_cancellations").unwrap_or(0);
    println!("  groups active: {groups_active}  contended: {groups_contended}  pending_holders: {pending_holders}  deepest_pending: {deepest}  queue_max: {qmax}  overflow_cancellations: {overflow}");
    let sched = status.get("scheduler").unwrap_or(&serde_json::Value::Null);
    let enabled = get_bool(sched, "enabled").unwrap_or(false);
    let schedules = get_u64(sched, "schedules").unwrap_or(0);
    let last_scan = get_str(sched, "last_scan_at").unwrap_or_else(|| "-".to_owned());
    let next_fire = get_str(sched, "next_fire_at").unwrap_or_else(|| "-".to_owned());
    let fired = get_u64(sched, "fired").unwrap_or(0);
    let skipped = get_u64(sched, "skipped_overlapping").unwrap_or(0);
    let late = get_u64(sched, "late_fires").unwrap_or(0);
    let max_delay = get_f64(sched, "max_fire_delay_seconds");
    println!("  scheduler enabled: {enabled}  schedules: {schedules}  fired: {fired}  skipped_overlapping: {skipped}  late_fires: {late}");
    println!(
        "  last_scan: {last_scan}  next_fire: {next_fire}  max_delay: {}",
        max_delay
            .map(|v| format!("{v:.1}s"))
            .unwrap_or_else(|| "-".to_owned())
    );

    // 4. pool + runners
    println!("\n== pool & runners ==");
    let pool = status.get("pool").unwrap_or(&serde_json::Value::Null);
    let mode = get_str(pool, "mode").unwrap_or_else(|| "-".to_owned());
    let desired = get_u64(pool, "desired").unwrap_or(0);
    let preparing = pool
        .get("preparing")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let building = get_u64(pool, "building").unwrap_or(0);
    let provisioning = get_u64(pool, "provisioning").unwrap_or(0);
    let pool_idle = get_u64(pool, "idle").unwrap_or(0);
    let pool_busy = get_u64(pool, "busy").unwrap_or(0);
    let paused = get_u64(pool, "paused").unwrap_or(0);
    let failures = get_u64(pool, "consecutive_provision_failures").unwrap_or(0);
    println!("  pool mode: {mode}  desired: {desired}  idle: {pool_idle}  busy: {pool_busy}  building: {building}  provisioning: {provisioning}  paused: {paused}  preparing: {preparing}  provision_failures: {failures}");
    let runners = status.get("runners").unwrap_or(&serde_json::Value::Null);
    let reg = get_u64(runners, "registered").unwrap_or(0);
    let sessions = get_u64(runners, "sessions").unwrap_or(0);
    let idle = get_u64(runners, "idle").unwrap_or(0);
    let busy = get_u64(runners, "busy").unwrap_or(0);
    let stale = get_u64(runners, "stale").unwrap_or(0);
    let max_poll = get_f64(runners, "max_poll_age_seconds");
    let max_lease = get_f64(runners, "max_lease_age_seconds");
    println!("  runners registered: {reg}  sessions: {sessions}  idle: {idle}  busy: {busy}  stale: {stale}");
    println!(
        "  max_poll_age: {}  max_lease_age: {}",
        max_poll
            .map(|v| format!("{v:.1}s"))
            .unwrap_or_else(|| "-".to_owned()),
        max_lease
            .map(|v| format!("{v:.1}s"))
            .unwrap_or_else(|| "-".to_owned())
    );

    // 5. VM fleet stub
    println!("\n== vm fleet ==");
    let vms = status
        .get("vms")
        .unwrap_or(status.get("vm").unwrap_or(&serde_json::Value::Null));
    if vms.is_null() || vms.as_object().map(|m| m.is_empty()).unwrap_or(false) {
        println!("  source: unavailable  (host sampler not yet reporting)");
        println!("  capabilities: cpu=false memory=false sparse_disk=false");
        println!("  host_usage: -");
    } else {
        let source = get_str(vms, "source").unwrap_or_else(|| "unknown".to_owned());
        let sample_age = get_f64(vms, "sample_age_seconds")
            .map(|v| format!("{v:.1}s"))
            .unwrap_or_else(|| "-".to_owned());
        println!("  source: {source}  sample_age: {sample_age}");
        if let Some(caps) = vms.get("capabilities") {
            let cap_str = caps
                .as_object()
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| format!("{k}={}", v.as_bool().unwrap_or(false)))
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .unwrap_or_else(|| "-".to_owned());
            println!("  capabilities: {cap_str}");
        }
        if let Some(cnt) = vms.get("count") {
            let runner = get_u64(cnt, "runner").unwrap_or(0);
            let golden = get_u64(cnt, "golden").unwrap_or(0);
            let unavailable = get_u64(cnt, "unavailable").unwrap_or(0);
            println!("  count runner: {runner}  golden: {golden}  unavailable: {unavailable}");
        }
        if let Some(conf) = vms.get("configured") {
            let vcpus = get_u64(conf, "vcpus")
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_owned());
            let mem = get_u64(conf, "memory_bytes")
                .map(|v| format!("{v}"))
                .unwrap_or_else(|| "-".to_owned());
            let storage = get_u64(conf, "storage_bytes")
                .map(|v| format!("{v}"))
                .unwrap_or_else(|| "-".to_owned());
            println!("  configured vcpus: {vcpus}  memory: {mem}  storage: {storage}");
        }
        if let Some(usage) = vms.get("host_usage") {
            let cores = get_f64(usage, "cpu_cores")
                .map(|v| format!("{v:.1}"))
                .unwrap_or_else(|| "-".to_owned());
            let mem = get_u64(usage, "memory_bytes")
                .map(|v| format!("{v}"))
                .unwrap_or_else(|| "-".to_owned());
            println!("  host_usage cpu_cores: {cores}  memory: {mem}");
        }
        if let Some(top) = vms.get("top_consumers").and_then(|x| x.as_array()) {
            if !top.is_empty() {
                println!("  top_consumers ({}):", top.len().min(5));
                for c in top.iter().take(5) {
                    let name = c
                        .get("machine_name")
                        .and_then(|x| x.as_str())
                        .unwrap_or("?");
                    let role = c.get("role").and_then(|x| x.as_str()).unwrap_or("?");
                    let activity = c.get("activity").and_then(|x| x.as_str()).unwrap_or("?");
                    println!("    - {name} ({role}/{activity})");
                }
            } else {
                println!("  top_consumers: -");
            }
        }
    }

    // 6. store/storage/GitHub/debug/telemetry
    println!("\n== store / storage / github / debug / telemetry ==");
    let store = status.get("store").unwrap_or(&serde_json::Value::Null);
    let backend = get_str(store, "backend").unwrap_or_else(|| "-".to_owned());
    let cfail = get_u64(store, "consecutive_failures").unwrap_or(0);
    println!("  store backend: {backend}  consecutive_failures: {cfail}");
    let storage = status.get("storage").unwrap_or(&serde_json::Value::Null);
    if !storage.is_null() {
        let free = get_u64(storage, "state_fs_free_bytes")
            .map(|v| format!("{v}"))
            .unwrap_or_else(|| "-".to_owned());
        let ratio = get_f64(storage, "state_fs_free_ratio")
            .map(|v| format!("{v:.2}"))
            .unwrap_or_else(|| "-".to_owned());
        println!("  storage free: {free}  ratio: {ratio}");
        if let Some(comps) = storage.get("components").and_then(|x| x.as_array()) {
            for c in comps {
                let store_name = c.get("store").and_then(|x| x.as_str()).unwrap_or("?");
                let bytes = c.get("bytes").and_then(|x| x.as_u64()).unwrap_or(0);
                println!("    {store_name}: {bytes} bytes");
            }
        }
    } else {
        println!("  storage: -");
    }
    let github = status.get("github").unwrap_or(&serde_json::Value::Null);
    if !github.is_null() {
        let configured = get_bool(github, "configured").unwrap_or(false);
        let pending = get_u64(github, "pending_check_updates").unwrap_or(0);
        println!("  github configured: {configured}  pending_check_updates: {pending}");
        if let Some(rl) = github.get("rate_limit") {
            let remaining = get_u64(rl, "remaining").unwrap_or(0);
            let limit_rl = get_u64(rl, "limit").unwrap_or(0);
            println!("  github rate_limit: {remaining}/{limit_rl} remaining");
        }
        if let Some(exp) = github
            .get("installation_token_expires_in_seconds")
            .and_then(|x| x.as_u64())
        {
            println!("  github token expires_in: {exp}s");
        }
    } else {
        println!("  github: -");
    }
    let debug = status.get("debug").unwrap_or(&serde_json::Value::Null);
    if !debug.is_null() {
        let active = get_u64(debug, "active_sessions").unwrap_or(0);
        let oldest = debug
            .get("oldest_session_seconds")
            .and_then(|x| x.as_f64())
            .map(|v| format!("{v:.0}s"))
            .unwrap_or_else(|| "-".to_owned());
        println!("  debug active_sessions: {active}  oldest: {oldest}");
    }
    let tele = status.get("telemetry").unwrap_or(&serde_json::Value::Null);
    if !tele.is_null() {
        let enabled = get_bool(tele, "otlp_enabled").unwrap_or(false);
        let dropped = get_u64(tele, "dropped_records").unwrap_or(0);
        println!("  telemetry otlp_enabled: {enabled}  dropped_records: {dropped}");
    }

    // 7. non-zero limits
    println!("\n== limits (non-zero) ==");
    let limits = status.get("limits").and_then(|x| x.as_array());
    let mut any_limit = false;
    if let Some(arr) = limits {
        for l in arr {
            let dropped = l.get("dropped").and_then(|x| x.as_u64()).unwrap_or(0);
            let rejected = l.get("rejected").and_then(|x| x.as_u64()).unwrap_or(0);
            if dropped > 0 || rejected > 0 {
                any_limit = true;
                let name = l.get("limit").and_then(|x| x.as_str()).unwrap_or("?");
                let value = l
                    .get("value")
                    .and_then(|x| x.as_u64())
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_owned());
                println!("  {name}: value={value} dropped={dropped} rejected={rejected}");
            }
        }
    }
    if !any_limit {
        println!("  (no limits with drops/rejects)");
    }

    // 8. stale tasks
    println!("\n== tasks (stale/exited) ==");
    let tasks = status.get("tasks").and_then(|x| x.as_array());
    let mut any_task = false;
    if let Some(arr) = tasks {
        for t in arr {
            let state = t.get("state").and_then(|x| x.as_str()).unwrap_or("running");
            if state == "stale" || state == "exited" {
                any_task = true;
                let name = t.get("name").and_then(|x| x.as_str()).unwrap_or("?");
                let critical = t.get("critical").and_then(|x| x.as_bool()).unwrap_or(false);
                let age = t
                    .get("heartbeat_age_seconds")
                    .and_then(|x| x.as_f64())
                    .map(|v| format!("{v:.1}s"))
                    .unwrap_or_else(|| "-".to_owned());
                println!("  {name}: state={state} critical={critical} age={age}");
            }
        }
    }
    if !any_task {
        println!("  (all tasks healthy)");
    }

    // 9. conditions with one-line actions (≤5 exemplars)
    println!("\n== conditions ==");
    let conditions = status.get("conditions").and_then(|x| x.as_array());
    if let Some(arr) = conditions {
        if arr.is_empty() {
            println!("  (no conditions)");
        } else {
            for c in arr {
                let code = c.get("code").and_then(|x| x.as_str()).unwrap_or("?");
                let severity = c.get("severity").and_then(|x| x.as_str()).unwrap_or("info");
                let msg = c.get("message").and_then(|x| x.as_str()).unwrap_or("");
                let action = condition_action(code);
                println!("  [{severity}] {code}: {msg} -> {action}");
                if let Some(exs) = c.get("exemplars").and_then(|x| x.as_array()) {
                    for ex in exs.iter().take(5) {
                        let ex_str = match ex {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        println!("    - {ex_str}");
                    }
                    if exs.len() > 5 {
                        println!("    ... and {} more", exs.len() - 5);
                    }
                } else if let Some(exs) = c.get("exemplar").and_then(|x| x.as_str()) {
                    println!("    - {exs}");
                }
            }
        }
    } else {
        println!("  (no conditions)");
    }

    // 10. recent runs table
    println!("\n== recent runs (limit={}) ==", limit);
    if runs.is_empty() {
        println!("No runs found.");
        return;
    }
    println!(
        "{:<38}  {:<6}  {:<12}  {:<12}  {:<10}  WORKFLOW",
        "RUN ID", "#", "STATUS", "EVENT", "PUSH"
    );
    println!("{}", "-".repeat(104));
    for run in runs {
        let run_id = run["run_id"].as_str().unwrap_or("?");
        let run_number = run
            .get("run_number")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let st = run["status"].as_str().unwrap_or("?");
        let event = run
            .get("event")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                run.get("submission")
                    .and_then(|s| s.get("event"))
                    .and_then(serde_json::Value::as_str)
            })
            .unwrap_or("?");
        let workflow = run
            .get("workflow_path")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                run.get("workflow_path_str")
                    .and_then(serde_json::Value::as_str)
            })
            .or_else(|| {
                run.get("submission")
                    .and_then(|s| s.get("workflow_path"))
                    .and_then(serde_json::Value::as_str)
            })
            .unwrap_or("?");
        let push = match run.get("push_state").and_then(|state| state.get("status")) {
            Some(s) => {
                let s = s.as_str().unwrap_or("?");
                match run["push_state"]["pr_number"].as_u64() {
                    Some(n) => format!("{s} #{n}"),
                    None => s.to_owned(),
                }
            }
            None => "-".to_owned(),
        };
        println!(
            "{:<38}  {:<6}  {:<12}  {:<12}  {:<10}  {}",
            run_id, run_number, st, event, push, workflow
        );
    }
}

fn condition_action(code: &str) -> &'static str {
    match code {
        "queue_no_registered_runner" => "register a runner or enable the pool",
        "queue_label_mismatch" => "add a runner with that label",
        "concurrency_queue_overflow" => "raise concurrency queue max or reduce parallelism",
        "concurrency_group_starved" => "check concurrency group that starves others",
        "scheduler_scan_stale" => "check scheduler scan heartbeat (restart if stuck)",
        "scheduler_fire_late" => "check scheduler clock / load",
        "pool_preparing" => "wait for image preparation to finish",
        "pool_provisioning_deficit" => "check pool capacity / provision failures",
        "pool_repeated_provision_failure" => "inspect provision logs and VM host capacity",
        "runner_poll_stale" => "check runner connectivity and heartbeat",
        "runner_lease_stale" => "check runner lease renewal",
        "vm_sampler_stale" | "vm_sample_unavailable" | "vm_unreachable" => {
            "check VM host sampler and SmolVM health"
        }
        "vm_host_memory_pressure" => "free host memory or reduce pool size",
        "vm_host_cpu_throttled" => "reduce host CPU load or raise CPU quota",
        "vm_host_oom_kill" => "check host OOM kills and runner memory",
        "vm_sparse_disk_pressure" => "free disk space on VM data volume",
        "store_write_failure" | "store_connection_down" => "check store connectivity and disk",
        "storage_capacity_pressure" => "free disk space or run GC/prune",
        "limit_drop_active" => "raise that limit or reduce load",
        "limit_reject_active" => "raise that limit or back off",
        "github_check_update_failure" => "check GitHub App permissions and network",
        "github_terminal_check_pending" => "retry check update or check GitHub status",
        "github_rate_limit_low" => "back off GitHub API or wait for rate-limit reset",
        "github_installation_token_expiring" => "refresh GitHub installation token",
        "debug_session_stale" => "close stale debug session",
        "debug_audit_evicted" => "increase audit retention or flush audits",
        "telemetry_export_failure" => "check OTLP endpoint and credentials",
        "state_sampler_stale" | "task_stale" | "task_exited" => "check background task health",
        _ => "see runbook for this condition",
    }
}

async fn cmd_logs(args: LogsArgs) -> anyhow::Result<()> {
    let client = build_client();
    let url = server_url();
    let run_id = match args.run_id {
        Some(id) => id,
        None => latest_run_id(&client, &url, None)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no runs found"))?,
    };
    let mut request = client.get(format!("{url}/api/v1/runs/{run_id}/logs"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    let body = response.text().await?;
    print!("{body}");
    Ok(())
}

async fn cmd_cancel(args: CancelArgs) -> anyhow::Result<()> {
    let client = build_client();
    let url = server_url();
    let run_id = match args.run_id {
        Some(id) => id,
        None => latest_run_id(&client, &url, Some("in_progress"))
            .await?
            .ok_or_else(|| anyhow::anyhow!("no active runs found"))?,
    };
    let mut request = client.post(format!("{url}/api/v1/runs/{run_id}/cancel"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    println!("Run {run_id} cancelled.");
    Ok(())
}

async fn latest_run_id(
    client: &reqwest::Client,
    url: &str,
    status: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let query = status.map_or_else(
        || "limit=1".to_owned(),
        |status| format!("status={status}&limit=1"),
    );
    let mut request = client.get(format!("{url}/api/v1/runs?{query}"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    let runs: Vec<serde_json::Value> = response.json().await?;
    Ok(runs
        .first()
        .and_then(|run| run["run_id"].as_str())
        .map(str::to_owned))
}

async fn cmd_shell(args: ShellArgs) -> anyhow::Result<()> {
    let debug_dir = preloop_home().join("state").join("debug");

    // Find the preserved VM. If a run_ref is given, treat it as a machine
    // name prefix; otherwise pick the first (usually only) marker.
    let machine_name = if let Some(run_ref) = &args.run_ref {
        let path = debug_dir.join(run_ref);
        if path.is_file() {
            run_ref.clone()
        } else {
            // Try matching as a prefix (e.g. "0" → "preloop-runner-0")
            find_debug_machine(&debug_dir, Some(run_ref))?
        }
    } else {
        find_debug_machine(&debug_dir, None)?
    };

    let marker = debug_dir.join(&machine_name);
    eprintln!("[preloop] Connecting to preserved VM: {machine_name}");
    eprintln!("[preloop] Exit the shell to release the VM.");

    // Build the smolvm command BEFORE claiming the marker: an invalid
    // sandbox override makes this fail, and the marker claim + heartbeat
    // below must not exist yet when it does — otherwise the preserved VM
    // would stay marked ACTIVE (and the heartbeat would keep touching the
    // marker) until the orchestrator's idle timeout, with nobody attached.
    let mut command = crate::smolvm_command()?;

    // Claim the session before starting so the orchestrator stops counting down.
    let _ = std::fs::write(&marker, preloop_orchestrator::DEBUG_MARKER_ACTIVE);

    // Spawn a background task to touch the marker every 15 seconds,
    // keeping the orchestrator's 10-minute timeout alive.
    let heartbeat_marker = marker.clone();
    let heartbeat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            if std::fs::write(&heartbeat_marker, preloop_orchestrator::DEBUG_MARKER_ACTIVE).is_err()
            {
                break;
            }
        }
    });

    // Run smolvm machine shell interactively. The shell boots a stopped
    // machine, so the sandbox environment applies exactly as for a provider
    // spawn.
    let status = command
        .args(["machine", "shell", "--name", &machine_name])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .context("failed to run smolvm machine shell")?;

    heartbeat.abort();

    // Remove the marker so the orchestrator cleans up the VM.
    let _ = std::fs::remove_file(&marker);
    eprintln!("[preloop] Shell exited — VM will be cleaned up.");

    if !status.success() {
        anyhow::bail!("shell exited with {status}");
    }
    Ok(())
}

fn find_debug_machine(
    debug_dir: &std::path::Path,
    prefix: Option<&String>,
) -> anyhow::Result<String> {
    let entries: Vec<String> = std::fs::read_dir(debug_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(prefix) = prefix {
                if name.contains(prefix) {
                    Some(name)
                } else {
                    None
                }
            } else {
                Some(name)
            }
        })
        .collect();

    match entries.len() {
        0 => anyhow::bail!(
            "no preserved VMs found. Run with --preserve-on-failure to keep failed job VMs alive"
        ),
        1 => Ok(entries.into_iter().next().unwrap()),
        _ => anyhow::bail!(
            "multiple preserved VMs found: {}. Specify one with: preloop shell <name>",
            entries.join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;

    /// Serializes tests that mutate process-global env vars read by
    /// `local_runner_pool_config` (`PRELOOP_RUNNER_BUNDLE`,
    /// `PRELOOP_RUNNER_BASE_IMAGE`, `PRELOOP_RUNNER_STORAGE_GB`): parallel
    /// test threads would otherwise race each other's set_var/remove_var
    /// pairs.
    static TEST_ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn first_serve_token_creates_private_home_and_file() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("missing").join(".preloop");
        assert!(!home.exists());

        let token = prepare_engine_token(&home, None).unwrap();

        assert_eq!(token.len(), 64);
        assert_eq!(
            std::fs::read_to_string(home.join("engine.token")).unwrap(),
            token
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&home).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                std::fs::metadata(home.join("engine.token"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("preloop").chain(args.iter().copied()))
    }

    #[test]
    fn build_golden_accepts_custom_image_and_storage_override() {
        let cli = parse(&[
            "build-golden",
            "--runner-bundle",
            "/tmp/runner",
            "--output",
            "/tmp/golden",
            "--base-image",
            "ghcr.io/acme/runner-images:ubuntu24-runner-large-latest-arm64",
            "--storage-gb",
            "80",
        ])
        .unwrap();

        let Command::BuildGolden(args) = cli.command else {
            panic!("expected build-golden command");
        };
        assert_eq!(
            args.base_image,
            "ghcr.io/acme/runner-images:ubuntu24-runner-large-latest-arm64"
        );
        assert_eq!(args.storage_gib, Some(80));
    }

    #[test]
    fn base_digest_policy_rejects_mutable_registry_references() {
        assert!(require_digest_pinned_base(
            "mirror.gcr.io/library/ubuntu:24.04@sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90"
        )
        .is_ok());
        assert!(require_digest_pinned_base("/tmp/preloop.smolmachine").is_ok());
        assert!(require_digest_pinned_base("ghcr.io/acme/base:latest").is_err());
        assert!(require_digest_pinned_base("ghcr.io/acme/base@sha256:not-a-digest").is_err());
    }

    #[test]
    fn runner_storage_gib_ignores_invalid_or_zero_values() {
        let _env_guard = TEST_ENV_MUTEX.lock().unwrap();
        for (value, expected) in [
            ("80", 80),
            (" 96 ", 96),
            ("0", RUNNER_STORAGE_GIB),
            ("not-a-number", RUNNER_STORAGE_GIB),
        ] {
            unsafe {
                std::env::set_var("PRELOOP_RUNNER_STORAGE_GB", value);
            }
            assert_eq!(runner_storage_gib(), expected, "{value}");
        }
        unsafe {
            std::env::remove_var("PRELOOP_RUNNER_STORAGE_GB");
        }
        assert_eq!(runner_storage_gib(), RUNNER_STORAGE_GIB);
    }

    #[test]
    fn warm_pool_tracks_host_capacity_within_bounds() {
        let parallelism = std::thread::available_parallelism().map_or(2, |value| value.get());

        // `cpus_per_runner = 0` would divide by zero; it is read as one core.
        for cpus in [0_u16, 1, 2, 4, 64, u16::MAX] {
            let by_cpu = (parallelism / usize::from(cpus.max(1))).max(1);
            let size = host_runner_pool_size(cpus);

            assert!(size >= 1, "a host must always keep a runner warm");
            assert!(size <= WARM_POOL_CAP, "{size} exceeds the warm pool cap");
            assert!(
                size >= by_cpu.min(WARM_POOL_CAP),
                "warm pool {size} starves the {by_cpu} jobs this host can run at once"
            );
            assert!(
                size <= by_cpu.saturating_mul(2).max(1),
                "warm pool {size} parks more than twice the CPU budget of {by_cpu}"
            );
        }
    }

    #[test]
    fn env_flag_accepts_common_boolean_values() {
        for value in ["1", "true", "yes", "on", " TRUE "] {
            assert_eq!(parse_flag(value), Some(true));
        }
        for value in ["0", "false", "no", "off", " OFF "] {
            assert_eq!(parse_flag(value), Some(false));
        }
        assert_eq!(parse_flag("maybe"), None);
        assert!(env_flag("__PRELOOP_TEST_FLAG_UNSET__", true));
        assert!(!env_flag("__PRELOOP_TEST_FLAG_UNSET__", false));
        assert!(!env_flag(
            "__PRELOOP_TEST_POOL_FLAG_UNSET__",
            DEFAULT_RUNNER_POOL_ENABLED
        ));
        assert!(env_flag(
            "__PRELOOP_TEST_GOLDEN_FLAG_UNSET__",
            DEFAULT_USE_PACKED_GOLDEN
        ));
    }

    #[test]
    fn runner_pool_labels_includes_base_and_extra_labels() {
        unsafe {
            std::env::set_var("PRELOOP_RUNNER_LABELS", "X64,linux, custom ");
        }
        let labels = runner_pool_labels();
        unsafe {
            std::env::remove_var("PRELOOP_RUNNER_LABELS");
        }
        assert!(labels.iter().any(|l| l == "self-hosted"));
        assert!(labels.iter().any(|l| l == "Linux"));
        assert!(labels.iter().any(|l| l == std::env::consts::ARCH));
        // X64 appended; `linux` dropped (case-insensitive duplicate of the
        // base label); `custom` trimmed and appended.
        assert!(labels.iter().any(|l| l == "X64"));
        assert!(!labels.iter().any(|l| l == "linux"));
        assert!(labels.iter().any(|l| l == "custom"));
        assert_eq!(labels.len(), 5);
        // Empty extras add nothing.
        unsafe {
            std::env::set_var("PRELOOP_RUNNER_LABELS", ", ,");
        }
        let labels = runner_pool_labels();
        unsafe {
            std::env::remove_var("PRELOOP_RUNNER_LABELS");
        }
        assert_eq!(labels.len(), 3);
    }

    #[test]
    fn custom_base_image_disables_environment_replacement() {
        let _env_guard = TEST_ENV_MUTEX.lock().unwrap();
        let home = tempfile::tempdir().unwrap();
        // The config resolves a Linux guest runner bundle; fabricate one so
        // the construction reaches the base-image decision.
        let bundle = tempfile::tempdir().unwrap();
        std::fs::write(
            bundle.path().join("preloop-runner"),
            [0x7f, b'E', b'L', b'F'],
        )
        .unwrap();
        unsafe {
            std::env::set_var("PRELOOP_RUNNER_BUNDLE", bundle.path());
            std::env::set_var("PRELOOP_RUNNER_STORAGE_GB", "80");
            std::env::remove_var("PRELOOP_USE_PACKED_GOLDEN");
        }
        let queue_depth = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let next_job_runs_on =
            std::sync::Arc::new(std::sync::RwLock::new(vec!["ubuntu-latest".to_owned()]));
        let config = |base: &str| {
            unsafe {
                std::env::set_var("PRELOOP_RUNNER_BASE_IMAGE", base);
            }
            let config = local_runner_pool_config(
                home.path(),
                "http://127.0.0.1:9090".to_owned(),
                None,
                None,
                queue_depth.clone(),
                next_job_runs_on.clone(),
                false,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                std::sync::Arc::new(std::sync::RwLock::new(std::collections::BTreeMap::new())),
            )
            .unwrap();
            unsafe {
                std::env::remove_var("PRELOOP_RUNNER_BASE_IMAGE");
            }
            config
        };
        // Stock base: environment-based replacement stays enabled.
        let stock = config("ubuntu:24.04");
        assert!(stock.next_job_runs_on.is_some());
        assert!(
            stock.use_packed_artifact,
            "packed golden use must be enabled by default"
        );
        assert!(
            stock.use_fork,
            "packed golden forks must remain enabled when the warm pool is off"
        );
        assert_eq!(stock.storage_gib, 80);
        assert!(
            config("mirror.gcr.io/library/ubuntu:24.04")
                .next_job_runs_on
                .is_some(),
            "the mirrored pinned Ubuntu image is still a stock environment"
        );
        // Custom base (artifact): disabled, so idle runners are never
        // replaced by the job's implied stock base.
        assert!(config("/tmp/custom.smolmachine").next_job_runs_on.is_none());
        unsafe {
            std::env::set_var("PRELOOP_USE_PACKED_GOLDEN", "false");
        }
        assert!(
            !config("ubuntu:24.04").use_packed_artifact,
            "operators can still disable packed golden use explicitly"
        );
        unsafe {
            std::env::remove_var("PRELOOP_USE_PACKED_GOLDEN");
            std::env::remove_var("PRELOOP_RUNNER_BUNDLE");
            std::env::remove_var("PRELOOP_RUNNER_STORAGE_GB");
        }
    }

    #[test]
    fn packed_artifact_cache_key_tracks_base_image_digest() {
        let _env_guard = TEST_ENV_MUTEX.lock().unwrap();
        let home = tempfile::tempdir().unwrap();
        let bundle = tempfile::tempdir().unwrap();
        std::fs::write(
            bundle.path().join("preloop-runner"),
            [0x7f, b'E', b'L', b'F'],
        )
        .unwrap();
        unsafe {
            std::env::set_var("PRELOOP_RUNNER_BUNDLE", bundle.path());
        }
        let queue_depth = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let next_job_runs_on =
            std::sync::Arc::new(std::sync::RwLock::new(vec!["ubuntu-latest".to_owned()]));
        let stem = |digest: &str| {
            unsafe {
                std::env::set_var(
                    "PRELOOP_RUNNER_BASE_IMAGE",
                    format!("ubuntu:24.04@sha256:{digest}"),
                );
            }
            let config = local_runner_pool_config(
                home.path(),
                "http://127.0.0.1:9090".to_owned(),
                None,
                None,
                queue_depth.clone(),
                next_job_runs_on.clone(),
                false,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                std::sync::Arc::new(std::sync::RwLock::new(std::collections::BTreeMap::new())),
            )
            .unwrap();
            unsafe {
                std::env::remove_var("PRELOOP_RUNNER_BASE_IMAGE");
            }
            config.artifact_stem
        };
        let first = stem("aaaa");
        let second = stem("bbbb");
        unsafe {
            std::env::remove_var("PRELOOP_RUNNER_BUNDLE");
        }
        assert_ne!(
            first, second,
            "a digest bump must invalidate the packed golden cache key (stale golden reuse)"
        );
        assert!(
            first.to_string_lossy().contains("ubuntu"),
            "the cache key should still name the base image: {first:?}"
        );
    }

    #[test]
    fn run_no_args() {
        let cli = parse(&["run"]).unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected Run");
        };
        assert!(args.file.is_none());
        assert!(args.job.is_none());
        assert!(args.event.is_none());
        assert!(args.base.is_none());
        assert!(!args.no_debug, "pausing on failure is the default");
        assert!(args.secrets.is_empty());
    }

    #[test]
    fn local_pull_request_defaults_to_synchronize_activity() {
        assert_eq!(
            default_local_activity_type("pull_request", &serde_json::json!({})).as_deref(),
            Some("synchronize")
        );
        assert_eq!(
            default_local_activity_type("pull_request", &serde_json::json!({"action": "opened"}))
                .as_deref(),
            None
        );
        assert_eq!(
            default_local_activity_type("push", &serde_json::json!({})),
            None
        );
    }

    #[test]
    fn run_push_flags() {
        let draft = |args: &[&str]| {
            let cli = parse(args).unwrap();
            let Command::Run(args) = cli.command else {
                panic!("expected Run");
            };
            (args.push, args.create_pr, args.pr_draft)
        };
        assert_eq!(draft(&["run"]), (false, false, true));
        assert_eq!(draft(&["run", "--push"]), (true, false, true));
        // `--create-pr` implies `--push` in `cmd_run`, not in the parser.
        assert_eq!(draft(&["run", "--create-pr"]), (false, true, true));
        // A bare flag still means draft; only an explicit value opts out.
        assert_eq!(draft(&["run", "--push", "--pr-draft"]), (true, false, true));
        assert_eq!(
            draft(&["run", "--push", "--pr-draft=false"]),
            (true, false, false)
        );
    }

    #[test]
    fn run_all_flags() {
        let cli = parse(&[
            "run",
            "-f",
            "ci.yml",
            "--job",
            "test",
            "--event",
            "pull_request",
            "--base",
            "main",
            "--no-debug",
            "--secret",
            "TOKEN=abc",
            "--secret",
            "KEY=xyz",
        ])
        .unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected Run");
        };
        assert_eq!(args.file.unwrap(), PathBuf::from("ci.yml"));
        assert_eq!(args.job.unwrap(), "test");
        assert_eq!(args.event.unwrap(), "pull_request");
        assert_eq!(args.base.unwrap(), "main");
        assert!(args.no_debug);
        assert_eq!(args.secrets, vec!["TOKEN=abc", "KEY=xyz"]);
    }

    #[test]
    fn run_detach_aliases_enable_detached_submission() {
        for args in [["run", "-d"], ["run", "--detach"]] {
            let cli = parse(&args).unwrap();
            let Command::Run(args) = cli.command else {
                panic!("expected Run");
            };
            assert!(args.detach, "detach alias should enable detached mode");
        }
    }

    #[test]
    fn run_file_full_path() {
        let cli = parse(&["run", "-f", ".github/workflows/deploy.yml"]).unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected Run");
        };
        assert_eq!(
            args.file.unwrap(),
            PathBuf::from(".github/workflows/deploy.yml")
        );
    }

    #[test]
    fn run_rejects_unknown_flag() {
        let err = parse(&["run", "--unknown"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn run_job_requires_value() {
        let err = parse(&["run", "--job"]).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidValue | ErrorKind::MissingRequiredArgument
        ));
    }

    // -- plan --

    #[test]
    fn plan_no_args() {
        let cli = parse(&["plan"]).unwrap();
        let Command::Plan(args) = cli.command else {
            panic!("expected Plan");
        };
        assert!(args.file.is_none());
        assert!(!args.json);
    }

    #[test]
    fn plan_json_flag() {
        let cli = parse(&["plan", "-f", "ci.yml", "--json"]).unwrap();
        let Command::Plan(args) = cli.command else {
            panic!("expected Plan");
        };
        assert_eq!(args.file.unwrap(), PathBuf::from("ci.yml"));
        assert!(args.json);
    }

    // -- logs --

    #[test]
    fn logs_filters() {
        let cli = parse(&["logs", "--job", "build", "--step", "3"]).unwrap();
        let Command::Logs(args) = cli.command else {
            panic!("expected Logs");
        };
        assert_eq!(args.job.unwrap(), "build");
        assert_eq!(args.step.unwrap(), 3);
    }

    #[test]
    fn logs_step_rejects_non_numeric() {
        let err = parse(&["logs", "--step", "abc"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ValueValidation);
    }

    // -- secret --

    #[test]
    fn secret_set() {
        let cli = parse(&["secret", "set", "MY_TOKEN"]).unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        let github_setup::SecretCommand::Set { name, .. } = args.command else {
            panic!("expected Set");
        };
        assert_eq!(name, "MY_TOKEN");
    }

    #[test]
    fn secret_set_with_repo_scope() {
        let cli = parse(&[
            "secret",
            "set",
            "MY_TOKEN",
            "--repo",
            "owner/repo",
            "--value",
            "x",
        ])
        .unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        let github_setup::SecretCommand::Set { name, repo, .. } = args.command else {
            panic!("expected Set");
        };
        assert_eq!(name, "MY_TOKEN");
        assert_eq!(repo.as_deref(), Some("owner/repo"));
    }

    #[test]
    fn secret_list() {
        let cli = parse(&["secret", "list"]).unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        let github_setup::SecretCommand::List { repo, .. } = args.command else {
            panic!("expected List");
        };
        assert!(repo.is_none());
    }

    #[test]
    fn secret_list_with_repo_scope() {
        let cli = parse(&["secret", "list", "--repo", "owner/repo"]).unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        let github_setup::SecretCommand::List { repo, .. } = args.command else {
            panic!("expected List");
        };
        assert_eq!(repo.as_deref(), Some("owner/repo"));
    }

    #[test]
    fn secret_set_with_env_scope() {
        let cli = parse(&[
            "secret",
            "set",
            "DEPLOY_KEY",
            "--repo",
            "owner/repo",
            "--env",
            "prod",
            "--value",
            "x",
        ])
        .unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        let github_setup::SecretCommand::Set {
            name, repo, env, ..
        } = args.command
        else {
            panic!("expected Set");
        };
        assert_eq!(name, "DEPLOY_KEY");
        assert_eq!(repo.as_deref(), Some("owner/repo"));
        assert_eq!(env.as_deref(), Some("prod"));
    }

    #[test]
    fn secret_list_with_env_scope() {
        let cli = parse(&["secret", "list", "--repo", "owner/repo", "--env", "prod"]).unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        let github_setup::SecretCommand::List { repo, env } = args.command else {
            panic!("expected List");
        };
        assert_eq!(repo.as_deref(), Some("owner/repo"));
        assert_eq!(env.as_deref(), Some("prod"));
    }

    #[test]
    fn secret_requires_subcommand() {
        let err = parse(&["secret"]).unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    // -- shell --

    #[test]
    fn shell_default_ref() {
        let cli = parse(&["shell"]).unwrap();
        let Command::Shell(args) = cli.command else {
            panic!("expected Shell");
        };
        assert!(args.run_ref.is_none());
    }

    #[test]
    fn shell_explicit_ref() {
        let cli = parse(&["shell", "0"]).unwrap();
        let Command::Shell(args) = cli.command else {
            panic!("expected Shell");
        };
        assert_eq!(args.run_ref.as_deref(), Some("0"));
    }

    /// `preloop shell` spawns `smolvm machine shell`, which boots a stopped
    /// machine — the sandbox environment must reach it exactly like a
    /// provider spawn. The observable contract is the environment of the
    /// spawned process.
    #[tokio::test]
    async fn shell_spawn_carries_the_sandbox_environment() {
        let _guard = crate::SMOLVM_PATH_LOCK.lock().await;
        let (directory, executable) = crate::fake_smolvm_on_path();
        let previous_path = std::env::var_os("PATH");
        let previous_home = std::env::var_os("PRELOOP_HOME");
        let mut path = directory.path().as_os_str().to_owned();
        path.push(":");
        if let Some(previous) = &previous_path {
            path.push(previous);
        }
        std::env::set_var("PATH", path);

        let home = tempfile::tempdir().unwrap();
        let debug_dir = home.path().join("state/debug");
        std::fs::create_dir_all(&debug_dir).unwrap();
        std::fs::write(debug_dir.join("preloop-runner-0-1"), "claimed").unwrap();
        std::env::set_var("PRELOOP_HOME", home.path());

        let result = cmd_shell(ShellArgs {
            run_ref: Some("preloop-runner-0-1".to_owned()),
        })
        .await;

        match previous_home {
            Some(previous) => std::env::set_var("PRELOOP_HOME", previous),
            None => std::env::remove_var("PRELOOP_HOME"),
        }
        match previous_path {
            Some(previous) => std::env::set_var("PATH", previous),
            None => std::env::remove_var("PATH"),
        }
        result.unwrap();

        let read = |suffix: &str| {
            std::fs::read_to_string(executable.with_extension(suffix))
                .unwrap()
                .trim()
                .to_owned()
        };
        #[cfg(target_os = "linux")]
        {
            assert_eq!(read("seccomp"), "SMOLVM_SECCOMP=enforce");
            assert_eq!(read("landlock"), "SMOLVM_LANDLOCK=enforce");
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(read("seccomp"), "SMOLVM_SECCOMP=");
            assert_eq!(read("landlock"), "SMOLVM_LANDLOCK=");
        }
        // The direct CLI spawn must consult the engine's registry, not the
        // caller's: pinned to <PRELOOP_HOME>/smolvm, which the unit sets for
        // the service and this test sets to a temp dir.
        let data_dir = home.path().join("smolvm");
        assert_eq!(
            read("datadir"),
            format!("SMOLVM_DATA_DIR={}", data_dir.display())
        );
    }

    // -- top-level --

    #[test]
    fn no_subcommand_errors() {
        let err = parse(&[]).unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn version_subcommand_parses_without_starting_engine() {
        let cli = parse(&["version"]).unwrap();
        assert!(matches!(cli.command, Command::Version));
    }

    #[test]
    fn version_flag_uses_package_version() {
        let err = Cli::try_parse_from(["preloop", "--version"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DisplayVersion);
        assert!(err.to_string().contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn status_parses() {
        let cli = parse(&["status"]).unwrap();
        let Command::Status(args) = cli.command else {
            panic!("expected Status");
        };
        assert!(!args.json);
        assert_eq!(args.limit, 20);
        let cli = parse(&["status", "--json", "--limit", "5"]).unwrap();
        let Command::Status(args) = cli.command else {
            panic!("expected Status");
        };
        assert!(args.json);
        assert_eq!(args.limit, 5);
        // Default limit is 20 and --json defaults to false
        let cli = parse(&["status", "--limit", "42"]).unwrap();
        let Command::Status(args) = cli.command else {
            panic!("expected Status");
        };
        assert_eq!(args.limit, 42);
    }

    #[test]
    fn hook_lifecycle() {
        // All tests in this binary share one process working directory and
        // run in parallel threads; restore the original cwd so the tempdir
        // (and this test's `.git/`) cannot leak into other tests.
        struct CwdGuard(std::path::PathBuf);
        impl Drop for CwdGuard {
            fn drop(&mut self) {
                let _ = std::env::set_current_dir(&self.0);
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let original = std::env::current_dir().expect("current dir");
        std::env::set_current_dir(repo).unwrap();
        let _guard = CwdGuard(original);

        // `git init` inside this standalone tempdir cannot use the parent
        // checkout's `.git/`, and exercises the same Git path resolution as
        // the real installation flow.
        std::process::Command::new("git")
            .args(["init", "-q"])
            .status()
            .expect("git init")
            .success()
            .then_some(())
            .expect("git init succeeds");

        // A developer-configured global `core.hooksPath` (e.g.
        // `~/.config/git/hooks`) would make `git rev-parse --git-path hooks`
        // resolve outside this repository: `install_hook` would write into —
        // and this test's assertions would miss — the shared location, and
        // the "uninstall" step could clobber a real global hook. Pin the
        // hooks directory to this repository's own `.git/hooks` via a local
        // config so the whole lifecycle stays self-contained.
        let local_hooks = std::fs::canonicalize(repo.join(".git/hooks")).expect("hooks dir");
        assert!(
            std::process::Command::new("git")
                .args(["config", "--local", "core.hooksPath"])
                .arg(&local_hooks)
                .status()
                .expect("git config core.hooksPath")
                .success(),
            "git config core.hooksPath succeeds"
        );

        assert!(!hook_installed(), "should not be installed yet");
        assert!(!hook_decided(), "should not be decided yet");

        install_hook().unwrap();
        assert!(hook_installed(), "should be installed after install_hook");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(".git/hooks/pre-push")
                .unwrap()
                .permissions()
                .mode();
            assert_ne!(mode & 0o111, 0, "hook must be executable");
        }

        // Install is idempotent — no error on second call.
        install_hook().unwrap();
        assert!(hook_installed());
        assert!(
            !std::path::Path::new(".git/hooks/pre-push.preloop-prev").exists(),
            "reinstall over our own hook must not create a backup"
        );

        // A pre-existing third-party hook is chained, not clobbered.
        std::fs::write(".git/hooks/pre-push", "#!/bin/sh\necho 'third-party'\n").unwrap();
        install_hook().unwrap();
        let backed = std::fs::read_to_string(".git/hooks/pre-push.preloop-prev").unwrap();
        assert_eq!(
            backed, "#!/bin/sh\necho 'third-party'\n",
            "the previous hook must be preserved for chaining"
        );
        let installed = std::fs::read_to_string(".git/hooks/pre-push").unwrap();
        assert!(
            installed.contains("pre-push.preloop-prev"),
            "the installed hook must chain the previous one"
        );

        // Uninstall and verify decline mark persists.
        std::fs::remove_file(".git/hooks/pre-push").unwrap();
        mark_hook_declined();
        assert!(hook_decided(), "should be decided after declining");
    }

    #[test]
    fn cancel_parses() {
        let cli = parse(&["cancel"]).unwrap();
        let Command::Cancel(args) = cli.command else {
            panic!("expected Cancel");
        };
        assert!(args.run_id.is_none());
    }

    #[test]
    fn cancel_accepts_run_id() {
        let cli = parse(&["cancel", "run-42"]).unwrap();
        let Command::Cancel(args) = cli.command else {
            panic!("expected Cancel");
        };
        assert_eq!(args.run_id.as_deref(), Some("run-42"));
    }

    #[test]
    fn local_engine_url_keeps_workspace_checkout_offline() {
        assert!(
            should_send_local_workspace_header("http://127.0.0.1:19090", false),
            "an explicit loopback engine URL still targets the local engine"
        );
    }

    #[test]
    fn remote_engine_url_does_not_send_host_workspace_path() {
        assert!(!should_send_local_workspace_header(
            "https://ci.example.test",
            false
        ));
    }

    #[test]
    fn default_engine_transport_sends_workspace_path() {
        assert!(should_send_local_workspace_header(
            "http://127.0.0.1:9090",
            true
        ));
    }

    #[test]
    fn mounted_socket_routes_only_loopback_advertised_origins() {
        assert_eq!(
            mounted_control_origin("http://127.0.0.1:9090/").as_deref(),
            Some("http://127.0.0.1:9090")
        );
        assert_eq!(
            mounted_control_origin("http://localhost:9090").as_deref(),
            Some("http://localhost:9090")
        );
        assert_eq!(mounted_control_origin("https://preloop.preloop.dev"), None);
    }
}
