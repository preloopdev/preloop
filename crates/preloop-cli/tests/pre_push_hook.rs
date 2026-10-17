//! End-to-end harness for the pre-push CI gate (`PRE_PUSH_HOOK`).
//!
//! Every case runs the exact hook text `preloop` installs — extracted from the
//! `PRE_PUSH_HOOK` raw string in `src/main.rs` and checked byte-for-byte
//! against `contrib/pre-push` — through a real `git push` into a scratch bare
//! repository, with a fake `preloop` CLI standing in for the engine. No
//! engine, no VM, no network, no `preloop.pid`: the fake is the only
//! "preloop" executable on `PATH`, and each case asserts that the push either
//! proceeded or was rejected, plus exactly which CLI invocations happened.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

const ZERO_SHA: &str = "0000000000000000000000000000000000000000";

/// The hook text as installed: cut out of the `PRE_PUSH_HOOK` raw string, then
/// checked against the manual-install copy in `contrib/`. Testing the string
/// the CLI writes — rather than a copy kept beside the test — is the point.
fn hook_source() -> String {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = fs::read_to_string(manifest.join("src/main.rs")).expect("read src/main.rs");
    let marker = "const PRE_PUSH_HOOK: &str = r#\"";
    let start = source.find(marker).expect("PRE_PUSH_HOOK const") + marker.len();
    let rest = &source[start..];
    let end = rest.find("\n\"#;").expect("PRE_PUSH_HOOK terminator");
    let hook = format!("{}\n", &rest[..end]);
    let contrib =
        fs::read_to_string(manifest.join("../../contrib/pre-push")).expect("read contrib/pre-push");
    assert_eq!(
        hook, contrib,
        "contrib/pre-push must mirror PRE_PUSH_HOOK byte for byte"
    );
    hook
}

/// Fake engine CLI whose engine is not running: it records the invocation and
/// fails the way the real CLI does — with the machine-readable marker on
/// stderr that the hook must treat as "gate skipped", not "CI failed".
const FAKE_NO_ENGINE: &str = r#"#!/usr/bin/env bash
printf 'fake-preloop %s\n' "$*" >> "__LOG__"
case "${1:-}" in
  run)
    echo "Run 11111111-1111-1111-1111-111111111111 created"
    echo "error: preloop engine unreachable (PRELOOP_UNREACHABLE)" >&2
    exit 1 ;;
  status)
    echo "error: preloop engine unreachable (PRELOOP_UNREACHABLE)" >&2
    exit 1 ;;
  *) exit 0 ;;
esac
"#;

/// Fake engine CLI that runs CI and reports a red verdict.
const FAKE_RED_RUN: &str = r#"#!/usr/bin/env bash
printf 'fake-preloop %s\n' "$*" >> "__LOG__"
case "${1:-}" in
  run)
    echo "Run 11111111-1111-1111-1111-111111111111 created"
    echo "Tests failed"
    exit 1 ;;
  *) exit 0 ;;
esac
"#;

/// Fake engine CLI that answers `status <id>` with a cached red verdict.
const FAKE_RED_STATUS: &str = r#"#!/usr/bin/env bash
printf 'fake-preloop %s\n' "$*" >> "__LOG__"
case "${1:-}" in
  status) echo "failure" ;;
  *) exit 0 ;;
esac
"#;

struct FakeCli(&'static str);

struct Sandbox {
    _temp: tempfile::TempDir,
    root: PathBuf,
    bin: PathBuf,
    home: PathBuf,
    hook: PathBuf,
}

impl Sandbox {
    fn new() -> Sandbox {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().to_path_buf();
        let bin = root.join("bin");
        let home = root.join("home");
        fs::create_dir_all(&bin).expect("create bin");
        fs::create_dir_all(&home).expect("create home");
        let hook = root.join("pre-push");
        fs::write(&hook, hook_source()).expect("write hook");
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).expect("chmod hook");
        Sandbox {
            _temp: temp,
            root,
            bin,
            home,
            hook,
        }
    }

    fn fake_log(&self) -> String {
        fs::read_to_string(self.root.join("fake.log")).unwrap_or_default()
    }

    fn write_fake_cli(&self, cli: FakeCli) {
        let script = cli
            .0
            .replace("__LOG__", &self.root.join("fake.log").display().to_string());
        let path = self.bin.join("preloop");
        fs::write(&path, script).expect("write fake cli");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod fake cli");
    }

    /// Scratch repository with a bare `origin`, the hook installed, HEAD on a
    /// fresh `feature` branch, and the given workflows under
    /// `.github/workflows/`.
    fn repo(&self, name: &str, workflows: &[&str]) -> Repo {
        let dir = self.root.join(name);
        let remote = self.root.join(format!("{name}.git"));
        let workflows_dir = dir.join(".github/workflows");
        fs::create_dir_all(&workflows_dir).expect("create workflows dir");
        for workflow in workflows {
            fs::write(workflows_dir.join(workflow), format!("name: {workflow}\n"))
                .expect("write workflow");
        }
        git(&dir, &["init", "-q", "-b", "main", "--object-format=sha1"]);
        git(&dir, &["config", "user.email", "gate@example.test"]);
        git(&dir, &["config", "user.name", "gate"]);
        git(&dir, &["config", "commit.gpgsign", "false"]);
        fs::write(dir.join("file.txt"), "base\n").expect("write file");
        git(&dir, &["add", "-A"]);
        // The base commit carries the label: this is the history the old
        // `git log <sha>` scan walked and then skipped every new branch.
        git(&dir, &["commit", "-q", "-m", "base commit [skip ci]"]);
        git(
            &dir,
            &["init", "-q", "--bare", remote.to_str().expect("utf-8 path")],
        );
        git(
            &dir,
            &[
                "remote",
                "add",
                "origin",
                remote.to_str().expect("utf-8 path"),
            ],
        );
        git(&dir, &["push", "-q", "origin", "main"]);
        git(&dir, &["checkout", "-q", "-b", "feature"]);
        let hooks = dir.join(".git/hooks");
        git(
            &dir,
            &[
                "config",
                "core.hooksPath",
                hooks.to_str().expect("utf-8 path"),
            ],
        );
        fs::copy(&self.hook, hooks.join("pre-push")).expect("install hook");
        fs::set_permissions(hooks.join("pre-push"), fs::Permissions::from_mode(0o755))
            .expect("chmod installed hook");
        Repo { dir, remote }
    }

    /// Real `git push` through the installed hook.
    fn push(&self, repo: &Repo, cache: &str, refspec: &str, env: &[(&str, &str)]) -> Output {
        let mut cmd = Command::new("git");
        cmd.arg("push")
            .arg("origin")
            .arg(refspec)
            .current_dir(&repo.dir);
        self.apply_env(&mut cmd, cache, env);
        cmd.output().expect("git push")
    }

    /// The hook invoked directly, with pre-push stdin the caller controls.
    fn hook_direct(&self, repo: &Repo, cache: &str, env: &[(&str, &str)], stdin: &str) -> Output {
        let mut cmd = Command::new("bash");
        cmd.arg(repo.dir.join(".git/hooks/pre-push"))
            .arg("origin")
            .arg(repo.remote.to_str().expect("utf-8 path"))
            .current_dir(&repo.dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        self.apply_env(&mut cmd, cache, env);
        let mut child = cmd.spawn().expect("spawn hook");
        {
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .expect("hook stdin")
                .write_all(stdin.as_bytes())
                .expect("feed refs");
        }
        child.wait_with_output().expect("hook output")
    }

    fn apply_env(&self, cmd: &mut Command, cache: &str, env: &[(&str, &str)]) {
        let path = format!(
            "{}:{}",
            self.bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        cmd.env("HOME", &self.home)
            .env("XDG_CACHE_HOME", self.root.join(cache))
            .env("PATH", path)
            .env("PRELOOP_BIN", "preloop");
        for (key, value) in env {
            cmd.env(key, value);
        }
    }

    /// The cache filename the hook derives for (remote, endpoint, commit).
    fn seed_cache(&self, repo: &Repo, cache: &str, endpoint: &str, run_id: &str) {
        let sha = rev_parse(&repo.dir, "HEAD");
        let mut child = Command::new("git")
            .arg("hash-object")
            .arg("--stdin")
            .current_dir(&repo.dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("git hash-object");
        {
            use std::io::Write;
            let remote = repo.remote.display().to_string();
            child
                .stdin
                .as_mut()
                .expect("hash-object stdin")
                .write_all(format!("{remote}\n{endpoint}\n{sha}\n").as_bytes())
                .expect("feed cache key");
        }
        let out = child.wait_with_output().expect("hash-object output");
        let key = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        let dir = self.root.join(cache).join("preloop-push");
        fs::create_dir_all(&dir).expect("create cache dir");
        let remote = repo.remote.display().to_string();
        fs::write(dir.join(key), format!("{run_id}\t{remote}\t{endpoint}\n")).expect("seed cache");
    }

    /// A push must never leave engine state behind: no pid file, no process.
    fn assert_no_engine_artifacts(&self) {
        let pid_files: Vec<_> = walk(&self.root)
            .into_iter()
            .filter(|path| path.file_name().is_some_and(|name| name == "preloop.pid"))
            .collect();
        assert!(
            pid_files.is_empty(),
            "hook run left engine state behind: {pid_files:?}"
        );
    }
}

struct Repo {
    dir: PathBuf,
    remote: PathBuf,
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(walk(&path));
        } else {
            found.push(path);
        }
    }
    found
}

fn git(dir: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap_or_else(|error| panic!("run git {args:?}: {error}"));
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn rev_parse(dir: &Path, rev: &str) -> String {
    let output = Command::new("git")
        .args(["rev-parse", rev])
        .current_dir(dir)
        .output()
        .expect("git rev-parse");
    assert!(output.status.success(), "git rev-parse {rev}");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn has_ref(dir: &Path, reference: &str) -> bool {
    Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", reference])
        .current_dir(dir)
        .output()
        .expect("git rev-parse")
        .status
        .success()
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_contains(haystack: &str, needle: &str) {
    assert!(
        haystack.contains(needle),
        "missing {needle:?} in:\n{haystack}"
    );
}

#[test]
fn skip_ci_in_the_ancestry_does_not_skip_a_new_branch() {
    let sandbox = Sandbox::new();
    sandbox.write_fake_cli(FakeCli(FAKE_NO_ENGINE));
    let repo = sandbox.repo("ancestor", &["ci.yml"]);
    // The branch's own commit carries no label; main's base commit does.
    git(
        &repo.dir,
        &["commit", "-q", "--allow-empty", "-m", "feature work"],
    );

    let output = sandbox.push(
        &repo,
        "cache",
        "feature",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
    );
    let out = text(&output);

    assert!(
        output.status.success(),
        "engine-down push must proceed: {out}"
    );
    assert_contains(&out, "running CI on preloop before pushing feature");
    assert!(
        !out.contains("[skip ci] found"),
        "an ancestor's label must not skip the push:\n{out}"
    );
    // The gate path ran, with the repository's single workflow chosen for it.
    assert_contains(&sandbox.fake_log(), "run -f .github/workflows/ci.yml");
    // And the push itself landed on the remote.
    assert_eq!(
        rev_parse(&repo.remote, "refs/heads/feature"),
        rev_parse(&repo.dir, "HEAD")
    );
    sandbox.assert_no_engine_artifacts();
}

#[test]
fn skip_ci_in_any_pushed_commit_skips_the_gate() {
    let sandbox = Sandbox::new();
    sandbox.write_fake_cli(FakeCli(FAKE_NO_ENGINE));
    let repo = sandbox.repo("skips", &["ci.yml"]);
    git(
        &repo.dir,
        &["commit", "-q", "--allow-empty", "-m", "clean commit"],
    );
    sandbox.push(
        &repo,
        "cache",
        "feature",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
    );

    // A non-tip commit in the pushed batch carries the label: the engine
    // suppresses the run for the whole push, and so does the hook.
    git(
        &repo.dir,
        &[
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "mid commit [skip ci]",
        ],
    );
    git(
        &repo.dir,
        &["commit", "-q", "--allow-empty", "-m", "tip commit"],
    );
    let before = sandbox.fake_log();
    let output = sandbox.push(
        &repo,
        "cache",
        "feature",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
    );
    let out = text(&output);

    assert!(output.status.success(), "{out}");
    assert_contains(&out, "[skip ci] found");
    assert_eq!(before, sandbox.fake_log(), "no CI may be invoked");

    // And a tip label skips just the same.
    git(
        &repo.dir,
        &["commit", "-q", "--allow-empty", "-m", "ship [skip ci]"],
    );
    let before = sandbox.fake_log();
    let output = sandbox.push(
        &repo,
        "cache",
        "feature",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
    );
    assert!(output.status.success(), "{}", text(&output));
    assert_eq!(before, sandbox.fake_log(), "no CI may be invoked");
    sandbox.assert_no_engine_artifacts();
}

#[test]
fn skip_ci_buried_in_main_history_does_not_skip_a_main_push() {
    let sandbox = Sandbox::new();
    sandbox.write_fake_cli(FakeCli(FAKE_NO_ENGINE));
    let repo = sandbox.repo("main-push", &["ci.yml"]);
    git(&repo.dir, &["checkout", "-q", "main"]);
    git(
        &repo.dir,
        &["commit", "-q", "--allow-empty", "-m", "normal work"],
    );

    let output = sandbox.push(
        &repo,
        "cache",
        "main",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
    );
    let out = text(&output);

    assert!(output.status.success(), "{out}");
    assert!(
        !out.contains("[skip ci] found"),
        "history alone must not skip the push:\n{out}"
    );
    assert_contains(&sandbox.fake_log(), "run -f .github/workflows/ci.yml");
    sandbox.assert_no_engine_artifacts();
}

#[test]
fn the_workflow_is_always_selected_explicitly() {
    let sandbox = Sandbox::new();
    sandbox.write_fake_cli(FakeCli(FAKE_NO_ENGINE));

    // Two workflows and no override: skip with a message instead of letting
    // `preloop run` fail with "specify a workflow with -f".
    let ambiguous = sandbox.repo("ambiguous", &["ci.yml", "release.yml"]);
    git(
        &ambiguous.dir,
        &["commit", "-q", "--allow-empty", "-m", "feature work"],
    );
    let output = sandbox.push(
        &ambiguous,
        "cache-ambiguous",
        "feature",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
    );
    let out = text(&output);
    assert!(output.status.success(), "{out}");
    assert_contains(&out, "holds 2 workflows");
    assert_contains(&out, "PRELOOP_PREPUSH_WORKFLOW");
    assert_contains(&out, "CI gate skipped for this push");
    assert!(
        sandbox.fake_log().is_empty(),
        "an un-evaluable gate must not invoke CI: {}",
        sandbox.fake_log()
    );

    // The override names both the workflow and the job.
    git(
        &ambiguous.dir,
        &["commit", "-q", "--allow-empty", "-m", "second try"],
    );
    let output = sandbox.push(
        &ambiguous,
        "cache-override",
        "feature",
        &[
            ("PRELOOP_URL", "http://127.0.0.1:9"),
            ("PRELOOP_PREPUSH_WORKFLOW", "release.yml"),
            ("PRELOOP_PREPUSH_JOB", "build"),
        ],
    );
    assert!(output.status.success(), "{}", text(&output));
    assert_contains(&sandbox.fake_log(), "run -f release.yml --job build");

    // A single workflow needs no configuration, but is still passed as `-f`.
    let single = sandbox.repo("single", &["only.yml"]);
    git(
        &single.dir,
        &["commit", "-q", "--allow-empty", "-m", "feature work"],
    );
    let output = sandbox.push(
        &single,
        "cache-single",
        "feature",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
    );
    assert!(output.status.success(), "{}", text(&output));
    assert_contains(&sandbox.fake_log(), "run -f .github/workflows/only.yml");
    sandbox.assert_no_engine_artifacts();
}

#[test]
fn a_missing_engine_skips_the_gate_and_says_so() {
    let sandbox = Sandbox::new();
    sandbox.write_fake_cli(FakeCli(FAKE_NO_ENGINE));
    let repo = sandbox.repo("no-engine", &["ci.yml"]);
    git(
        &repo.dir,
        &["commit", "-q", "--allow-empty", "-m", "feature work"],
    );

    let output = sandbox.push(
        &repo,
        "cache",
        "feature",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
    );
    let out = text(&output);

    assert!(
        output.status.success(),
        "a missing engine must not read as CI failure: {out}"
    );
    assert_contains(&out, "no preloop engine is running at http://127.0.0.1:9");
    assert_contains(&out, "CI gate skipped for this push");
    assert_contains(&out, "preloop serve");
    assert!(
        !out.contains("push aborted"),
        "engine-down is a skip, not an abort:\n{out}"
    );
    sandbox.assert_no_engine_artifacts();
}

#[test]
fn a_cached_run_without_an_engine_skips_the_gate() {
    let sandbox = Sandbox::new();
    sandbox.write_fake_cli(FakeCli(FAKE_NO_ENGINE));
    let repo = sandbox.repo("cached", &["ci.yml"]);
    git(
        &repo.dir,
        &["commit", "-q", "--allow-empty", "-m", "feature work"],
    );
    let sha = rev_parse(&repo.dir, "HEAD");
    sandbox.seed_cache(&repo, "cache", "http://127.0.0.1:9", "run-1");

    let stdin = format!("refs/heads/feature {sha} refs/heads/feature {ZERO_SHA}\n");
    let output = sandbox.hook_direct(
        &repo,
        "cache",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
        &stdin,
    );
    let out = text(&output);
    let calls = sandbox.fake_log();

    assert!(output.status.success(), "{out}");
    assert_contains(&out, "no preloop engine is running");
    assert_contains(&out, "CI gate skipped");
    assert_contains(&calls, "status run-1");
    assert!(
        !calls.contains("run -f"),
        "a recorded run must not be re-created when the engine is down: {calls}"
    );
    sandbox.assert_no_engine_artifacts();
}

#[test]
fn a_red_verdict_aborts_the_push() {
    let sandbox = Sandbox::new();
    sandbox.write_fake_cli(FakeCli(FAKE_RED_RUN));
    let repo = sandbox.repo("red", &["ci.yml"]);
    git(
        &repo.dir,
        &["commit", "-q", "--allow-empty", "-m", "feature work"],
    );

    let output = sandbox.push(
        &repo,
        "cache",
        "feature",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
    );
    let out = text(&output);

    assert!(
        !output.status.success(),
        "red CI must reject the push: {out}"
    );
    assert_contains(&out, "CI failed for feature — push aborted");
    assert_contains(&out, "git push --no-verify");
    assert!(
        !has_ref(&repo.remote, "refs/heads/feature"),
        "the rejected commit must not reach the remote"
    );
    sandbox.assert_no_engine_artifacts();
}

#[test]
fn a_cached_red_verdict_aborts_the_push() {
    let sandbox = Sandbox::new();
    sandbox.write_fake_cli(FakeCli(FAKE_RED_STATUS));
    let repo = sandbox.repo("cached-red", &["ci.yml"]);
    git(
        &repo.dir,
        &["commit", "-q", "--allow-empty", "-m", "feature work"],
    );
    let sha = rev_parse(&repo.dir, "HEAD");
    sandbox.seed_cache(&repo, "cache", "http://127.0.0.1:9", "run-2");

    let stdin = format!("refs/heads/feature {sha} refs/heads/feature {ZERO_SHA}\n");
    let output = sandbox.hook_direct(
        &repo,
        "cache",
        &[("PRELOOP_URL", "http://127.0.0.1:9")],
        &stdin,
    );
    let out = text(&output);

    assert!(!output.status.success(), "{out}");
    assert_contains(&out, "cached CI verdict");
    sandbox.assert_no_engine_artifacts();
}
