//! Contract tests for the AgentENV provider against a fake `aenv`.
//!
//! The provider's whole job is translating [`VmProvider`] calls into `aenv`
//! invocations, so the argv it emits *is* the observable behaviour: a wrong
//! flag here is a job that never boots on a real host. The fake records every
//! invocation, answers the handful of commands the provider parses, and lets
//! each test assert both what was asked and what the provider concluded.
//!
//! Every command string asserted here was verified against AgentENV 0.2.0 on a
//! Linux/KVM host (see `docs/control-plane/architecture/sandboxes.mdx`).

#![cfg(unix)]

use preloop_vm::agentenv::AgentEnvProvider;
use preloop_vm::{
    MachineName, MachineSpec, MachineState, NetworkPolicy, SocketMount, VmError, VmProvider,
    VolumeMount,
};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Write `contents` to `path` as an executable, without this process ever
/// holding a write descriptor.
///
/// Concurrent tests in one binary otherwise hit ETXTBSY: a descriptor open in
/// this process is inherited by every other test's `fork` until that child
/// reaches `exec`. Same reasoning as `tests/provider.rs`.
fn write_executable(path: &Path, contents: &str) {
    use std::io::Write;

    let staged = path.with_extension("staged");
    let mut child = std::process::Command::new("sh")
        .args(["-c", r#"cat > "$1" && chmod 755 "$1""#, "sh"])
        .arg(&staged)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(contents.as_bytes())
        .unwrap();
    assert!(child.wait().unwrap().success());
    fs::rename(staged, path).unwrap();
}

/// A fake `aenv` that appends every invocation to `<exe>.calls` (one argv per
/// line, NUL-joined arguments) and mimics the real CLI's output shapes.
///
/// Sandboxes and volumes are real rows in side files, so a test sees what the
/// provider created, mounted and deleted:
///
/// * `<exe>.state` selects what `ls` reports for every live sandbox: running,
///   paused, or `gone` (none listed, as after a TTL expiry).
/// * Each `start` mints the next `sb-NNNN` id. `--volume` must name an existing
///   volume and is recorded as a mount until the sandbox is deleted.
/// * `volume create` refuses a taken name (as the real server does) and a
///   missing `--from-volume`. `volume delete` refuses a mounted volume.
/// * `<exe>.volume-status` overrides the status `volume inspect` reports.
/// * `<exe>.fail-volume-create` makes every `volume create` fail.
/// * `<exe>.fail-sudo` and `<exe>.fail-payload` behave as before.
fn fake_aenv() -> (TempDir, PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("aenv");
    write_executable(
        &executable,
        r##"#!/bin/sh
set -eu
calls="$0.calls"
line=""
for arg in "$@"; do
  if [ -z "$line" ]; then line="$arg"; else line="$line|$arg"; fi
done
printf '%s\n' "$line" >> "$calls"

state=running
[ -f "$0.state" ] && state=$(cat "$0.state")
sandboxes="$0.sandboxes"
mounts="$0.mounts"
volumes="$0.volumes"
touch "$sandboxes" "$mounts" "$volumes"

case "${1-}" in
  start)
    # Concurrent starts are real in some tests: serialize the id counter and the
    # mount table with a lock directory.
    while ! mkdir "$0.lock" 2>/dev/null; do sleep 0.01; done
    count=0
    [ -f "$0.starts" ] && count=$(cat "$0.starts")
    count=$((count + 1))
    id=$(printf 'sb-%04d' "$count")
    shift
    previous=""
    refused=""
    for arg in "$@"; do
      if [ "$previous" = "--volume" ]; then
        volume=${arg#*=}
        grep -q "^$volume|" "$volumes" || refused="$volume"
      fi
      previous="$arg"
    done
    if [ -n "$refused" ]; then
      rmdir "$0.lock"
      printf 'volume %s not found\n' "$refused" >&2
      exit 1
    fi
    previous=""
    for arg in "$@"; do
      if [ "$previous" = "--volume" ]; then
        printf '%s|%s|%s\n' "$id" "${arg%%=*}" "${arg#*=}" >> "$mounts"
      fi
      previous="$arg"
    done
    printf '%s' "$count" > "$0.starts"
    printf '%s\n' "$id" >> "$sandboxes"
    rmdir "$0.lock"
    # `aenv start ... -d` prints only the sandbox id.
    printf '%s\n' "$id"
    ;;
  ls)
    if [ "$state" = gone ]; then
      printf '[]\n'
    else
      first=1
      printf '['
      while IFS= read -r id; do
        [ -n "$id" ] || continue
        [ $first -eq 1 ] || printf ','
        first=0
        printf '{"sandboxID":"%s","templateID":"t","alias":null,"state":"%s","cpuCount":4,"memoryMB":4096,"diskSizeMB":65536,"startedAt":"now","endAt":"later"}' "$id" "$state"
      done < "$sandboxes"
      printf ']\n'
    fi
    ;;
  snapshot)
    if [ -f "$0.fail-snapshot" ]; then
      printf 'snapshot refused\n' >&2
      exit 1
    fi
    printf 'Created snapshot snap-9999\n'
    ;;
  exec)
    # Guest probes and provisioning shell-outs all succeed. When the test
    # plants `<exe>.fail-payload`, the secret wrapper's payload fails instead
    # (its cleanup and every probe still succeed). `<exe>.fail-sudo` makes
    # every root step refuse sudo, as a guest without passwordless sudo would.
    last=""
    for arg in "$@"; do last="$arg"; done
    if [ -f "$0.fail-sudo" ]; then
      case "$last" in
        *"sudo -n sh"*) printf 'sudo: a password is required\n' >&2; exit 1 ;;
      esac
    fi
    if [ -f "$0.fail-payload" ]; then
      case "$last" in
        "set -eu; SECRETS="*) exit 3 ;;
      esac
    fi
    exit 0
    ;;
  upload)
    if [ -f "$0.fail-upload" ]; then
      printf 'upload refused\n' >&2
      exit 1
    fi
    printf 'Uploaded\n'
    ;;
  download)
    printf 'Downloaded\n'
    ;;
  pause|resume|timeout)
    printf 'ok\n'
    ;;
  delete)
    id=${2-}
    if ! grep -qx "$id" "$sandboxes"; then
      printf 'sandbox %s not found\n' "$id" >&2
      exit 1
    fi
    grep -vx "$id" "$sandboxes" > "$sandboxes.tmp" || true
    mv "$sandboxes.tmp" "$sandboxes"
    grep -v "^$id|" "$mounts" > "$mounts.tmp" || true
    mv "$mounts.tmp" "$mounts"
    printf 'ok\n'
    ;;
  volume)
    action=${2-}
    case "$action" in
      create)
        name=${3-}
        shift 3
        mode=exclusive
        from=""
        while [ $# -gt 0 ]; do
          case "$1" in
            --mode) mode=$2; shift 2 ;;
            --from-volume) from=$2; shift 2 ;;
            *) shift ;;
          esac
        done
        if [ -f "$0.fail-volume-create" ]; then
          printf 'backend refused volume %s\n' "$name" >&2
          exit 1
        fi
        # Overlap probe: with `<exe>.overlap-probe`, a create holds a marker
        # directory for a moment and records it when a second create is inside
        # it at the same time. One global populate lock can never overlap; a
        # per-tree lock does, which is how a test tells them apart without
        # measuring wall time or waiting for the other side.
        if [ -f "$0.overlap-probe" ]; then
          if ! mkdir "$0.in-create" 2>/dev/null; then
            : > "$0.overlap"
          fi
          sleep 1
          rmdir "$0.in-create" 2>/dev/null || true
        fi
        if grep -q "^$name|" "$volumes"; then
          printf 'volume %s already exists\n' "$name" >&2
          exit 1
        fi
        if [ -n "$from" ] && ! grep -q "^$from|" "$volumes"; then
          printf 'volume %s not found\n' "$from" >&2
          exit 1
        fi
        printf '%s|%s|%s\n' "$name" "$mode" "$from" >> "$volumes"
        printf 'Created volume %s\n' "$name"
        ;;
      inspect)
        name=${3-}
        if ! grep -q "^$name|" "$volumes"; then
          printf 'volume %s not found\n' "$name" >&2
          exit 1
        fi
        mode=$(grep "^$name|" "$volumes" | head -n 1 | cut -d'|' -f2)
        status=ready
        [ -f "$0.volume-status" ] && status=$(cat "$0.volume-status")
        printf '{"name":"%s","mode":"%s","status":"%s"}\n' "$name" "$mode" "$status"
        ;;
      delete)
        name=${3-}
        if ! grep -q "^$name|" "$volumes"; then
          printf 'volume %s not found\n' "$name" >&2
          exit 1
        fi
        if grep -q "|$name\$" "$mounts"; then
          printf 'volume %s is mounted by a sandbox and cannot be deleted\n' "$name" >&2
          exit 1
        fi
        grep -v "^$name|" "$volumes" > "$volumes.tmp" || true
        mv "$volumes.tmp" "$volumes"
        printf 'Deleted volume %s\n' "$name"
        ;;
      list)
        first=1
        printf '['
        while IFS='|' read -r name mode from; do
          [ -n "$name" ] || continue
          [ $first -eq 1 ] || printf ','
          first=0
          printf '{"name":"%s","mode":"%s","status":"ready"}' "$name" "$mode"
        done < "$volumes"
        printf ']\n'
        ;;
      *)
        printf 'unexpected volume command: %s\n' "$action" >&2
        exit 64
        ;;
    esac
    ;;
  *)
    printf 'unexpected command: %s\n' "${1-}" >&2
    exit 64
    ;;
esac
"##,
    );
    (directory, executable)
}

/// Every recorded invocation, arguments joined by `|`.
fn calls(executable: &Path) -> Vec<String> {
    fs::read_to_string(executable.with_extension("calls"))
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

/// The recorded invocations whose first argument is `verb`.
fn calls_for(executable: &Path, verb: &str) -> Vec<String> {
    calls(executable)
        .into_iter()
        .filter(|line| line.split('|').next() == Some(verb))
        .collect()
}

/// The guest scripts a root-or-sudo wrapper carries, decoded.
///
/// `run_as_root_or_sudo_strict` embeds each script base64-encoded so it
/// survives both shells, so the contract is the decoded text.
fn embedded_guest_scripts(call: &str) -> Vec<String> {
    use base64::Engine as _;

    const MARKER: &str = "printf %s '";
    call.match_indices(MARKER)
        .filter_map(|(start, _)| {
            let rest = &call[start + MARKER.len()..];
            let encoded = &rest[..rest.find('\'')?];
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()?;
            String::from_utf8(bytes).ok()
        })
        .collect()
}

/// Every recorded `exec` whose decoded guest script contains `needle`.
fn exec_calls_with_script(executable: &Path, needle: &str) -> Vec<String> {
    calls_for(executable, "exec")
        .into_iter()
        .filter(|call| {
            embedded_guest_scripts(call)
                .iter()
                .any(|script| script.contains(needle))
        })
        .collect()
}

fn set_state(executable: &Path, state: &str) {
    fs::write(executable.with_extension("state"), state).unwrap();
}

/// The version pins test volumes are archived with.
fn bundle_manifest() -> preloop_vm::agentenv::volume::BundleManifest {
    preloop_vm::agentenv::volume::BundleManifest::new("2.336.0", "20.20.2", "24.19.0")
}

fn provider(executable: &Path, registry: &Path) -> AgentEnvProvider {
    AgentEnvProvider::new(executable)
        .with_registry_path(registry.join("machines.json"))
        .with_volume_index_path(registry.join("volumes.json"))
        .with_bundle_manifest(bundle_manifest())
        .with_ttl_secs(600)
}

fn spec(name: &str, image: &str) -> MachineSpec {
    MachineSpec {
        name: MachineName::new(name).unwrap(),
        image: image.to_owned(),
        cpus: 4,
        memory_mib: 4096,
        storage_gib: 20,
        overlay_gib: None,
        network: NetworkPolicy::Unrestricted,
        volumes: Vec::new(),
        sockets: Vec::new(),
        dns: None,
        rosetta: false,
    }
}

#[tokio::test]
async fn create_records_the_machine_without_touching_the_server() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-0").unwrap();

    provider
        .create(&spec("runner-0", "ubuntu:24.04"))
        .await
        .unwrap();

    // AgentENV has no defined-but-unstarted sandbox, so `create` must not
    // boot one: paying for a VM here would double the pool's create+start.
    assert!(
        calls(&executable).is_empty(),
        "create invoked aenv: {:?}",
        calls(&executable)
    );
    assert_eq!(provider.status(&name).await.unwrap(), MachineState::Stopped);
    assert_eq!(provider.list().await.unwrap(), vec![name]);
}

#[tokio::test]
async fn start_cold_boots_with_the_specs_resources_and_a_detached_ttl() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-1").unwrap();

    provider
        .create(&spec("runner-1", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();

    assert_eq!(
        calls_for(&executable, "start"),
        vec![
            "start|--cold|ubuntu:24.04|--cpu|4|--memory|4096|--disk-size-mb|20480|--timeout|600|-d"
                .to_owned()
        ]
    );
    assert_eq!(provider.status(&name).await.unwrap(), MachineState::Running);
}

#[tokio::test]
async fn a_paused_sandbox_is_resumed_instead_of_replaced() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-2").unwrap();

    provider
        .create(&spec("runner-2", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();
    set_state(&executable, "paused");
    assert_eq!(provider.status(&name).await.unwrap(), MachineState::Stopped);

    set_state(&executable, "paused");
    provider.start(&name).await.unwrap();

    // Exactly one cold boot, and the second start resumed the same sandbox:
    // re-creating it would discard the job's filesystem.
    assert_eq!(calls_for(&executable, "start").len(), 1);
    assert_eq!(
        calls_for(&executable, "resume"),
        vec!["resume|sb-0001|--timeout|600".to_owned()]
    );
}

#[tokio::test]
async fn stop_pauses_and_delete_removes_the_sandbox() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-3").unwrap();

    provider
        .create(&spec("runner-3", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();
    provider.stop(&name).await.unwrap();
    provider.delete(&name).await.unwrap();

    assert_eq!(calls_for(&executable, "pause"), vec!["pause|sb-0001"]);
    assert_eq!(calls_for(&executable, "delete"), vec!["delete|sb-0001"]);
    // Deleting drops the name, so the pool's stale sweep cannot see it again.
    assert!(provider.list().await.unwrap().is_empty());
    assert_eq!(provider.status(&name).await.unwrap(), MachineState::Missing);
}
/// Debug suspension leans on AgentENV's snapshot pause: a resumed sandbox
/// must be the same live VM the worker was blocked in, or the debug session
/// would come back without its workspace state.
#[test]
fn suspension_is_runtime_preserving() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    assert!(provider.capabilities().preserves_runtime_state_on_suspend);
}

#[tokio::test]
async fn fork_snapshots_the_golden_once_and_starts_each_clone_from_it() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let golden = MachineName::new("golden").unwrap();

    provider
        .create(&spec("golden", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start_forkable(&golden).await.unwrap();
    for clone in ["clone-a", "clone-b", "clone-c"] {
        provider
            .fork(&golden, &MachineName::new(clone).unwrap())
            .await
            .unwrap();
    }

    // One snapshot serves every clone: AgentENV snapshots are immutable and
    // re-forkable, unlike SmolVM's single retained checkpoint.
    let snapshots = calls_for(&executable, "snapshot");
    assert_eq!(snapshots.len(), 1, "snapshots: {snapshots:?}");
    let snapshot_name = snapshots[0]
        .split('|')
        .next_back()
        .expect("snapshot name argument")
        .to_owned();
    assert!(
        snapshot_name.starts_with("golden-snap-"),
        "unexpected snapshot name {snapshot_name}"
    );
    assert_eq!(
        snapshots[0],
        format!("snapshot|create|sb-0001|--name|{snapshot_name}")
    );

    // The golden's cold boot plus one artifact start per clone.
    let starts = calls_for(&executable, "start");
    assert_eq!(starts.len(), 4, "starts: {starts:?}");
    for start in &starts[1..] {
        assert_eq!(
            start,
            &format!("start|{snapshot_name}|--timeout|600|-d"),
            "a clone must boot from the snapshot, not the image"
        );
    }
}

#[tokio::test]
async fn a_fork_base_is_always_rearmable() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let golden = MachineName::new("golden-rearm").unwrap();

    provider
        .create(&spec("golden-rearm", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start_forkable(&golden).await.unwrap();
    provider
        .fork(&golden, &MachineName::new("clone-x").unwrap())
        .await
        .unwrap();

    // SmolVM refuses while a clone is live; AgentENV has no spent-base state,
    // so the pool must be told it can keep forking.
    assert!(
        provider
            .rearm_fork_base(&golden, Some(&MachineName::new("clone-x").unwrap()))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn a_vanished_sandbox_reconciles_to_stopped_and_restarts_on_demand() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-ttl").unwrap();

    provider
        .create(&spec("runner-ttl", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();

    // TTL expiry (or an out-of-band delete) removes the sandbox server-side.
    set_state(&executable, "gone");
    assert_eq!(provider.status(&name).await.unwrap(), MachineState::Stopped);

    set_state(&executable, "running");
    provider.start(&name).await.unwrap();
    assert_eq!(
        calls_for(&executable, "start").len(),
        2,
        "a vanished sandbox must be replaced, not resumed"
    );
    // Nothing was resumed: resuming a sandbox the server forgot would 404.
    assert!(calls_for(&executable, "resume").is_empty());
}

/// The read-only mount every runner gets for the bundle.
fn bundle_mount(host: &Path) -> VolumeMount {
    VolumeMount {
        host: host.to_path_buf(),
        guest: PathBuf::from("/opt/preloop/bin"),
        read_only: true,
    }
}

/// The volume a recorded `start` mounts at `guest`, if it mounts one there.
fn mounted_volume(start: &str, guest: &str) -> Option<String> {
    let fields: Vec<&str> = start.split('|').collect();
    let prefix = format!("{guest}=");
    fields
        .windows(2)
        .find(|pair| pair[0] == "--volume" && pair[1].starts_with(&prefix))
        .map(|pair| pair[1][prefix.len()..].to_owned())
}

/// Every recorded `volume create`, as its argv.
fn volume_creates(executable: &Path) -> Vec<String> {
    calls_for(executable, "volume")
        .into_iter()
        .filter(|call| call.starts_with("volume|create|"))
        .collect()
}

/// Whether `name` has the shape of a content-addressed runner volume name.
fn is_runner_volume(name: &str) -> bool {
    name.strip_prefix("preloop-runner-").is_some_and(|digest| {
        digest.len() == 12 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

#[tokio::test]
async fn read_only_host_trees_are_mounted_from_a_shared_volume_not_copied_into_the_runner() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-vol").unwrap();

    let mut machine = spec("runner-vol", "ubuntu:24.04");
    machine.volumes = vec![bundle_mount(bundle.path())];
    provider.create(&machine).await.unwrap();
    provider.start(&name).await.unwrap();

    // The populate sandbox (`sb-0001`) is the only one that receives an archive;
    // the runner (`sb-0002`) receives nothing and mounts the shared volume.
    let uploads = calls_for(&executable, "upload");
    assert_eq!(uploads.len(), 1, "uploads: {uploads:?}");
    assert!(uploads[0].starts_with("upload|sb-0001|"), "{uploads:?}");
    assert!(uploads[0].contains("|/tmp/preloop-volume-"), "{uploads:?}");
    let starts = calls_for(&executable, "start");
    assert_eq!(starts.len(), 2, "starts: {starts:?}");
    assert!(
        starts[0].contains("--volume|/mnt/preloop-volume=preloop-runner-stage-"),
        "the populate sandbox mounts the exclusive staging volume: {starts:?}"
    );
    let volume = mounted_volume(&starts[1], "/opt/preloop/bin")
        .expect("the runner mounts the bundle's read-only volume");
    assert!(is_runner_volume(&volume), "{volume}");

    // Populate: one exclusive staging volume, committed by deleting its sandbox
    // before it is frozen into the read-only volume the runners mount.
    let creates = volume_creates(&executable);
    assert_eq!(creates.len(), 2, "{creates:?}");
    assert!(creates[0].contains("|--mode|exclusive|"), "{creates:?}");
    assert_eq!(
        creates[1],
        format!(
            "volume|create|{volume}|--mode|ro|--from-volume|{}|--size-mb|1024",
            creates[0].split('|').nth(2).unwrap()
        ),
        "{creates:?}"
    );
    let order = calls(&executable);
    let commit = order
        .iter()
        .position(|call| call == "delete|sb-0001")
        .unwrap();
    let freeze = order
        .iter()
        .position(|call| {
            call.starts_with("volume|create|preloop-runner-") && call.contains("--mode|ro")
        })
        .unwrap();
    assert!(
        commit < freeze,
        "the staging sandbox must be deleted before the fork: {order:?}"
    );
}

#[tokio::test]
async fn a_second_runner_reuses_the_published_volume_without_archiving_again() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());

    for name in ["runner-a", "runner-b"] {
        let mut machine = spec(name, "ubuntu:24.04");
        machine.volumes = vec![bundle_mount(bundle.path())];
        provider.create(&machine).await.unwrap();
        provider
            .start(&MachineName::new(name).unwrap())
            .await
            .unwrap();
    }

    assert_eq!(
        calls_for(&executable, "upload").len(),
        1,
        "one populate, ever"
    );
    assert_eq!(
        volume_creates(&executable).len(),
        2,
        "one staging volume and one fork"
    );
    let starts = calls_for(&executable, "start");
    let first = mounted_volume(&starts[1], "/opt/preloop/bin").unwrap();
    let second = mounted_volume(&starts[2], "/opt/preloop/bin").unwrap();
    assert_eq!(first, second);
}

#[tokio::test]
async fn a_changed_tree_is_a_new_volume_and_the_old_one_is_left_for_prune() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF-v1").unwrap();

    let first = provider(&executable, home.path());
    let mut machine = spec("runner-v1", "ubuntu:24.04");
    machine.volumes = vec![bundle_mount(bundle.path())];
    first.create(&machine).await.unwrap();
    first
        .start(&MachineName::new("runner-v1").unwrap())
        .await
        .unwrap();
    let old = mounted_volume(&calls_for(&executable, "start")[1], "/opt/preloop/bin").unwrap();

    // A new engine process (a fresh provider) sees the new bundle.
    fs::write(bundle.path().join("preloop-runner"), b"ELF-v2").unwrap();
    let second = provider(&executable, home.path());
    let mut machine = spec("runner-v2", "ubuntu:24.04");
    machine.volumes = vec![bundle_mount(bundle.path())];
    second.create(&machine).await.unwrap();
    second
        .start(&MachineName::new("runner-v2").unwrap())
        .await
        .unwrap();
    let new = mounted_volume(
        calls_for(&executable, "start").last().unwrap(),
        "/opt/preloop/bin",
    )
    .unwrap();

    assert_ne!(old, new, "new content must not reuse the old volume");
    // Only staging volumes are deleted on the way; a final volume is pruned by
    // `preloop runner-volumes prune`, never as a side effect of a start.
    assert!(
        calls_for(&executable, "volume").iter().all(|call| {
            call.strip_prefix("volume|delete|")
                .is_none_or(|name| !is_runner_volume(name))
        }),
        "a final volume was deleted on start"
    );
}

#[tokio::test]
async fn a_writable_host_mount_is_refused_before_anything_runs() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());

    let mut machine = spec("runner-rw", "ubuntu:24.04");
    machine.volumes = vec![VolumeMount {
        host: bundle.path().to_path_buf(),
        guest: PathBuf::from("/run/preloop-control"),
        read_only: false,
    }];
    let error = provider
        .create(&machine)
        .await
        .expect_err("writable mounts cannot be shared");
    assert!(
        matches!(&error, VmError::InvalidSpec(message) if message.contains("writable")),
        "{error:?}"
    );
    assert!(
        calls(&executable).is_empty(),
        "nothing may run: {:?}",
        calls(&executable)
    );
}

#[tokio::test]
async fn a_failed_volume_create_is_a_named_error_and_the_tree_is_never_copied() {
    let (_directory, executable) = fake_aenv();
    fs::write(executable.with_extension("fail-volume-create"), "1").unwrap();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-nofallback").unwrap();

    let mut machine = spec("runner-nofallback", "ubuntu:24.04");
    machine.volumes = vec![bundle_mount(bundle.path())];
    provider.create(&machine).await.unwrap();
    let error = provider
        .start(&name)
        .await
        .expect_err("the volume could not be created");
    match error {
        VmError::Command {
            operation, message, ..
        } => {
            assert_eq!(operation, "runner_volume");
            assert!(message.contains("backend refused"), "{message}");
        }
        other => panic!("unexpected error {other:?}"),
    }
    assert!(
        calls_for(&executable, "upload").is_empty(),
        "no tar fallback"
    );
    assert!(
        calls_for(&executable, "start").is_empty(),
        "no sandbox may boot without its volume"
    );
}

#[tokio::test]
async fn a_failed_populate_leaves_neither_the_final_volume_nor_its_staging_copy() {
    let (_directory, executable) = fake_aenv();
    fs::write(executable.with_extension("fail-upload"), "1").unwrap();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-badunpack").unwrap();

    let mut machine = spec("runner-badunpack", "ubuntu:24.04");
    machine.volumes = vec![bundle_mount(bundle.path())];
    provider.create(&machine).await.unwrap();
    let error = provider
        .start(&name)
        .await
        .expect_err("the archive did not unpack");
    assert!(
        matches!(&error, VmError::Command { message, .. } if message.contains("upload refused")),
        "{error:?}"
    );
    assert_eq!(
        volume_creates(&executable).len(),
        1,
        "only the staging volume was created"
    );
    assert!(calls_for(&executable, "delete").contains(&"delete|sb-0001".to_owned()));
    assert!(
        calls_for(&executable, "volume")
            .iter()
            .any(|call| call.starts_with("volume|delete|preloop-runner-stage-"))
    );
}

#[tokio::test]
async fn a_volume_in_a_state_other_than_ready_is_refused_not_repopulated() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let digest = preloop_vm::agentenv::volume::archive_tree(bundle.path(), &bundle_manifest())
        .await
        .unwrap()
        .digest;
    let name = preloop_vm::agentenv::volume::volume_name(&digest);
    fs::write(
        executable.with_extension("volumes"),
        format!("{name}|ro|\n"),
    )
    .unwrap();
    fs::write(executable.with_extension("volume-status"), "uploading").unwrap();
    let provider = provider(&executable, home.path());

    let mut machine = spec("runner-uploading", "ubuntu:24.04");
    machine.volumes = vec![bundle_mount(bundle.path())];
    provider.create(&machine).await.unwrap();
    let error = provider
        .start(&MachineName::new("runner-uploading").unwrap())
        .await
        .expect_err("an uploading volume is not mountable");
    assert!(
        matches!(&error, VmError::Command { message, .. } if message.contains("uploading")),
        "{error:?}"
    );
    assert!(
        volume_creates(&executable).is_empty(),
        "the owner may still be writing it"
    );
    assert!(calls_for(&executable, "upload").is_empty());
}

#[tokio::test]
async fn concurrent_first_starts_of_one_tree_populate_it_once() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());
    for name in ["runner-p", "runner-q"] {
        let mut machine = spec(name, "ubuntu:24.04");
        machine.volumes = vec![bundle_mount(bundle.path())];
        provider.create(&machine).await.unwrap();
    }

    let first_name = MachineName::new("runner-p").unwrap();
    let second_name = MachineName::new("runner-q").unwrap();
    let (first, second) = tokio::join!(provider.start(&first_name), provider.start(&second_name),);
    first.unwrap();
    second.unwrap();
    assert_eq!(
        calls_for(&executable, "upload").len(),
        1,
        "one archive, one populate"
    );
    assert_eq!(
        volume_creates(&executable).len(),
        2,
        "one staging volume and one fork"
    );
}

#[tokio::test]
async fn a_clone_mounts_the_golden_bundle_volume_at_fork_and_uploads_nothing() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());
    let golden = MachineName::new("golden-vol").unwrap();

    let machine = spec("golden-vol", "ubuntu:24.04");
    provider.create(&machine).await.unwrap();
    provider.start_forkable(&golden).await.unwrap();
    provider
        .fork_with_volumes(
            &golden,
            &MachineName::new("clone-vol").unwrap(),
            &[bundle_mount(bundle.path())],
        )
        .await
        .unwrap();

    // A clone starts from the snapshot with the current read-only volume attached,
    // so a changed bundle reaches clones without rebuilding the golden.
    assert_eq!(calls_for(&executable, "upload").len(), 1);
    let starts = calls_for(&executable, "start");
    let clone = starts.last().unwrap();
    assert!(clone.starts_with("start|golden-vol-snap-"), "{clone}");
    assert!(mounted_volume(clone, "/opt/preloop/bin").is_some());
}

#[tokio::test]
async fn an_in_place_bundle_update_is_noticed_by_the_same_engine() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF-v1").unwrap();
    let provider = provider(&executable, home.path());

    for (name, contents) in [("runner-before", b"ELF-v1"), ("runner-after", b"ELF-v2")] {
        fs::write(bundle.path().join("preloop-runner"), contents).unwrap();
        let mut machine = spec(name, "ubuntu:24.04");
        machine.volumes = vec![bundle_mount(bundle.path())];
        provider.create(&machine).await.unwrap();
        provider
            .start(&MachineName::new(name).unwrap())
            .await
            .unwrap();
    }

    let starts = calls_for(&executable, "start");
    let before = mounted_volume(&starts[1], "/opt/preloop/bin").unwrap();
    let after = mounted_volume(&starts[3], "/opt/preloop/bin").unwrap();
    assert_ne!(
        before, after,
        "an in-place update must not keep mounting the old volume"
    );
    assert_eq!(
        calls_for(&executable, "upload").len(),
        2,
        "each version is populated once"
    );
    assert_eq!(volume_creates(&executable).len(), 4);
}

#[tokio::test]
async fn a_volume_deleted_under_a_running_engine_is_populated_again() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());

    for name in ["runner-a", "runner-b"] {
        let mut machine = spec(name, "ubuntu:24.04");
        machine.volumes = vec![bundle_mount(bundle.path())];
        provider.create(&machine).await.unwrap();
        provider
            .start(&MachineName::new(name).unwrap())
            .await
            .unwrap();
        if name == "runner-a" {
            // `preloop volumes prune` deleted the volume while the engine ran.
            fs::write(executable.with_extension("volumes"), "").unwrap();
        }
    }

    assert_eq!(
        calls_for(&executable, "upload").len(),
        2,
        "a remembered volume that is gone is populated again, not mounted dead"
    );
    let starts = calls_for(&executable, "start");
    assert!(
        starts[2].contains("--volume|/mnt/preloop-volume=preloop-runner-stage-"),
        "the second start repopulates: {starts:?}"
    );
    assert_eq!(
        mounted_volume(&starts[1], "/opt/preloop/bin"),
        mounted_volume(&starts[3], "/opt/preloop/bin"),
        "the repopulated volume has the same content address"
    );
}

#[tokio::test]
async fn a_populated_volume_records_what_it_holds_and_prune_forgets_it() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());
    let machine = MachineName::new("runner-indexed").unwrap();

    let mut spec = spec("runner-indexed", "ubuntu:24.04");
    spec.volumes = vec![bundle_mount(bundle.path())];
    provider.create(&spec).await.unwrap();
    provider.start(&machine).await.unwrap();

    let volume = mounted_volume(&calls_for(&executable, "start")[1], "/opt/preloop/bin").unwrap();
    let entry = provider
        .volume_index()
        .await
        .get(&volume)
        .cloned()
        .expect("the populated volume is indexed");
    assert_eq!(entry.manifest.runner_version(), "2.336.0");
    assert_eq!(entry.manifest.node20_version(), "20.20.2");
    assert_eq!(entry.manifest.node24_version(), "24.19.0");
    assert_eq!(entry.bundle_files, 1);
    assert!(entry.bundle_bytes > 0);
    assert_eq!(entry.host, bundle.path().display().to_string());
    assert!(entry.created > 0, "the index carries the created time");

    // The index is on disk, so a restarted engine still knows what it holds.
    let reloaded = AgentEnvProvider::new(&executable)
        .with_volume_index_path(home.path().join("volumes.json"))
        .volume_index()
        .await;
    assert_eq!(reloaded.get(&volume), Some(&entry));

    // Prune drops the entry once the volume is gone, and nothing is mounted.
    provider.delete(&machine).await.unwrap();
    let report = provider.prune_volumes(&[], false).await.unwrap();
    assert_eq!(report.deleted, vec![volume.clone()]);
    assert!(provider.volume_index().await.is_unknown(&volume));
}

#[tokio::test]
async fn a_dry_run_reports_candidates_and_a_real_run_reports_a_mounted_volume() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());
    let machine = MachineName::new("runner-mounted").unwrap();

    let mut spec = spec("runner-mounted", "ubuntu:24.04");
    spec.volumes = vec![bundle_mount(bundle.path())];
    provider.create(&spec).await.unwrap();
    provider.start(&machine).await.unwrap();
    let mounted = mounted_volume(&calls_for(&executable, "start")[1], "/opt/preloop/bin").unwrap();
    let stale = "preloop-runner-0123456789ab".to_owned();
    fs::write(
        executable.with_extension("volumes"),
        format!("{mounted}|ro|\n{stale}|ro|\n"),
    )
    .unwrap();

    // A dry run cannot ask the server which volumes a sandbox holds, so it
    // names candidates and claims nothing was, or would be, deleted.
    let dry = provider.prune_volumes(&[], true).await.unwrap();
    let mut candidates = dry.candidates.clone();
    candidates.sort();
    assert_eq!(candidates, vec![stale.clone(), mounted.clone()]);
    assert!(dry.deleted.is_empty() && dry.in_use.is_empty());
    assert!(executable.with_extension("volumes").exists());

    // The real run learns from the delete attempt: the mounted one stays.
    let real = provider.prune_volumes(&[], false).await.unwrap();
    assert_eq!(real.deleted, vec![stale]);
    assert_eq!(real.in_use, vec![mounted]);
    assert!(real.candidates.is_empty());
}

#[tokio::test]
async fn different_trees_populate_without_waiting_for_each_other() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let first_bundle = tempfile::tempdir().unwrap();
    let second_bundle = tempfile::tempdir().unwrap();
    fs::write(first_bundle.path().join("preloop-runner"), b"ELF-one").unwrap();
    fs::write(second_bundle.path().join("preloop-runner"), b"ELF-two").unwrap();
    fs::write(executable.with_extension("overlap-probe"), "").unwrap();
    let provider = provider(&executable, home.path());

    let mut names = Vec::new();
    for (index, bundle) in [&first_bundle, &second_bundle].into_iter().enumerate() {
        let name = format!("runner-par-{index}");
        let mut machine = spec(&name, "ubuntu:24.04");
        machine.volumes = vec![bundle_mount(bundle.path())];
        provider.create(&machine).await.unwrap();
        names.push(MachineName::new(&name).unwrap());
    }

    let (first, second) = tokio::join!(provider.start(&names[0]), provider.start(&names[1]));
    first.unwrap();
    second.unwrap();
    assert!(
        executable.with_extension("overlap").exists(),
        "two creates must be inside the probe at once: a per-tree lock lets \
         different trees populate together, one global lock never would"
    );
    assert_eq!(calls_for(&executable, "upload").len(), 2);
}

#[tokio::test]
async fn socket_mounts_are_refused_before_anything_is_started() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());

    let mut machine = spec("runner-sock", "ubuntu:24.04");
    machine.sockets = vec![SocketMount {
        host: PathBuf::from("/run/preloop/engine.sock"),
        guest: PathBuf::from("/run/preloop-control/engine.sock"),
    }];
    let error = provider.create(&machine).await.unwrap_err();

    assert!(
        matches!(&error, VmError::InvalidSpec(message) if message.contains("control_socket")),
        "the error must name the remedy: {error}"
    );
    assert!(calls(&executable).is_empty());
}

#[tokio::test]
async fn exec_rejects_guest_failures_and_keeps_streams_in_error() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    // A fake that echoes to both streams and exits non-zero, so the provider's
    // handling of a *failing guest command* is observable: it is a command
    // error, not a successful result that callers can accidentally ignore.
    write_executable(
        &executable,
        r##"#!/bin/sh
set -eu
case "${1-}" in
  start) printf 'sb-0001\n' ;;
  exec)
    shift 2
    if [ "${1-}" = "--" ]; then shift; fi
    if [ "${1-}" != "/bin/report" ]; then exit 0; fi
    printf 'to-stdout\n'
    printf 'to-stderr\n' >&2
    exit 9
    ;;
  ls) printf '[{"sandboxID":"sb-0001","state":"running"}]\n' ;;
  *) exit 0 ;;
esac
"##,
    );
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-exec").unwrap();
    provider
        .create(&spec("runner-exec", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();

    let error = provider
        .exec(&name, &["/bin/report".to_owned()])
        .await
        .unwrap_err();
    assert!(matches!(
        &error,
        VmError::Command {
            operation: "exec",
            exit_code: 9,
            message,
        } if message.contains("to-stderr")
    ));
}

#[tokio::test]
async fn secret_env_values_never_appear_in_the_guest_argv() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-secret").unwrap();
    provider
        .create(&spec("runner-secret", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();

    let secret_file = home.path().join("token");
    fs::write(&secret_file, "s3cr3t-token\n").unwrap();
    provider
        .exec_with_secret_env(
            &name,
            &[
                "/opt/preloop/bin/preloop-runner".to_owned(),
                "run".to_owned(),
            ],
            &[(
                "PRELOOP_SYSTEM_TOKEN".to_owned(),
                preloop_vm::SecretSource::HostFile(secret_file),
            )],
        )
        .await
        .unwrap();

    let recorded = calls(&executable).join("\n");
    assert!(
        !recorded.contains("s3cr3t-token"),
        "the secret leaked into an aenv invocation:\n{recorded}"
    );
    // It travelled as an uploaded file. The wrapper reads and unlinks it through
    // the root-or-sudo step, then runs the payload as the exec user.
    let wrapper = calls_for(&executable, "exec")
        .into_iter()
        .find(|call| call.contains("SECRETS=$("))
        .expect("the payload must still run through the secret wrapper");
    assert!(wrapper.contains("eval \"$SECRETS\""), "{wrapper}");
    assert!(
        wrapper.contains("exec '/opt/preloop/bin/preloop-runner' 'run'"),
        "{wrapper}"
    );
    // The privileged part is exactly the read and the unlink of that file, and
    // the payload is not inside either of them.
    let privileged = embedded_guest_scripts(&wrapper);
    assert_eq!(privileged.len(), 2, "{privileged:?}");
    assert!(
        privileged[0].starts_with("cat '/run/preloop-vm-secrets/"),
        "{privileged:?}"
    );
    assert!(
        privileged[1].starts_with("rm -f '/run/preloop-vm-secrets/"),
        "{privileged:?}"
    );
    assert!(
        privileged
            .iter()
            .all(|script| !script.contains("preloop-runner")),
        "the payload must stay with the exec user: {privileged:?}"
    );
}

/// A payload that exits non-zero must surface as `Err`, exactly as it does
/// from `SmolVmProvider`: the orchestrator's `configure` call relies on the
/// error to fail provisioning instead of launching a runner that never
/// registered. The wrapper-sourced secret file must also be cleaned up when
/// the wrapper did not get to unlink it.
#[tokio::test]
async fn secret_env_payload_exit_code_propagates_as_an_error() {
    let (_directory, executable) = fake_aenv();
    fs::write(executable.with_extension("fail-payload"), "1").unwrap();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-fail").unwrap();
    provider
        .create(&spec("runner-fail", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();

    let secret_file = home.path().join("token");
    fs::write(&secret_file, "s3cr3t-token\n").unwrap();
    let result = provider
        .exec_with_secret_env(
            &name,
            &["/bin/true".to_owned()],
            &[(
                "PRELOOP_SYSTEM_TOKEN".to_owned(),
                preloop_vm::SecretSource::HostFile(secret_file),
            )],
        )
        .await;

    assert!(
        matches!(result, Err(VmError::Command { exit_code: 3, .. })),
        "a failing payload must surface the guest exit code, got: {result:?}"
    );
    // Best-effort cleanup ran: a standalone exec of the secret file, distinct
    // from the wrapper's own removal, which travels inside the wrapper call.
    let standalone: Vec<String> =
        exec_calls_with_script(&executable, "rm -f '/run/preloop-vm-secrets/")
            .into_iter()
            .filter(|call| !call.contains("SECRETS="))
            .collect();
    assert!(
        !standalone.is_empty(),
        "the secret file must be cleaned up after a failed payload"
    );
}

#[tokio::test]
async fn copy_maps_machine_prefixed_paths_onto_upload_and_download() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-cp").unwrap();
    provider
        .create(&spec("runner-cp", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();

    provider
        .copy("/host/file.bin", "runner-cp:/guest/file.bin")
        .await
        .unwrap();
    provider
        .copy("runner-cp:/guest/out.bin", "/host/out.bin")
        .await
        .unwrap();

    assert_eq!(
        calls_for(&executable, "upload"),
        vec!["upload|sb-0001|/host/file.bin|/guest/file.bin".to_owned()]
    );
    assert_eq!(
        calls_for(&executable, "download"),
        vec!["download|sb-0001|/guest/out.bin|/host/out.bin|--force".to_owned()]
    );

    // Neither side naming a machine is a caller bug, not a silent upload.
    assert!(matches!(
        provider.copy("/host/a", "/host/b").await,
        Err(VmError::InvalidSpec(_))
    ));
}

#[tokio::test]
async fn pack_writes_a_snapshot_descriptor_that_create_can_boot_from() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("golden-pack").unwrap();
    provider
        .create(&spec("golden-pack", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();

    let output = home.path().join("vms").join("golden");
    provider.pack(&name, &output).await.unwrap();

    // The orchestrator's artifact plumbing looks for the `.smolmachine`
    // sidecar first, so both paths must carry the descriptor.
    for path in [
        output.clone(),
        PathBuf::from(format!("{}.smolmachine", output.display())),
    ] {
        let descriptor: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(descriptor["provider"], "agentenv");
        assert_eq!(descriptor["image"], "ubuntu:24.04");
        assert!(
            descriptor["snapshot"]
                .as_str()
                .unwrap()
                .starts_with("golden-pack-pack-")
        );
    }

    // A machine created from the descriptor boots the snapshot, not an image.
    let reused = MachineName::new("from-pack").unwrap();
    provider
        .create(&spec("from-pack", &output.display().to_string()))
        .await
        .unwrap();
    provider.start(&reused).await.unwrap();
    let starts = calls_for(&executable, "start");
    let last = starts.last().unwrap();
    assert!(
        last.starts_with("start|golden-pack-pack-") && !last.contains("--cold"),
        "a packed machine must start from its snapshot: {last}"
    );
}

#[tokio::test]
async fn the_registry_survives_a_restart_so_machines_stay_deletable() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let name = MachineName::new("runner-persist").unwrap();
    {
        let provider = provider(&executable, home.path());
        provider
            .create(&spec("runner-persist", "ubuntu:24.04"))
            .await
            .unwrap();
        provider.start(&name).await.unwrap();
    }

    // A restarted engine must still find the sandbox behind the name, or the
    // pool's stale sweep leaks every VM the previous process created.
    let restarted = provider(&executable, home.path());
    assert_eq!(restarted.list().await.unwrap(), vec![name.clone()]);
    assert_eq!(
        restarted.status(&name).await.unwrap(),
        MachineState::Running
    );
    restarted.delete(&name).await.unwrap();
    assert_eq!(
        calls_for(&executable, "delete"),
        vec!["delete|sb-0001".to_owned()]
    );
}

#[tokio::test]
async fn capabilities_tell_the_pool_what_agentenv_cannot_do() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let capabilities = provider(&executable, home.path()).capabilities();

    assert!(!capabilities.live_host_volumes);
    assert!(!capabilities.socket_mounts);
    assert!(!capabilities.file_packs);
    assert!(!capabilities.fork_bases_may_mount_read_only_volumes);
    assert!(capabilities.runner_bundle_carries_externals);
}

#[tokio::test]
async fn guest_scratch_directories_are_prepared_through_the_root_or_sudo_wrapper() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-dirs").unwrap();
    provider
        .create(&spec("runner-dirs", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();

    let prepared = exec_calls_with_script(&executable, "mkdir -p /run/preloop-vm-secrets");
    assert_eq!(prepared.len(), 1, "{prepared:?}");
    assert!(
        prepared[0].contains("if [ \"$(id -u)\" -eq 0 ]; then"),
        "{}",
        prepared[0]
    );
    assert!(prepared[0].contains("| sudo -n sh; fi"), "{}", prepared[0]);
    assert_eq!(
        embedded_guest_scripts(&prepared[0]),
        vec!["mkdir -p /run/preloop-vm-secrets && chmod 700 /run/preloop-vm-secrets".to_owned()],
    );
}

#[tokio::test]
async fn a_guest_without_passwordless_sudo_fails_start_with_the_prerequisite_named() {
    let (_directory, executable) = fake_aenv();
    fs::write(executable.with_extension("fail-sudo"), "1").unwrap();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-nosudo").unwrap();
    provider
        .create(&spec("runner-nosudo", "ubuntu:24.04"))
        .await
        .unwrap();

    let error = provider
        .start(&name)
        .await
        .expect_err("a root step that sudo refuses must not report a started machine");
    match error {
        VmError::Command { message, .. } => assert!(
            message.contains("passwordless sudo"),
            "the error must name the prerequisite: {message}"
        ),
        other => panic!("expected a command error naming sudo, got {other:?}"),
    }
}

#[tokio::test]
async fn an_explicit_dns_is_refused_before_anything_runs_and_an_absent_one_starts() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());

    let mut machine = spec("runner-dns", "ubuntu:24.04");
    machine.dns = Some("192.168.1.1".to_owned());
    let refused = provider.create(&machine).await;
    assert!(
        matches!(refused, Err(VmError::InvalidSpec(_))),
        "AgentENV cannot honour a per-sandbox resolver: {refused:?}"
    );
    assert!(
        calls(&executable).is_empty(),
        "a refused spec must not reach aenv"
    );

    // The runner default is not sent to this backend, so an absent resolver is
    // the normal case and the machine starts.
    let name = MachineName::new("runner-nodns").unwrap();
    provider
        .create(&spec("runner-nodns", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();
}

#[tokio::test]
async fn pack_resumes_a_paused_machine_before_snapshotting() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("golden-pack").unwrap();
    provider
        .create(&spec("golden-pack", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();
    // The orchestrator stops a golden before packing it, and AgentENV's stop
    // is a pause, which the server will not snapshot.
    provider.stop(&name).await.unwrap();
    set_state(&executable, "paused");

    let output = home.path().join("golden.smolmachine.json");
    provider.pack(&name, &output).await.unwrap();

    let ordered: Vec<String> = calls(&executable)
        .into_iter()
        .filter(|call| call.starts_with("resume|") || call.starts_with("snapshot|"))
        .collect();
    assert_eq!(ordered.len(), 2, "{ordered:?}");
    assert_eq!(ordered[0], "resume|sb-0001|--timeout|600", "{ordered:?}");
    assert!(
        ordered[1].starts_with("snapshot|create|sb-0001|--name|golden-pack-pack-"),
        "{ordered:?}"
    );
}

/// The lifecycle calls the fake recorded, in order: pauses, resumes, snapshots.
fn lifecycle_calls(executable: &Path) -> Vec<String> {
    calls(executable)
        .into_iter()
        .filter(|call| {
            ["pause|", "resume|", "snapshot|"]
                .iter()
                .any(|verb| call.starts_with(verb))
        })
        .collect()
}

/// Packing a paused golden resumes it for the snapshot and parks it again
/// afterwards, so it is paused as the orchestrator recorded it.
#[tokio::test]
async fn pack_pauses_the_sandbox_again_after_snapshotting_it() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("golden-repause").unwrap();
    provider
        .create(&spec("golden-repause", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();
    provider.stop(&name).await.unwrap();
    set_state(&executable, "paused");

    let output = home.path().join("golden.smolmachine.json");
    provider.pack(&name, &output).await.unwrap();

    let ordered = lifecycle_calls(&executable);
    let [.., resume, snapshot, pause] = ordered.as_slice() else {
        panic!("expected resume, snapshot, pause: {ordered:?}");
    };
    assert_eq!(
        resume.as_str(),
        "resume|sb-0001|--timeout|600",
        "{ordered:?}"
    );
    assert!(
        snapshot.starts_with("snapshot|create|sb-0001|--name|golden-repause-pack-"),
        "{ordered:?}"
    );
    assert_eq!(pause.as_str(), "pause|sb-0001", "{ordered:?}");
}

/// A refused snapshot still parks the sandbox it resumed: the pause runs on the
/// error path too, and the caller sees the snapshot's refusal.
#[tokio::test]
async fn pack_pauses_the_sandbox_again_when_the_snapshot_fails() {
    let (_directory, executable) = fake_aenv();
    std::fs::write(format!("{}.fail-snapshot", executable.display()), "").unwrap();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("golden-refused").unwrap();
    provider
        .create(&spec("golden-refused", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&name).await.unwrap();
    provider.stop(&name).await.unwrap();
    set_state(&executable, "paused");

    let output = home.path().join("golden.smolmachine.json");
    assert!(
        provider.pack(&name, &output).await.is_err(),
        "the refused snapshot must surface as an error"
    );

    let ordered = lifecycle_calls(&executable);
    let [.., resume, snapshot, pause] = ordered.as_slice() else {
        panic!("expected resume, snapshot, pause: {ordered:?}");
    };
    assert_eq!(
        resume.as_str(),
        "resume|sb-0001|--timeout|600",
        "{ordered:?}"
    );
    assert!(
        snapshot.starts_with("snapshot|create|sb-0001|--name|golden-refused-pack-"),
        "{ordered:?}"
    );
    assert_eq!(pause.as_str(), "pause|sb-0001", "{ordered:?}");
}

/// Forking from a paused golden resumes it to snapshot it, then parks it again.
#[tokio::test]
async fn fork_pauses_the_golden_again_after_snapshotting_it() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let provider = provider(&executable, home.path());
    let golden = MachineName::new("golden-fork-repause").unwrap();
    let clone = MachineName::new("clone-fork-repause").unwrap();
    provider
        .create(&spec("golden-fork-repause", "ubuntu:24.04"))
        .await
        .unwrap();
    provider.start(&golden).await.unwrap();
    provider.stop(&golden).await.unwrap();
    set_state(&executable, "paused");

    provider.fork(&golden, &clone).await.unwrap();

    let ordered = lifecycle_calls(&executable);
    let snapshot_at = ordered
        .iter()
        .position(|call| {
            call.starts_with("snapshot|create|sb-0001|--name|golden-fork-repause-snap-")
        })
        .unwrap_or_else(|| panic!("no golden snapshot: {ordered:?}"));
    assert_eq!(
        ordered[snapshot_at - 1].as_str(),
        "resume|sb-0001|--timeout|600",
        "{ordered:?}"
    );
    assert_eq!(
        ordered.get(snapshot_at + 1).map(String::as_str),
        Some("pause|sb-0001"),
        "{ordered:?}"
    );
}
