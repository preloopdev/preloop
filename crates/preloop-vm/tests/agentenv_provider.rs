//! Contract tests for the AgentENV provider against a fake `aenv`.
//!
//! The provider's whole job is translating [`VmProvider`] calls into `aenv`
//! invocations, so the argv it emits *is* the observable behaviour: a wrong
//! flag here is a job that never boots on a real host. The fake records every
//! invocation, answers the handful of commands the provider parses, and lets
//! each test assert both what was asked and what the provider concluded.
//!
//! Every command string asserted here was verified against AgentENV 0.2.0 on a
//! Linux/KVM host (see `docs/vm-substrates.md`).

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
/// `<exe>.state` selects what `ls` reports for the sandbox it invented, so a
/// test can drive the provider through running/paused/vanished.
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

case "${1-}" in
  start)
    # `aenv start ... -d` prints only the sandbox id.
    printf 'sb-0001\n'
    ;;
  ls)
    if [ "$state" = gone ]; then
      printf '[]\n'
    else
      printf '[{"sandboxID":"sb-0001","templateID":"t","alias":null,"state":"%s","cpuCount":4,"memoryMB":4096,"diskSizeMB":65536,"startedAt":"now","endAt":"later"}]\n' "$state"
    fi
    ;;
  snapshot)
    printf 'Created snapshot snap-9999\n'
    ;;
  exec)
    # Guest probes and provisioning shell-outs all succeed. When the test
    # plants `<exe>.fail-payload`, the wrapper that sources a secret file
    # and execs the payload fails instead — the cleanup `rm -f` exec and
    # every probe still succeed, matching a real control plane.
    last=""
    for arg in "$@"; do last="$arg"; done
    if [ -f "$0.fail-payload" ]; then
      case "$last" in
        "rm -f "*|"set -eu; . "*) exit 3 ;;
      esac
    fi
    exit 0
    ;;
  upload)
    printf 'Uploaded\n'
    ;;
  download)
    printf 'Downloaded\n'
    ;;
  pause|resume|delete|timeout)
    printf 'ok\n'
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

fn set_state(executable: &Path, state: &str) {
    fs::write(executable.with_extension("state"), state).unwrap();
}

fn provider(executable: &Path, registry: &Path) -> AgentEnvProvider {
    AgentEnvProvider::new(executable)
        .with_registry_path(registry.join("machines.json"))
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
    assert!(provider
        .rearm_fork_base(&golden, Some(&MachineName::new("clone-x").unwrap()))
        .await
        .unwrap());
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

#[tokio::test]
async fn host_volumes_are_streamed_in_as_a_tar_and_read_only_mounts_lose_write_bits() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());
    let name = MachineName::new("runner-vol").unwrap();

    let mut machine = spec("runner-vol", "ubuntu:24.04");
    machine.volumes = vec![VolumeMount {
        host: bundle.path().to_path_buf(),
        guest: PathBuf::from("/opt/preloop/bin"),
        read_only: true,
    }];
    provider.create(&machine).await.unwrap();
    provider.start(&name).await.unwrap();

    let uploads = calls_for(&executable, "upload");
    assert_eq!(uploads.len(), 1, "uploads: {uploads:?}");
    let fields: Vec<&str> = uploads[0].split('|').collect();
    assert_eq!(fields[0], "upload");
    assert_eq!(fields[1], "sb-0001");
    assert!(
        fields[2].ends_with(".tar"),
        "a host volume must move as one tar stream (aenv upload rejects symlinks): {:?}",
        fields
    );
    assert!(fields[3].starts_with("/run/preloop-vm-staging/"));

    let unpack = calls_for(&executable, "exec")
        .into_iter()
        .find(|call| call.contains("tar -xf"))
        .expect("an exec must unpack the uploaded archive");
    assert!(unpack.contains("'/opt/preloop/bin'"), "{unpack}");
    assert!(
        unpack.contains("chmod -R a-w '/opt/preloop/bin'"),
        "a read-only mount must lose its write bits: {unpack}"
    );
}

#[tokio::test]
async fn a_clone_inherits_materialized_volumes_instead_of_re_uploading_them() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path());
    let golden = MachineName::new("golden-vol").unwrap();

    let mut machine = spec("golden-vol", "ubuntu:24.04");
    machine.volumes = vec![VolumeMount {
        host: bundle.path().to_path_buf(),
        guest: PathBuf::from("/opt/preloop/bin"),
        read_only: true,
    }];
    provider.create(&machine).await.unwrap();
    provider.start_forkable(&golden).await.unwrap();
    provider
        .fork(&golden, &MachineName::new("clone-vol").unwrap())
        .await
        .unwrap();

    // The snapshot already contains the unpacked bundle. Re-uploading per
    // clone would put the pool's whole runner bundle on the fork path.
    assert_eq!(calls_for(&executable, "upload").len(), 1);
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
        error,
        VmError::Command {
            operation: "exec",
            exit_code: 9,
            ref message,
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
    // It travelled as an uploaded file that the wrapper sources and unlinks.
    let wrapper = calls_for(&executable, "exec")
        .into_iter()
        .find(|call| call.contains("preloop-runner"))
        .expect("the payload must still run");
    assert!(wrapper.contains(". '/run/preloop-vm-secrets/"), "{wrapper}");
    assert!(
        wrapper.contains("rm -f '/run/preloop-vm-secrets/"),
        "{wrapper}"
    );
    assert!(
        wrapper.contains("exec '/opt/preloop/bin/preloop-runner' 'run'"),
        "{wrapper}"
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
    // Best-effort cleanup ran: a plain `rm -f` exec of the secret file,
    // distinct from the wrapper's inlined `rm -f`.
    let cleanup = calls_for(&executable, "exec")
        .into_iter()
        .find(|call| call.contains("|rm -f '/run/preloop-vm-secrets/"))
        .expect("the secret file must be cleaned up after a failed payload");
    assert!(
        cleanup.contains("rm -f '/run/preloop-vm-secrets/"),
        "{cleanup}"
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
        assert!(descriptor["snapshot"]
            .as_str()
            .unwrap()
            .starts_with("golden-pack-pack-"));
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
}
