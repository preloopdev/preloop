//! Contract tests for automatic collection of AgentENV volumes against a fake
//! `aenv`.
//!
//! What a pass decides is unit-tested in `preloop_vm::agentenv::gc`; what it
//! *does* is `aenv volume delete` argv, so these tests drive the real provider,
//! populate real volumes through the fake server, and assert both the plan and
//! the calls it made.
//!
//! The fixture is the fake of `crates/preloop-vm/tests/agentenv_provider.rs`
//! (see its doc comment for what each side file means), trimmed to the commands
//! collection needs. It lives here rather than there because the two files are
//! edited by different changes: a pass is only interesting in the states those
//! tests do not set up — a volume no sandbox mounts, a staging leftover, a
//! populate still running.

#![cfg(unix)]

use preloop_vm::agentenv::gc::{self, Config, Mode, Request, StagedVolume, StagingRegistry};
use preloop_vm::agentenv::{AgentEnvProvider, AgentEnvVolume};
use preloop_vm::{MachineName, MachineRole, MachineSpec, NetworkPolicy, VmProvider, VolumeMount};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

/// Write `contents` to `path` as an executable, without this process ever
/// holding a write descriptor.
///
/// Concurrent tests in one binary otherwise hit ETXTBSY: a descriptor open in
/// this process is inherited by every other test's `fork` until that child
/// reaches `exec`. Same reasoning as `tests/agentenv_provider.rs`.
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
/// line, NUL-joined arguments) and keeps sandboxes, mounts and volumes in side
/// files, so a test can plant a leftover volume or see what a pass deleted.
///
/// * `<exe>.volumes` holds one `name|mode|from` row per volume.
/// * `<exe>.mounts` holds one `sandbox|guest|volume` row per mount, and
///   `volume delete` refuses a volume in it (as the real server does).
/// * `<exe>.slow-exec` makes every `exec` sleep, i.e. parks a populate between
///   creating its staging volume and committing it.
/// * `<exe>.state` selects what `ls` reports (`gone` lists nothing).
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
  exec)
    if [ -f "$0.slow-exec" ]; then sleep 3; fi
    exit 0
    ;;
  upload)
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
        status=ready
        [ -f "$0.volume-status" ] && status=$(cat "$0.volume-status")
        first=1
        printf '['
        while IFS='|' read -r name mode from; do
          [ -n "$name" ] || continue
          [ $first -eq 1 ] || printf ','
          first=0
          printf '{"name":"%s","mode":"%s","status":"%s"}' "$name" "$mode" "$status"
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

/// The volumes the fake server holds, in creation order.
fn volume_names(executable: &Path) -> Vec<String> {
    fs::read_to_string(executable.with_extension("volumes"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.split('|').next().map(str::to_owned))
        .filter(|name| !name.is_empty())
        .collect()
}

/// Plant a volume in the fake server, as one left by an earlier engine.
fn plant_volume(executable: &Path, name: &str) {
    let path = executable.with_extension("volumes");
    let mut rows = fs::read_to_string(&path).unwrap_or_default();
    rows.push_str(&format!("{name}|exclusive|\n"));
    fs::write(&path, rows).unwrap();
}

/// Wait (bounded) for the fake server to hold a volume whose name starts with
/// `prefix`, and return it.
///
/// Asynchronous, and always awaited: the loop must yield, or a populate running
/// on this test's runtime never gets to create its staging volume.
async fn wait_for_volume(executable: &Path, prefix: &str) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(name) = volume_names(executable)
            .into_iter()
            .find(|name| name.starts_with(prefix))
        {
            return name;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no volume starting with {prefix} ever appeared"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The staging registry a provider writes records to.
fn staging_path(gc_home: &Path) -> PathBuf {
    gc_home.join("agentenv-volume-staging.json")
}

/// The staging records in the registry file.
fn staging_records(gc_home: &Path) -> StagingRegistry {
    match fs::read(staging_path(gc_home)) {
        Ok(bytes) => serde_json::from_slice(&bytes).expect("a readable staging registry"),
        Err(_) => StagingRegistry::default(),
    }
}

/// Write staging records, as a populate that died would leave them.
fn plant_staging(gc_home: &Path, staged: &[(&str, &str, u64)]) {
    let registry = StagingRegistry {
        staging: staged
            .iter()
            .map(|(name, digest, created)| {
                (
                    (*name).to_owned(),
                    StagedVolume {
                        digest: (*digest).to_owned(),
                        host: "/opt/preloop/bundle".to_owned(),
                        created: *created,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>(),
    };
    fs::write(
        staging_path(gc_home),
        serde_json::to_vec_pretty(&registry).unwrap(),
    )
    .unwrap();
}

/// A provider whose passes run with `grace` and `keep_previous`, and whose
/// collection never starts on its own — every test runs a pass by hand.
fn provider(
    executable: &Path,
    home: &Path,
    gc_home: &Path,
    grace_secs: u64,
    keep_previous: usize,
) -> AgentEnvProvider {
    AgentEnvProvider::new(executable)
        .with_registry_path(home.join("machines.json"))
        .with_volume_index_path(home.join("agentenv-runner-volumes.json"))
        .with_ttl_secs(600)
        .with_volume_gc(gc::GcState::new(
            Config {
                mode: Mode::Off,
                keep_previous,
                grace: Duration::from_secs(grace_secs),
                interval: Duration::ZERO,
            },
            Some(staging_path(gc_home)),
        ))
}

/// A provider that collects on its own: automatic mode, the given timer, and a
/// grace period long enough that only the planted leftovers are old.
fn collecting_engine(
    executable: &Path,
    home: &Path,
    gc_home: &Path,
    grace_secs: u64,
    keep_previous: usize,
    interval: Duration,
) -> AgentEnvProvider {
    AgentEnvProvider::new(executable)
        .with_registry_path(home.join("machines.json"))
        .with_volume_index_path(home.join("agentenv-runner-volumes.json"))
        .with_ttl_secs(600)
        .with_volume_gc(gc::GcState::new(
            Config {
                mode: Mode::Auto,
                keep_previous,
                grace: Duration::from_secs(grace_secs),
                interval,
            },
            Some(staging_path(gc_home)),
        ))
}

/// Wait (bounded) for the fake server to drop `name`.
async fn wait_for_volume_gone(executable: &Path, name: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while volume_names(executable).iter().any(|volume| volume == name) {
        assert!(
            std::time::Instant::now() < deadline,
            "{name} was never collected"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A provider that runs a pass without holding the machines.
///
/// That is the engine after a restart and the CLI: a provider that published a
/// volume keeps it (its own live set is part of every plan), so a test that
/// expects a deletion plans with a fresh provider and starts its machines with
/// [`provider`].
fn collecting_provider(
    executable: &Path,
    home: &Path,
    gc_home: &Path,
    grace_secs: u64,
    keep_previous: usize,
) -> AgentEnvProvider {
    provider(executable, home, gc_home, grace_secs, keep_previous)
}

fn bundle_mount(host: &Path) -> VolumeMount {
    VolumeMount {
        host: host.to_path_buf(),
        guest: PathBuf::from("/opt/preloop/bin"),
        read_only: true,
    }
}

fn spec(name: &str, bundle: &Path) -> MachineSpec {
    MachineSpec {
        name: MachineName::new(name).unwrap(),
        image: "ubuntu:24.04".to_owned(),
        cpus: 4,
        memory_mib: 4096,
        storage_gib: 20,
        overlay_gib: None,
        network: NetworkPolicy::Unrestricted,
        volumes: vec![bundle_mount(bundle)],
        sockets: Vec::new(),
        dns: None,
        rosetta: false,
        role: MachineRole::Image,
        guest_volumes: None,
        workload_init: None,
    }
}

/// The fake server's `volume list`, as the provider reads it.
fn listed(executable: &Path) -> Vec<AgentEnvVolume> {
    let output = std::process::Command::new(executable)
        .args(["volume", "list"])
        .output()
        .expect("the fake aenv runs");
    serde_json::from_slice(&output.stdout).expect("a readable volume list")
}

/// Start a runner on `bundle` and return the volume it was published as.
async fn populated(
    provider: &AgentEnvProvider,
    executable: &Path,
    name: &str,
    bundle: &Path,
) -> String {
    provider.create(&spec(name, bundle)).await.unwrap();
    provider
        .start(&MachineName::new(name).unwrap())
        .await
        .unwrap();
    volume_names(executable)
        .into_iter()
        .find(|volume| !volume.starts_with("preloop-runner-stage-"))
        .expect("the populate published a volume")
}

#[tokio::test]
async fn collection_never_deletes_a_volume_a_sandbox_mounts() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    // A grace period of zero, so the only thing that can save the volume is the
    // server refusing to delete a volume a sandbox holds.
    let provider = provider(&executable, home.path(), gc_home.path(), 0, 0);
    let volume = populated(&provider, &executable, "runner-0", bundle.path()).await;

    let collector = collecting_provider(&executable, home.path(), gc_home.path(), 0, 0);
    let outcome = gc::run_pass(&collector, Request::new()).await.unwrap();

    assert_eq!(
        outcome.plan.delete_names(),
        vec![volume.clone()],
        "the plan lists the volume it cannot use"
    );
    assert_eq!(
        outcome.report.in_use,
        vec![volume.clone()],
        "the server refused, so the pass reports it as in use"
    );
    assert!(outcome.report.deleted.is_empty(), "nothing was deleted");
    assert!(
        volume_names(&executable).contains(&volume),
        "the mounted volume is still there"
    );
    assert!(
        calls(&executable).contains(&format!("volume|delete|{volume}")),
        "the pass tried, and the server decided: {:?}",
        calls_for(&executable, "volume")
    );
    assert!(outcome.render(gc::unix_now()).contains("in use"));
}

#[tokio::test]
async fn a_volume_a_pass_deleted_is_populated_again_on_the_next_start() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path(), gc_home.path(), 0, 0);
    let volume = populated(&provider, &executable, "runner-0", bundle.path()).await;
    // No sandbox holds the volume once the runner is gone, which is exactly the
    // state a pass reclaims.
    provider
        .delete(&MachineName::new("runner-0").unwrap())
        .await
        .unwrap();

    let collector = collecting_provider(&executable, home.path(), gc_home.path(), 0, 0);
    let outcome = gc::run_pass(&collector, Request::new()).await.unwrap();

    assert_eq!(outcome.report.deleted, vec![volume.clone()]);
    assert!(!volume_names(&executable).contains(&volume));
    assert!(
        collector.volume_index().await.get(&volume).is_none(),
        "the index entry goes with the volume"
    );

    // The next start re-derives the same name from the same tree and populates
    // it again: the memo revalidates the name and finds it gone.
    provider
        .create(&spec("runner-0", bundle.path()))
        .await
        .unwrap();
    provider
        .start(&MachineName::new("runner-0").unwrap())
        .await
        .unwrap();
    let freezes = calls_for(&executable, "volume")
        .into_iter()
        .filter(|call| call.starts_with(&format!("volume|create|{volume}|--mode|ro|")))
        .count();
    assert_eq!(freezes, 2, "the volume was populated again");
    assert!(volume_names(&executable).contains(&volume));
    assert_eq!(
        calls_for(&executable, "upload").len(),
        2,
        "a repopulate uploads the tree again"
    );
}

#[tokio::test]
async fn a_plan_deletes_a_superseded_volume_without_repopulating_a_kept_one() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path(), gc_home.path(), 0, 1);
    let volume = populated(&provider, &executable, "runner-0", bundle.path()).await;
    provider
        .delete(&MachineName::new("runner-0").unwrap())
        .await
        .unwrap();

    // The default rollback depth keeps the only superseded volume, so nothing is
    // deleted and the volume a rollback would use is still there.
    let collector = collecting_provider(&executable, home.path(), gc_home.path(), 0, 1);
    let kept = gc::run_pass(&collector, Request::new()).await.unwrap();
    assert_eq!(kept.report.deleted, Vec::<String>::new());
    assert!(volume_names(&executable).contains(&volume));

    // With a second superseded volume the older one goes. The volume is planted
    // with an index entry, as a build that published it would have left, so the
    // rollback rule has two volumes to rank.
    let other = "preloop-runner-999999999999";
    plant_volume(&executable, other);
    let index_path = home.path().join("agentenv-runner-volumes.json");
    let mut index: serde_json::Value =
        serde_json::from_slice(&fs::read(&index_path).unwrap()).unwrap();
    index["volumes"][other] = serde_json::json!({
        "created": gc::unix_now().saturating_sub(10),
    });
    fs::write(&index_path, serde_json::to_vec(&index).unwrap()).unwrap();

    let collector = collecting_provider(&executable, home.path(), gc_home.path(), 0, 1);
    let outcome = gc::run_pass(&collector, Request::new()).await.unwrap();
    assert_eq!(outcome.report.deleted, vec![other.to_owned()]);
    assert!(!volume_names(&executable).iter().any(|name| name == other));
    assert!(volume_names(&executable).contains(&volume));
}

#[tokio::test]
async fn an_abandoned_staging_volume_is_reclaimed_and_a_fresh_one_is_not() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path(), gc_home.path(), 3600, 0);
    let stale = "preloop-runner-stage-0123456789ab-aaaaaaaa";
    let fresh = "preloop-runner-stage-fedcba987654-bbbbbbbb";
    plant_volume(&executable, stale);
    plant_volume(&executable, fresh);
    let now = gc::unix_now();
    plant_staging(
        gc_home.path(),
        &[
            (stale, "0123456789ab", now - 7200),
            (fresh, "fedcba987654", now - 30),
        ],
    );

    let outcome = gc::run_pass(&provider, Request::new()).await.unwrap();

    assert_eq!(outcome.report.deleted, vec![stale.to_owned()]);
    assert!(!volume_names(&executable).contains(&stale.to_owned()));
    assert!(volume_names(&executable).contains(&fresh.to_owned()));
    let records = staging_records(gc_home.path());
    assert!(records.get(stale).is_none(), "the record goes with it");
    assert!(records.get(fresh).is_some(), "the fresh record stays");
    assert_eq!(outcome.plan.delete_names(), vec![stale.to_owned()]);
}

#[tokio::test]
async fn a_populate_in_flight_keeps_its_staging_volume() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    // Grace zero: the only thing keeping the staging volume of a running
    // populate is the record the populate itself holds.
    let provider = provider(&executable, home.path(), gc_home.path(), 0, 0);
    fs::write(executable.with_extension("slow-exec"), b"1").unwrap();

    let starting = {
        let provider = provider.clone();
        let machine = spec("runner-0", bundle.path());
        let name = MachineName::new("runner-0").unwrap();
        tokio::spawn(async move {
            provider.create(&machine).await.unwrap();
            provider.start(&name).await.unwrap();
        })
    };

    // Wait until the populate has created its staging volume and is parked on
    // the slow `exec`, then collect while it runs.
    let staging = wait_for_volume(&executable, "preloop-runner-stage-").await;
    let outcome = gc::run_pass(&provider, Request::new()).await.unwrap();
    let verdict = outcome
        .plan
        .verdicts()
        .iter()
        .find(|verdict| verdict.name == staging)
        .expect("the running populate's stage is in the plan");
    assert_eq!(
        verdict.disposition,
        gc::Disposition::Keep(gc::KeepReason::Populating),
        "an in-flight stage is never a candidate"
    );
    assert!(outcome.report.deleted.is_empty(), "nothing was deleted");
    // The stage itself may be gone by the time this runs — the populate can
    // finish and discard it — so what proves the point is the plan (kept as
    // `populating`) and the report (nothing deleted).

    starting.await.unwrap();
    let published = volume_names(&executable)
        .into_iter()
        .find(|volume| !volume.starts_with("preloop-runner-stage-"))
        .expect("the populate published its volume");
    assert!(!volume_names(&executable).contains(&staging));
    assert!(
        calls_for(&executable, "start").len() >= 2,
        "the populate and the runner both started: {:?}",
        calls_for(&executable, "start")
    );
    assert!(provider.volume_index().await.get(&published).is_some());
}

#[tokio::test]
async fn a_populate_collects_a_superseded_volume_in_the_background() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    // A volume left by a build that is gone, with the index entry that makes it
    // old enough to collect. It is in the index before the provider reads it.
    let stale = "preloop-runner-999999999999";
    plant_volume(&executable, stale);
    fs::write(
        home.path().join("agentenv-runner-volumes.json"),
        serde_json::to_vec(&serde_json::json!({
            "volumes": { stale: { "created": gc::unix_now().saturating_sub(7200) } },
        }))
        .unwrap(),
    )
    .unwrap();
    let provider = collecting_engine(
        &executable,
        home.path(),
        gc_home.path(),
        3600,
        0,
        Duration::ZERO,
    );

    // One start publishes this engine's volume and asks for a pass. Nothing
    // waits for that pass, so the test waits for its effect instead.
    provider
        .create(&spec("runner-0", bundle.path()))
        .await
        .unwrap();
    provider
        .start(&MachineName::new("runner-0").unwrap())
        .await
        .unwrap();
    wait_for_volume_gone(&executable, stale).await;

    let published = volume_names(&executable)
        .into_iter()
        .find(|volume| volume.starts_with("preloop-runner-"))
        .expect("the start published a volume");
    assert_ne!(published, stale);
    assert!(
        calls(&executable).contains(&format!("volume|delete|{stale}")),
        "the pass deleted it through aenv: {:?}",
        calls_for(&executable, "volume")
    );
}

#[tokio::test]
async fn the_timer_collects_without_any_start() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let stale = "preloop-runner-999999999999";
    plant_volume(&executable, stale);
    fs::write(
        home.path().join("agentenv-runner-volumes.json"),
        serde_json::to_vec(&serde_json::json!({
            "volumes": { stale: { "created": gc::unix_now().saturating_sub(7200) } },
        }))
        .unwrap(),
    )
    .unwrap();
    // A 50 ms period instead of six hours, so the test can watch the timer work:
    // no machine, no populate, no pass asked for.
    let provider = collecting_engine(
        &executable,
        home.path(),
        gc_home.path(),
        3600,
        0,
        Duration::from_millis(50),
    );

    gc::ensure_scheduled(&provider);
    wait_for_volume_gone(&executable, stale).await;
}

#[tokio::test]
async fn a_dry_run_reports_the_plan_and_deletes_nothing() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path(), gc_home.path(), 0, 0);
    let volume = populated(&provider, &executable, "runner-0", bundle.path()).await;
    provider
        .delete(&MachineName::new("runner-0").unwrap())
        .await
        .unwrap();

    let before = calls(&executable).len();
    let collector = collecting_provider(&executable, home.path(), gc_home.path(), 0, 0);
    let outcome = gc::run_pass(&collector, Request::new().dry_run(true))
        .await
        .unwrap();

    assert!(outcome.dry_run);
    assert_eq!(outcome.plan.delete_names(), vec![volume.clone()]);
    assert!(outcome.report.deleted.is_empty());
    assert!(volume_names(&executable).contains(&volume));
    // The populate discarded its own staging volume earlier; what a dry run must
    // not do is delete anything itself.
    let during = calls(&executable)[before..].join("\n");
    assert!(
        !during.contains("volume|delete|"),
        "a dry run never deletes: {during}"
    );
    let rendered = outcome.render(gc::unix_now());
    assert!(rendered.contains(&volume), "{rendered}");
    assert!(rendered.contains("superseded"), "{rendered}");
}

#[tokio::test]
async fn a_pass_keeps_the_volumes_the_caller_names() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path(), gc_home.path(), 0, 0);
    let volume = populated(&provider, &executable, "runner-0", bundle.path()).await;
    provider
        .delete(&MachineName::new("runner-0").unwrap())
        .await
        .unwrap();

    // What `preloop volumes gc --keep <dir>` and an engine's own published set
    // do: the volume stays, however old and unmounted it is.
    let before = calls(&executable).len();
    let outcome = gc::run_pass(&provider, Request::new().with_live([volume.clone()]))
        .await
        .unwrap();

    assert!(outcome.report.deleted.is_empty());
    assert!(volume_names(&executable).contains(&volume));
    let during = calls(&executable)[before..].join("\n");
    assert!(
        !during.contains("volume|delete|"),
        "a kept volume is not deleted: {during}"
    );
}

#[tokio::test]
async fn a_volume_agentenv_reports_as_unusable_is_reclaimed_once_it_is_old() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path(), gc_home.path(), 0, 0);
    let volume = populated(&provider, &executable, "runner-0", bundle.path()).await;
    provider
        .delete(&MachineName::new("runner-0").unwrap())
        .await
        .unwrap();

    // The server reports the volume as failed: this engine refuses to mount it,
    // so a start stays stuck until a pass reclaims it.
    fs::write(executable.with_extension("volume-status"), b"failed").unwrap();
    let collector = collecting_provider(&executable, home.path(), gc_home.path(), 0, 0);
    let outcome = gc::run_pass(&collector, Request::new()).await.unwrap();

    assert_eq!(outcome.report.deleted, vec![volume.clone()]);
    assert!(!volume_names(&executable).contains(&volume));
    assert_eq!(
        outcome
            .plan
            .verdicts()
            .iter()
            .find(|verdict| verdict.name == volume)
            .map(|verdict| verdict.disposition),
        Some(gc::Disposition::Delete(gc::DeleteReason::Unusable))
    );
}

#[tokio::test]
async fn a_published_volume_the_server_calls_unusable_is_reclaimed_from_the_engine_that_published_it()
 {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    let provider = provider(&executable, home.path(), gc_home.path(), 0, 0);
    let volume = populated(&provider, &executable, "runner-0", bundle.path()).await;
    provider
        .delete(&MachineName::new("runner-0").unwrap())
        .await
        .unwrap();
    fs::write(executable.with_extension("volume-status"), b"failed").unwrap();

    // The engine that published the volume is still running, so its own memo
    // names it. Keeping it because of that would leave every start on this tree
    // stuck: the engine refuses to mount a volume that is not ready and never
    // populates it again while it is there.
    let outcome = gc::run_pass(&provider, Request::new()).await.unwrap();

    assert_eq!(
        outcome
            .plan
            .verdicts()
            .iter()
            .find(|verdict| verdict.name == volume)
            .map(|verdict| verdict.disposition),
        Some(gc::Disposition::Delete(gc::DeleteReason::Unusable)),
        "a memo is not a reason to keep a volume that cannot be mounted"
    );
    assert_eq!(outcome.report.deleted, vec![volume.clone()]);

    // With the unusable volume gone, the next start on the same tree populates
    // again instead of failing.
    fs::remove_file(executable.with_extension("volume-status")).unwrap();
    provider
        .create(&spec("runner-0", bundle.path()))
        .await
        .unwrap();
    provider
        .start(&MachineName::new("runner-0").unwrap())
        .await
        .unwrap();
    assert!(volume_names(&executable).contains(&volume));
    assert_eq!(
        calls_for(&executable, "upload").len(),
        2,
        "the tree was populated again"
    );
}

#[tokio::test]
async fn a_final_volume_that_is_not_ready_still_records_when_it_was_created() {
    let (_directory, executable) = fake_aenv();
    let home = tempfile::tempdir().unwrap();
    let gc_home = tempfile::tempdir().unwrap();
    let bundle = tempfile::tempdir().unwrap();
    fs::write(bundle.path().join("preloop-runner"), b"ELF").unwrap();
    // The server keeps reporting the volume as failed, so nothing can be mounted
    // from the tree and the populate fails after creating it.
    fs::write(executable.with_extension("volume-status"), b"failed").unwrap();
    let provider = provider(&executable, home.path(), gc_home.path(), 3600, 0);
    provider
        .create(&spec("runner-0", bundle.path()))
        .await
        .unwrap();
    assert!(
        provider
            .start(&MachineName::new("runner-0").unwrap())
            .await
            .is_err(),
        "a volume that is not ready cannot be mounted"
    );
    let volume = volume_names(&executable)
        .into_iter()
        .find(|volume| !volume.starts_with("preloop-runner-stage-"))
        .expect("the populate created the final volume before it failed");

    // The attempt is recorded, so the volume has an age. Without one it counts
    // as `unknown-age` and is kept for ever, which is what made the unusable
    // volume above impossible to recover from.
    let recorded = provider
        .volume_index()
        .await
        .get(&volume)
        .cloned()
        .expect("the failed final volume is recorded");
    assert!(recorded.created > 0);

    // An engine that did not publish it (a restart) sees it as old and reclaims
    // it, so a later start on the tree populates again.
    let collector = collecting_provider(&executable, home.path(), gc_home.path(), 0, 0);
    let outcome = gc::run_pass(&collector, Request::new()).await.unwrap();
    assert_eq!(outcome.report.deleted, vec![volume.clone()]);
    assert!(!volume_names(&executable).contains(&volume));
}

#[test]
fn the_fixture_reports_the_volumes_it_holds() {
    // The tests above read the volume table through `aenv`, so the fixture
    // itself is worth one assertion: a row per volume, in creation order.
    let (_directory, executable) = fake_aenv();
    plant_volume(&executable, "preloop-runner-0123456789ab");
    plant_volume(&executable, "preloop-runner-stage-0123456789ab-aaaaaaaa");
    assert_eq!(
        volume_names(&executable),
        vec![
            "preloop-runner-0123456789ab".to_owned(),
            "preloop-runner-stage-0123456789ab-aaaaaaaa".to_owned(),
        ]
    );
    let listed = listed(&executable);
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].status, "ready");
}
