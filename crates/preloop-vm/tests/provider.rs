use preloop_vm::{MachineName, VmError};

#[test]
fn machine_names_accept_dns_like_names_and_reject_invalid_boundaries() {
    let max_length = "a".repeat(63);
    for valid in ["runner", "runner-01", "A1", max_length.as_str()] {
        assert_eq!(MachineName::new(valid).unwrap().as_str(), valid);
    }

    let too_long = "a".repeat(64);
    for invalid in [
        "",
        "-leading",
        "trailing-",
        "has_underscore",
        "has space",
        too_long.as_str(),
    ] {
        assert!(matches!(
            MachineName::new(invalid),
            Err(VmError::InvalidMachineName(value)) if value == invalid
        ));
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use preloop_vm::{
        ExecOutput, GuestVolume, MachineRole, MachineSpec, MachineState, NetworkPolicy,
        SmolVmProvider, SocketMount, VmProvider, VolumeMount,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    static TEST_ENV_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn write_executable(path: &Path, contents: &str) {
        // Write through a child process rather than `fs::write`, then publish
        // with a rename.
        //
        // These tests run concurrently in one process. Any write descriptor
        // this process holds is inherited by every other test's `fork` and
        // stays open until that child reaches `exec`; executing the script
        // inside that window fails with ETXTBSY ("Text file busy") even though
        // our own writer is already closed. Staging plus rename alone does not
        // help, because the inherited descriptor refers to the same inode the
        // rename publishes. Reproduced at ~1 run in 16 under CPU load.
        // Keeping the file descriptor out of this process closes the window:
        // only `sh` ever holds it, and our forks cannot inherit its table.
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
        assert!(
            child.wait().unwrap().success(),
            "staging {}",
            staged.display()
        );
        fs::rename(staged, path).unwrap();
    }

    fn fake_smolvm() -> (TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("smolvm");
        write_executable(
            &executable,
            r##"#!/bin/sh
set -eu

args="$0.args"
: > "$args"
printf 'SMOLVM_EGRESS_FLOOR=%s\n' "${SMOLVM_EGRESS_FLOOR-}" > "$0.env"
env_file="$0.env"
printf 'SMOLVM_EGRESS_FLOOR=%s\n' "${SMOLVM_EGRESS_FLOOR-}" > "$env_file"
printf 'SMOLVM_SECCOMP=%s\n' "${SMOLVM_SECCOMP-}" > "$0.seccomp"
printf 'SMOLVM_LANDLOCK=%s\n' "${SMOLVM_LANDLOCK-}" > "$0.landlock"
printf 'SMOLVM_CGROUP_ROOT=%s\n' "${SMOLVM_CGROUP_ROOT-}" > "$0.cgroup"
printf 'TMPDIR=%s\n' "${TMPDIR-}" > "$0.tmpdir"
for arg in "$@"; do
  printf '%s\n' "$arg" >> "$args"
done

if [ "${1-}:${2-}" = "machine:update" ] && [ -f "$0.fail-update" ]; then
  printf 'rosetta unavailable\n' >&2
  exit 42
fi

if [ "${1-}:${2-}:${3-}" = "machine:create:--help" ]; then
  printf '%s\n' "Usage: smolvm machine create --name <NAME> --mount-socket <HOST:GUEST>"
  exit 0
fi

case "${1-}:${2-}" in
  machine:create)
    exit 0
    ;;
  pack:create)
    exit 0
    ;;
  machine:status)
    case "${4-}" in
      running) printf 'RUNNING\n' ;;
      stopped) printf 'stopped\n' ;;
      missing) printf 'machine not found\n' >&2; exit 1 ;;
      *) printf 'paused\n' ;;
    esac
    ;;
  machine:ls)
    printf '[{"name":"alpha"},{"name":"beta-2"},{"kind":"other"}]\n'
    ;;
  machine:exec)
    if [ "${3-}" = "--stream" ]; then
      printf 'stream-out\n'
      printf 'stream-err\n' >&2
    elif [ "${8-}" = "large-output" ]; then
      printf '%200000s' ''
      printf '%200000s' '' >&2
    else
      printf 'ok\n'
    fi
    ;;
  *)
    exit 0
    ;;
esac
"##,
        );
        (directory, executable)
    }

    fn captured_args(executable: &Path) -> Vec<String> {
        fs::read_to_string(executable.with_extension("args"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn captured_env(executable: &Path) -> String {
        fs::read_to_string(executable.with_extension("env"))
            .unwrap_or_default()
            .trim()
            .to_owned()
    }

    fn captured_sandbox_var(executable: &Path, suffix: &str) -> String {
        fs::read_to_string(executable.with_extension(suffix))
            .unwrap_or_default()
            .trim()
            .to_owned()
    }

    fn captured_tmpdir(executable: &Path) -> String {
        fs::read_to_string(executable.with_extension("tmpdir"))
            .unwrap_or_default()
            .trim()
            .to_owned()
    }

    fn valid_spec(name: MachineName) -> MachineSpec {
        MachineSpec {
            name,
            image: "ghcr.io/acme/runner:latest".to_owned(),
            cpus: 2,
            memory_mib: 256,
            storage_gib: 10,
            overlay_gib: None,
            network: NetworkPolicy::Disabled,
            volumes: Vec::new(),
            sockets: Vec::new(),
            dns: None,
            rosetta: false,
            role: MachineRole::Image,
            guest_volumes: None,
            workload_init: None,
        }
    }

    /// A smolvm whose `machine create` predates `--mount-socket`: only the
    /// old docker-specific flag exists, so the capability probe must fail.
    fn fake_old_smolvm() -> (TempDir, PathBuf) {
        let (directory, executable) = fake_smolvm();
        write_executable(
            &executable,
            r##"#!/bin/sh
set -eu

args="$0.args"
: > "$args"
for arg in "$@"; do
  printf '%s\n' "$arg" >> "$args"
done

if [ "${1-}:${2-}:${3-}" = "machine:create:--help" ]; then
  printf '%s\n' "Usage: smolvm machine create --name <NAME> --docker-socket [-- <COMMAND>...]"
  exit 0
fi

exit 0
"##,
        );
        (directory, executable)
    }

    #[tokio::test]
    async fn create_emits_exact_network_volume_and_socket_arguments() {
        let (directory, executable) = fake_smolvm();
        let host_rw = directory.path().join("workspace");
        let host_ro = directory.path().join("cache");
        let host_socket = directory.path().join("engine.sock");
        fs::create_dir_all(&host_rw).unwrap();
        fs::create_dir_all(&host_ro).unwrap();
        let _socket = std::os::unix::net::UnixListener::bind(&host_socket).unwrap();
        let spec = MachineSpec {
            name: MachineName::new("ci-01").unwrap(),
            image: "ghcr.io/acme/runner:latest".to_owned(),
            cpus: 4,
            memory_mib: 512,
            storage_gib: 20,
            overlay_gib: Some(30),
            network: NetworkPolicy::Restricted {
                hosts: vec!["example.com".to_owned(), "api.example.com".to_owned()],
                cidrs: vec!["10.0.0.0/8".to_owned(), "2001:db8::/32".to_owned()],
            },
            dns: Some("192.168.1.1".into()),
            volumes: vec![
                VolumeMount {
                    host: host_rw.clone(),
                    guest: PathBuf::from("/workspace"),
                    read_only: false,
                },
                VolumeMount {
                    host: host_ro.clone(),
                    guest: PathBuf::from("/cache"),
                    read_only: true,
                },
            ],
            sockets: vec![SocketMount {
                host: host_socket.clone(),
                guest: PathBuf::from("/run/preloop-engine.sock"),
            }],
            rosetta: false,
            role: MachineRole::Image,
            guest_volumes: None,
            workload_init: None,
        };

        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();

        assert_eq!(
            captured_args(&executable),
            vec![
                "machine".to_owned(),
                "create".to_owned(),
                "--name".to_owned(),
                "ci-01".to_owned(),
                "--image".to_owned(),
                "ghcr.io/acme/runner:latest".to_owned(),
                "--cpus".to_owned(),
                "4".to_owned(),
                "--mem".to_owned(),
                "512".to_owned(),
                "--storage".to_owned(),
                "20".to_owned(),
                "--overlay".to_owned(),
                "30".to_owned(),
                "--allow-host".to_owned(),
                "example.com".to_owned(),
                "--allow-host".to_owned(),
                "api.example.com".to_owned(),
                "--allow-cidr".to_owned(),
                "10.0.0.0/8".to_owned(),
                "--allow-cidr".to_owned(),
                "2001:db8::/32".to_owned(),
                "--dns".to_owned(),
                "192.168.1.1".to_owned(),
                "--volume".to_owned(),
                format!("{}:/workspace", host_rw.display()),
                "--volume".to_owned(),
                format!("{}:/cache:ro", host_ro.display()),
                "--mount-socket".to_owned(),
                format!("{}:/run/preloop-engine.sock", host_socket.display()),
            ]
        );
    }

    #[tokio::test]
    async fn create_with_socket_mount_rejects_smolvm_without_the_flag() {
        let (directory, executable) = fake_old_smolvm();
        let host_socket = directory.path().join("engine.sock");
        let _socket = std::os::unix::net::UnixListener::bind(&host_socket).unwrap();
        let mut spec = valid_spec(MachineName::new("ci-01").unwrap());
        spec.sockets.push(SocketMount {
            host: host_socket,
            guest: PathBuf::from("/run/preloop-engine.sock"),
        });

        let error = SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap_err();

        assert!(
            matches!(error, VmError::UnsupportedSocketMount { .. }),
            "expected UnsupportedSocketMount, got {error}"
        );
        assert!(
            !executable.with_extension("args").exists()
                || !fs::read_to_string(executable.with_extension("args"))
                    .unwrap()
                    .lines()
                    .any(|arg| arg == "--mount-socket"),
            "create must not run when the binary lacks --mount-socket"
        );
    }

    #[tokio::test]
    async fn create_rejects_invalid_specs_before_launching_smolvm() {
        type SpecMutation = fn(&mut MachineSpec);
        let cases: [(&str, SpecMutation); 4] = [
            ("empty image", |spec: &mut MachineSpec| {
                spec.image = " ".to_owned()
            }),
            ("zero cpus", |spec: &mut MachineSpec| spec.cpus = 0),
            ("too little memory", |spec: &mut MachineSpec| {
                spec.memory_mib = 127
            }),
            ("zero storage", |spec: &mut MachineSpec| {
                spec.storage_gib = 0
            }),
        ];

        for (label, mutate) in cases {
            let (_directory, executable) = fake_smolvm();
            let mut spec = valid_spec(MachineName::new("valid").unwrap());
            mutate(&mut spec);
            let error = SmolVmProvider::new(&executable)
                .create(&spec)
                .await
                .unwrap_err();
            assert!(matches!(error, VmError::InvalidSpec(_)), "{label}: {error}");
            assert!(
                !executable.with_extension("args").exists(),
                "{label} launched SmolVM"
            );
        }

        let (_directory, executable) = fake_smolvm();
        let mut spec = valid_spec(MachineName::new("valid").unwrap());
        spec.volumes.push(VolumeMount {
            host: PathBuf::from("relative/source"),
            guest: PathBuf::from("/guest"),
            read_only: false,
        });
        assert!(matches!(
            SmolVmProvider::new(&executable).create(&spec).await,
            Err(VmError::InvalidSpec(message)) if message == "volume paths must be absolute"
        ));
        assert!(!executable.with_extension("args").exists());

        let (directory, executable) = fake_smolvm();
        let mut spec = valid_spec(MachineName::new("valid").unwrap());
        spec.volumes.push(VolumeMount {
            host: directory.path().join("missing"),
            guest: PathBuf::from("/guest"),
            read_only: false,
        });
        assert!(matches!(
            SmolVmProvider::new(&executable).create(&spec).await,
            Err(VmError::InvalidSpec(message)) if message.starts_with("volume source does not exist:")
        ));
        assert!(!executable.with_extension("args").exists());

        let (_directory, executable) = fake_smolvm();
        let mut spec = valid_spec(MachineName::new("valid").unwrap());
        spec.sockets.push(SocketMount {
            host: PathBuf::from("relative/socket"),
            guest: PathBuf::from("/run/engine.sock"),
        });
        assert!(matches!(
            SmolVmProvider::new(&executable).create(&spec).await,
            Err(VmError::InvalidSpec(message)) if message == "socket paths must be absolute"
        ));
        assert!(!executable.with_extension("args").exists());

        let (directory, executable) = fake_smolvm();
        let host_socket = directory.path().join("engine.sock");
        fs::write(&host_socket, b"socket").unwrap();
        let mut spec = valid_spec(MachineName::new("valid").unwrap());
        spec.sockets.push(SocketMount {
            host: host_socket,
            guest: PathBuf::from("relative/guest.sock"),
        });
        assert!(matches!(
            SmolVmProvider::new(&executable).create(&spec).await,
            Err(VmError::InvalidSpec(message)) if message == "socket paths must be absolute"
        ));
        assert!(!executable.with_extension("args").exists());

        let (directory, executable) = fake_smolvm();
        let mut spec = valid_spec(MachineName::new("valid").unwrap());
        spec.sockets.push(SocketMount {
            host: directory.path().join("missing.sock"),
            guest: PathBuf::from("/run/engine.sock"),
        });
        assert!(matches!(
            SmolVmProvider::new(&executable).create(&spec).await,
            Err(VmError::InvalidSpec(message)) if message.starts_with("socket source does not exist:")
        ));
        assert!(!executable.with_extension("args").exists());

        // A socket mount is a hole in the guest boundary: an ordinary file that
        // merely exists must not be accepted in place of a real endpoint.
        let (directory, executable) = fake_smolvm();
        let regular_file = directory.path().join("not-a-socket");
        fs::write(&regular_file, b"socket").unwrap();
        let mut spec = valid_spec(MachineName::new("valid").unwrap());
        spec.sockets.push(SocketMount {
            host: regular_file,
            guest: PathBuf::from("/run/engine.sock"),
        });
        assert!(matches!(
            SmolVmProvider::new(&executable).create(&spec).await,
            Err(VmError::InvalidSpec(message)) if message.starts_with("socket source is not a Unix socket:")
        ));
        assert!(!executable.with_extension("args").exists());

        // Nor may the named path be a symlink that could be repointed at a
        // privileged endpoint (for example a container runtime socket).
        let (directory, executable) = fake_smolvm();
        let real_socket = directory.path().join("real.sock");
        let link = directory.path().join("link.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&real_socket).unwrap();
        std::os::unix::fs::symlink(&real_socket, &link).unwrap();
        let mut spec = valid_spec(MachineName::new("valid").unwrap());
        spec.sockets.push(SocketMount {
            host: link,
            guest: PathBuf::from("/run/engine.sock"),
        });
        assert!(matches!(
            SmolVmProvider::new(&executable).create(&spec).await,
            Err(VmError::InvalidSpec(message)) if message.starts_with("socket source must not be a symlink:")
        ));
        assert!(!executable.with_extension("args").exists());
    }

    #[tokio::test]
    async fn exec_bounds_both_streams_and_drains_the_child_to_completion() {
        let (_directory, executable) = fake_smolvm();
        let provider = SmolVmProvider::new(&executable).with_capture_limit(1024);
        let name = MachineName::new("runner").unwrap();

        let output: ExecOutput = provider
            .exec(&name, &["large-output".to_owned()])
            .await
            .unwrap();

        assert_eq!(output.exit_code, 0);
        assert_eq!(output.stdout.len(), 1024);
        assert_eq!(output.stderr.len(), 1024);
        assert!(output.stdout.iter().all(|byte| *byte == b' '));
        assert!(output.stderr.iter().all(|byte| *byte == b' '));
        assert!(output.truncated);
    }

    #[tokio::test]
    async fn status_maps_running_stopped_missing_and_unknown_states() {
        let (_directory, executable) = fake_smolvm();
        let provider = SmolVmProvider::new(&executable);
        for (name, expected) in [
            ("running", MachineState::Running),
            ("stopped", MachineState::Stopped),
            ("missing", MachineState::Missing),
            ("paused", MachineState::Unknown),
        ] {
            assert_eq!(
                provider
                    .status(&MachineName::new(name).unwrap())
                    .await
                    .unwrap(),
                expected,
                "status for {name}"
            );
        }
    }

    #[tokio::test]
    async fn list_parses_json_machine_names_and_ignores_entries_without_names() {
        let (_directory, executable) = fake_smolvm();
        let machines = SmolVmProvider::new(&executable).list().await.unwrap();
        assert_eq!(
            machines,
            vec![
                MachineName::new("alpha").unwrap(),
                MachineName::new("beta-2").unwrap()
            ]
        );
    }

    #[tokio::test]
    async fn pack_requires_absolute_output_and_emits_exact_arguments() {
        let (directory, executable) = fake_smolvm();
        let provider = SmolVmProvider::new(&executable);
        let name = MachineName::new("runner").unwrap();
        let output = directory.path().join("runner.smolmachine");

        provider.pack(&name, &output).await.unwrap();

        // The `.smolmachine` extension is stripped: smolvm 1.7.2 writes the
        // packed data as `<output>.smolmachine` beside the ELF stub at
        // `<output>` and rejects an explicit `.smolmachine` output name.
        assert_eq!(
            captured_args(&executable),
            vec![
                "pack".to_owned(),
                "create".to_owned(),
                "--from-vm".to_owned(),
                "runner".to_owned(),
                // Large layer staging follows the output, not smolvm's cache
                // dir on the root filesystem.
                "--staging-dir".to_owned(),
                directory.path().display().to_string(),
                "-o".to_owned(),
                directory.path().join("runner").display().to_string(),
            ]
        );
        assert_eq!(
            captured_tmpdir(&executable),
            format!("TMPDIR={}", output.parent().unwrap().display())
        );

        let (_directory, relative_executable) = fake_smolvm();
        let error = SmolVmProvider::new(&relative_executable)
            .pack(&name, Path::new("relative.smolmachine"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, VmError::InvalidSpec(message) if message == "pack output path must be absolute")
        );
        assert!(!relative_executable.with_extension("args").exists());
    }

    #[tokio::test]
    async fn pack_forwards_proxy_configuration_to_export_vm() {
        let (directory, executable) = fake_smolvm();
        let provider = SmolVmProvider::new(&executable).with_pack_network(
            Some("http://192.168.1.10:18080".to_owned()),
            Some("localhost,127.0.0.1,.internal".to_owned()),
        );
        let name = MachineName::new("runner").unwrap();
        let output = directory.path().join("runner");

        provider.pack(&name, &output).await.unwrap();

        assert_eq!(
            captured_args(&executable),
            vec![
                "pack".to_owned(),
                "create".to_owned(),
                "--from-vm".to_owned(),
                "runner".to_owned(),
                "--proxy".to_owned(),
                "http://192.168.1.10:18080".to_owned(),
                "--no-proxy".to_owned(),
                "localhost,127.0.0.1,.internal".to_owned(),
                "--staging-dir".to_owned(),
                directory.path().display().to_string(),
                "-o".to_owned(),
                output.display().to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn provider_environment_prefers_preloop_pack_proxy() {
        let _env_guard = TEST_ENV_MUTEX.lock().await;
        let (directory, executable) = fake_smolvm();
        let saved = [
            (
                "PRELOOP_RUNNER_PACK_PROXY",
                std::env::var_os("PRELOOP_RUNNER_PACK_PROXY"),
            ),
            (
                "PRELOOP_RUNNER_PACK_NO_PROXY",
                std::env::var_os("PRELOOP_RUNNER_PACK_NO_PROXY"),
            ),
            ("HTTPS_PROXY", std::env::var_os("HTTPS_PROXY")),
            ("NO_PROXY", std::env::var_os("NO_PROXY")),
        ];
        unsafe {
            std::env::set_var("PRELOOP_RUNNER_PACK_PROXY", "http://preloop.proxy:8080");
            std::env::set_var("PRELOOP_RUNNER_PACK_NO_PROXY", ".preloop.internal");
            std::env::set_var("HTTPS_PROXY", "http://standard.proxy:8080");
            std::env::set_var("NO_PROXY", ".standard.internal");
        }

        let provider = SmolVmProvider::from_environment(&executable);
        let name = MachineName::new("runner").unwrap();
        let output = directory.path().join("runner");
        provider.pack(&name, &output).await.unwrap();
        let args = captured_args(&executable);

        for (name, value) in saved {
            unsafe {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
        assert!(
            args.windows(2)
                .any(|pair| { pair == ["--proxy", "http://preloop.proxy:8080"] })
        );
        assert!(
            args.windows(2)
                .any(|pair| { pair == ["--no-proxy", ".preloop.internal"] })
        );
        assert!(!args.iter().any(|arg| arg.contains("standard.proxy")));
        assert!(!args.iter().any(|arg| arg.contains("standard.internal")));
    }

    #[tokio::test]
    async fn exec_preserves_shell_metacharacters_as_one_guest_argument() {
        let (directory, executable) = fake_smolvm();
        let marker = directory.path().join("must-not-be-created");
        let hostile = format!("$(touch {}); echo $HOME `uname`", marker.display());
        let name = MachineName::new("runner").unwrap();

        let output = SmolVmProvider::new(&executable)
            .exec(&name, &["echo".to_owned(), hostile.clone()])
            .await
            .unwrap();

        assert_eq!(output.stdout, b"ok\n");
        assert!(
            !marker.exists(),
            "guest argument was interpreted by a shell"
        );
        assert_eq!(
            captured_args(&executable),
            vec![
                "machine".to_owned(),
                "exec".to_owned(),
                "--name".to_owned(),
                "runner".to_owned(),
                "--timeout".to_owned(),
                "30m".to_owned(),
                "--".to_owned(),
                "echo".to_owned(),
                hostile,
            ]
        );
    }

    #[tokio::test]
    async fn public_only_selects_virtio_net_and_sets_smolvm_egress_floor_strict() {
        let (_directory, executable) = fake_smolvm();
        let provider = SmolVmProvider::new(executable.clone());
        let mut spec = valid_spec(MachineName::new("test-floor").unwrap());
        spec.network = NetworkPolicy::PublicOnly;
        provider.create(&spec).await.unwrap();
        assert!(
            captured_args(&executable)
                .windows(2)
                .any(|args| args == ["--net-backend", "virtio-net"])
        );
        assert_eq!(captured_env(&executable), "SMOLVM_EGRESS_FLOOR=strict");
    }

    /// The `PRELOOP_SMOLVM_NET_BACKEND` override must reach the spawned
    /// command as `--net-backend tsi` — the mapping function alone is not
    /// the contract, the wiring through `create` is. Driven through the
    /// `with_env_lookup` seam so no process-global environment is mutated.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn public_only_net_backend_override_reaches_the_spawned_command() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("smolvm");
        fs::write(
            &executable,
            r##"#!/bin/sh
for arg in "$@"; do printf '%s\n' "$arg" >> "$0.args"; done
"##,
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();

        let provider = SmolVmProvider::new(executable.clone())
            .with_env_lookup(|key| (key == "PRELOOP_SMOLVM_NET_BACKEND").then(|| "tsi".to_owned()));
        let mut spec = valid_spec(MachineName::new("test-tsi").unwrap());
        spec.network = NetworkPolicy::PublicOnly;
        provider.create(&spec).await.unwrap();
        let args = captured_args(&executable);
        assert!(
            args.windows(2).any(|pair| pair == ["--net-backend", "tsi"]),
            "the override must reach create's command line: {args:?}"
        );
    }

    /// The official runner pack's layout is known, so a runtime machine from
    /// it opts into the guest-local runtime state directories and the systemd
    /// workload; both are what the guest's docker needs to run overlay2 under
    /// the systemd cgroup driver.
    #[tokio::test]
    async fn official_pack_machines_get_guest_volumes_and_the_systemd_workload() {
        let (directory, executable) = fake_smolvm();
        let pack = directory
            .path()
            .join("preloop-ubuntu-24.04-x86_64.smolmachine");
        fs::write(&pack, b"pack").unwrap();
        let mut spec = valid_spec(MachineName::new("runner-0").unwrap());
        spec.image = pack.display().to_string();
        spec.network = NetworkPolicy::PublicOnly;
        spec.role = MachineRole::OfficialPack;
        spec.guest_volumes = None;
        spec.workload_init = None;

        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();

        let args = captured_args(&executable);
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--from", spec.image.as_str()])
                && args
                    .windows(2)
                    .any(|pair| pair == ["--volume", "/var/lib/docker"])
                && args
                    .windows(2)
                    .any(|pair| pair == ["--volume", "/var/lib/containerd"]),
            "{args:?}"
        );
        // The workload block is last: everything after `--` is the workload.
        let separator = args
            .iter()
            .position(|arg| arg == "--")
            .expect("a workload block");
        assert_eq!(&args[separator + 1..], ["/sbin/init"], "{args:?}");
    }

    #[tokio::test]
    async fn unrestricted_systemd_pack_uses_the_tsi_stub_without_a_backend_override() {
        let (directory, executable) = fake_smolvm();
        let pack = directory.path().join("official.smolmachine");
        fs::write(&pack, b"pack").unwrap();
        let mut spec = valid_spec(MachineName::new("unrestricted-systemd").unwrap());
        spec.image = pack.display().to_string();
        spec.role = MachineRole::OfficialPack;
        spec.network = NetworkPolicy::Unrestricted;
        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();
        let args = captured_args(&executable);
        assert!(!args.iter().any(|arg| arg == "--net-backend"));
        assert!(
            args.windows(2).any(|pair| pair == ["--dns", "127.0.0.53"]),
            "{args:?}"
        );
    }

    /// An arbitrary workload override does not make the guest a systemd host.
    /// A shell that never starts `systemd-resolved` must keep the resolver
    /// smolvm would configure instead of being pointed at a stub nothing
    /// answers on.
    #[tokio::test]
    async fn a_custom_workload_without_systemd_gets_no_stub_resolver() {
        let (directory, executable) = fake_smolvm();
        let pack = directory.path().join("acme.smolmachine");
        fs::write(&pack, b"pack").unwrap();
        let mut spec = valid_spec(MachineName::new("acme-shell").unwrap());
        spec.image = pack.display().to_string();
        spec.role = MachineRole::CustomPack;
        spec.network = NetworkPolicy::Unrestricted;
        spec.workload_init = Some(vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "sleep infinity".to_owned(),
        ]);

        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();

        let args = captured_args(&executable);
        assert!(
            args.windows(2).any(|pair| pair == ["--", "/bin/sh"]),
            "the explicit workload must still be passed: {args:?}"
        );
        assert!(
            !args.iter().any(|arg| arg == "--dns"),
            "a non-systemd workload has nothing answering on the stub: {args:?}"
        );
    }

    /// A custom pack is the operator's data. Its `/var/lib/docker` may hold
    /// the preloaded images the pack exists for, so no default guest-local
    /// volume may be mounted over it, and its own entrypoint/cmd is what runs
    /// unless the operator names a workload. Only explicit opt-ins (the spec
    /// or the host override) add either.
    #[tokio::test]
    async fn custom_pack_machines_keep_their_packed_container_state() {
        let (directory, executable) = fake_smolvm();
        let pack = directory.path().join("acme.smolmachine");
        fs::write(&pack, b"pack").unwrap();
        let mut spec = valid_spec(MachineName::new("acme-golden").unwrap());
        spec.image = pack.display().to_string();
        spec.network = NetworkPolicy::PublicOnly;
        spec.role = MachineRole::CustomPack;
        spec.guest_volumes = None;
        spec.workload_init = None;

        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();

        let args = captured_args(&executable);
        assert!(
            !args.iter().any(|arg| arg == "--volume"),
            "a custom pack must not get default guest volumes: {args:?}"
        );
        assert!(
            !args.iter().any(|arg| arg == "--"),
            "a custom pack keeps its own entrypoint/cmd: {args:?}"
        );

        // An explicit list is the opt-in for a custom pack that wants the
        // guest-local layout anyway.
        let (directory, executable) = fake_smolvm();
        let pack = directory.path().join("acme.smolmachine");
        fs::write(&pack, b"pack").unwrap();
        let mut spec = valid_spec(MachineName::new("acme-golden").unwrap());
        spec.image = pack.display().to_string();
        spec.role = MachineRole::CustomPack;
        spec.guest_volumes = Some(vec![GuestVolume::new("/srv/state").unwrap()]);
        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();
        let args = captured_args(&executable);
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--volume", "/srv/state"]),
            "{args:?}"
        );
        assert!(
            !args
                .windows(2)
                .any(|pair| pair == ["--volume", "/var/lib/docker"]),
            "the explicit list replaces the default, it does not add to it: {args:?}"
        );
    }

    /// A pack builder's root filesystem is about to be exported. Container
    /// state on a guest-local volume lives on the storage disk, which
    /// `pack create` does not capture, so the builder must never receive the
    /// default mounts (or a workload that rewrites the state it exports).
    #[tokio::test]
    async fn pack_builders_do_not_obscure_the_state_they_export() {
        let (directory, executable) = fake_smolvm();
        let pack = directory.path().join("acme.smolmachine");
        fs::write(&pack, b"pack").unwrap();
        let mut spec = valid_spec(MachineName::new("acme-builder").unwrap());
        spec.image = pack.display().to_string();
        spec.role = MachineRole::Builder;
        spec.guest_volumes = None;
        spec.workload_init = None;
        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();

        let args = captured_args(&executable);
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--from", spec.image.as_str()]),
            "{args:?}"
        );
        assert!(
            !args.iter().any(|arg| arg == "--volume"),
            "a builder must keep its source container state visible: {args:?}"
        );
        assert!(
            !args.iter().any(|arg| arg == "--"),
            "a builder keeps the pack's own workload: {args:?}"
        );
    }

    /// Explicit spec values are the caller's deliberate choice: they are
    /// honoured on any role, and `Some([])` is the explicit disable rather
    /// than "use the default".
    #[tokio::test]
    async fn explicit_guest_volumes_disable_or_name_the_list_on_any_role() {
        let (directory, executable) = fake_smolvm();
        let pack = directory.path().join("official.smolmachine");
        fs::write(&pack, b"pack").unwrap();
        let mut spec = valid_spec(MachineName::new("runner-0").unwrap());
        spec.image = pack.display().to_string();
        spec.role = MachineRole::OfficialPack;
        spec.guest_volumes = Some(Vec::new());
        spec.workload_init = Some(Vec::new());
        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();
        let args = captured_args(&executable);
        assert!(!args.iter().any(|arg| arg == "--volume"), "{args:?}");
        assert!(!args.iter().any(|arg| arg == "--"), "{args:?}");

        let (_directory, executable) = fake_smolvm();
        let mut spec = valid_spec(MachineName::new("runner-0").unwrap());
        spec.guest_volumes = Some(vec![GuestVolume::new("/srv/state").unwrap()]);
        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();
        let args = captured_args(&executable);
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--volume", "/srv/state"]),
            "{args:?}"
        );
        assert!(
            !args
                .windows(2)
                .any(|pair| pair == ["--volume", "/var/lib/docker"]),
            "an explicit list replaces the built-in default: {args:?}"
        );

        // A custom pack likewise keeps its own entrypoint unless the spec
        // names a workload, and then exactly that command runs.
        let (directory, executable) = fake_smolvm();
        let pack = directory.path().join("acme.smolmachine");
        fs::write(&pack, b"pack").unwrap();
        let mut spec = valid_spec(MachineName::new("acme-golden").unwrap());
        spec.image = pack.display().to_string();
        spec.role = MachineRole::CustomPack;
        spec.workload_init = Some(vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "sleep 1".to_owned(),
        ]);
        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();
        let args = captured_args(&executable);
        let separator = args
            .iter()
            .position(|arg| arg == "--")
            .expect("the explicit workload must be passed");
        assert_eq!(
            &args[separator + 1..],
            ["/bin/sh", "-c", "sleep 1"],
            "{args:?}"
        );
    }

    /// The pack workload override is pack-only: an image machine still boots
    /// its own entrypoint/keep-alive and never gains a trailing `--` block.
    #[tokio::test]
    async fn image_machines_ignore_the_pack_workload_override() {
        let (_directory, executable) = fake_smolvm();
        let mut spec = valid_spec(MachineName::new("builder-0").unwrap());
        spec.workload_init = Some(vec!["/sbin/init".to_owned()]);
        // An explicit guest volume is honoured wherever it is asked for; the
        // role only gates the built-in default set.
        spec.guest_volumes = Some(vec![GuestVolume::new("/var/lib/docker").unwrap()]);

        SmolVmProvider::new(&executable)
            .create(&spec)
            .await
            .unwrap();

        let args = captured_args(&executable);
        assert!(!args.iter().any(|arg| arg == "--"), "{args:?}");
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--volume", "/var/lib/docker"]),
            "{args:?}"
        );
    }

    #[tokio::test]
    async fn unrestricted_removes_smolvm_egress_floor() {
        let (_directory, executable) = fake_smolvm();
        let provider = SmolVmProvider::new(executable.clone());
        let mut spec = valid_spec(MachineName::new("test-unrestricted").unwrap());
        spec.network = NetworkPolicy::Unrestricted;
        provider.create(&spec).await.unwrap();
        assert_eq!(captured_env(&executable), "SMOLVM_EGRESS_FLOOR=");
    }

    #[tokio::test]
    async fn rosetta_update_failure_is_returned_and_partial_machine_is_deleted() {
        let (_directory, executable) = fake_smolvm();
        fs::write(executable.with_extension("fail-update"), "").unwrap();
        let provider = SmolVmProvider::new(executable.clone());
        let mut spec = valid_spec(MachineName::new("test-rosetta").unwrap());
        spec.rosetta = true;

        let error = provider.create(&spec).await.unwrap_err();

        assert!(matches!(
            error,
            VmError::Command {
                operation: "update",
                exit_code: 42,
                ..
            }
        ));
        assert_eq!(
            captured_args(&executable),
            ["machine", "delete", "--name", "test-rosetta", "-f"]
        );
    }

    /// A smolvm whose `machine fork` records entry and exit around a barrier,
    /// so a test can observe whether two forks are in flight at once.
    fn fake_blocking_fork_smolvm() -> (TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("smolvm");
        write_executable(
            &executable,
            r##"#!/bin/sh
set -eu
if [ "${1-}:${2-}" != "machine:fork" ]; then
  exit 0
fi
printf 'enter %s\n' "$6" >> "$0.forklog"
while [ ! -f "$0.release" ]; do sleep 0.02; done
printf 'exit %s\n' "$6" >> "$0.forklog"
"##,
        );
        (directory, executable)
    }

    fn fork_log(executable: &Path) -> Vec<String> {
        fs::read_to_string(executable.with_extension("forklog"))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    async fn wait_for_entries(executable: &Path, count: usize) -> bool {
        for _ in 0..250 {
            if fork_log(executable)
                .iter()
                .filter(|line| line.starts_with("enter"))
                .count()
                >= count
            {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        false
    }

    #[tokio::test]
    async fn retained_checkpoint_mode_allows_multiple_live_clones() {
        let (_directory, executable) = fake_blocking_fork_smolvm();
        // Forks are serialized per golden while multiple live clones remain
        // supported by the configured SmolVM runtime.
        let provider = SmolVmProvider::new(&executable);
        let golden = MachineName::new("runner-golden").unwrap();
        let first = MachineName::new("runner-0-1").unwrap();
        let second = MachineName::new("runner-1-1").unwrap();

        let one = {
            let provider = provider.clone();
            let (golden, first) = (golden.clone(), first.clone());
            tokio::spawn(async move { provider.fork(&golden, &first).await })
        };
        let two = {
            let provider = provider.clone();
            let (golden, second) = (golden.clone(), second.clone());
            tokio::spawn(async move { provider.fork(&golden, &second).await })
        };

        assert!(wait_for_entries(&executable, 1).await);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            fork_log(&executable)
                .iter()
                .filter(|line| line.starts_with("enter"))
                .count(),
            1,
            "the provider still serializes fork commands: {:?}",
            fork_log(&executable)
        );
        fs::write(executable.with_extension("release"), "").unwrap();
        one.await.unwrap().unwrap();
        two.await.unwrap().unwrap();

        assert_eq!(
            fork_log(&executable),
            [
                "enter runner-0-1",
                "exit runner-0-1",
                "enter runner-1-1",
                "exit runner-1-1"
            ]
        );
    }

    /// Serialization is per golden, not global: independent base images must
    /// still refill in parallel, which is the whole point of a fork pool.
    #[tokio::test]
    async fn forks_from_different_goldens_still_overlap() {
        let (_directory, executable) = fake_blocking_fork_smolvm();
        let provider = SmolVmProvider::new(&executable);
        let first_golden = MachineName::new("runner-golden-a").unwrap();
        let second_golden = MachineName::new("runner-golden-b").unwrap();

        let one = {
            let provider = provider.clone();
            let clone = MachineName::new("runner-0-1").unwrap();
            tokio::spawn(async move { provider.fork(&first_golden, &clone).await })
        };
        let two = {
            let provider = provider.clone();
            let clone = MachineName::new("runner-1-1").unwrap();
            tokio::spawn(async move { provider.fork(&second_golden, &clone).await })
        };

        assert!(
            wait_for_entries(&executable, 2).await,
            "both forks must be in flight: {:?}",
            fork_log(&executable)
        );

        fs::write(executable.with_extension("release"), "").unwrap();
        one.await.unwrap().unwrap();
        two.await.unwrap().unwrap();
    }

    #[cfg(target_os = "linux")]
    fn assert_sandbox_defaults(executable: &Path) {
        assert_eq!(
            captured_sandbox_var(executable, "seccomp"),
            "SMOLVM_SECCOMP=enforce",
            "every operation must default seccomp to enforce"
        );
        assert_eq!(
            captured_sandbox_var(executable, "landlock"),
            "SMOLVM_LANDLOCK=enforce",
            "every operation must default Landlock to enforce"
        );
    }

    /// The sandbox environment must reach every operation that can spawn or
    /// restart `_boot-vm` — not just machine creation — because a stopped
    /// machine's `start`, a fork clone's boot, and a pack all re-boot a VMM
    /// from the CLI process's environment. Override precedence and the
    /// validation of a pre-set mode are unit-tested against the policy
    /// itself (`sandbox_env_from`), which needs no process-global state.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn sandbox_enforce_reaches_every_operation() {
        let name = MachineName::new("runner").unwrap();

        let (_directory, executable) = fake_smolvm();
        SmolVmProvider::new(executable.clone())
            .create(&valid_spec(name.clone()))
            .await
            .unwrap();
        assert_sandbox_defaults(&executable);

        let (_directory, executable) = fake_smolvm();
        SmolVmProvider::new(executable.clone())
            .start(&name)
            .await
            .unwrap();
        assert_sandbox_defaults(&executable);

        let (_directory, executable) = fake_smolvm();
        SmolVmProvider::new(executable.clone())
            .start_forkable(&name)
            .await
            .unwrap();
        assert_sandbox_defaults(&executable);

        let (_directory, executable) = fake_smolvm();
        SmolVmProvider::new(executable.clone())
            .fork(&name, &MachineName::new("clone").unwrap())
            .await
            .unwrap();
        assert_sandbox_defaults(&executable);

        let (_directory, executable) = fake_smolvm();
        SmolVmProvider::new(executable.clone())
            .exec(&name, &["echo".to_owned()])
            .await
            .unwrap();
        assert_sandbox_defaults(&executable);

        let (_directory, executable) = fake_smolvm();
        let (sender, _receiver) = tokio::sync::mpsc::channel(16);
        SmolVmProvider::new(executable.clone())
            .exec_stream(&name, &["echo".to_owned()], sender)
            .await
            .unwrap();
        assert_sandbox_defaults(&executable);

        let (directory, executable) = fake_smolvm();
        let output = directory.path().join("packed");
        SmolVmProvider::new(executable.clone())
            .pack(&name, &output)
            .await
            .unwrap();
        assert_sandbox_defaults(&executable);
    }

    /// The cgroup root is handed to `_boot-vm` only when this process really
    /// sits in a usable cgroup v2 delegation. A test process never calls
    /// `init_vm_cgroup_delegation`, so it takes the read-only path: the
    /// variable is absent unless a delegation already exists, and when it is
    /// present it must name this process's own cgroup directory — the
    /// `/proc/self/cgroup` path appended to `/sys/fs/cgroup` verbatim,
    /// systemd `\xHH` escapes included.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cgroup_root_env_matches_actual_delegation() {
        let (_directory, executable) = fake_smolvm();
        SmolVmProvider::new(executable.clone())
            .create(&valid_spec(MachineName::new("cgroup").unwrap()))
            .await
            .unwrap();
        let captured = captured_sandbox_var(&executable, "cgroup");
        let root = captured
            .strip_prefix("SMOLVM_CGROUP_ROOT=")
            .expect("the fake smolvm always records the variable");
        if root.is_empty() {
            return;
        }
        let proc_cgroup = fs::read_to_string("/proc/self/cgroup").unwrap();
        let expected = proc_cgroup
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .expect("cgroup v2 entry")
            .trim()
            .trim_start_matches('/');
        // Compared as paths: the root cgroup prints `/`, whose join leaves a
        // trailing separator that a string comparison would trip over.
        assert_eq!(
            PathBuf::from(root),
            Path::new("/sys/fs/cgroup").join(expected)
        );
    }

    /// On non-Linux hosts seccomp and Landlock are no-ops in SmolVM; Preloop
    /// must not inject them (or a cgroup root that cannot exist), preserving
    /// macOS behavior.
    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn non_linux_injects_no_sandbox_env() {
        let (_directory, executable) = fake_smolvm();
        SmolVmProvider::new(executable.clone())
            .create(&valid_spec(MachineName::new("macos").unwrap()))
            .await
            .unwrap();
        assert_eq!(
            captured_sandbox_var(&executable, "seccomp"),
            "SMOLVM_SECCOMP="
        );
        assert_eq!(
            captured_sandbox_var(&executable, "landlock"),
            "SMOLVM_LANDLOCK="
        );
        assert_eq!(
            captured_sandbox_var(&executable, "cgroup"),
            "SMOLVM_CGROUP_ROOT="
        );
    }

    /// A rootfs tree the guard can read: `sbin/init` marks it as a real
    /// rootfs, and the agent binary carries the boot banner, its version, and
    /// (when asked) the quiesced-shutdown capability.
    fn write_agent_rootfs(path: &Path, agent: &str, capability: bool) {
        fs::create_dir_all(path.join("sbin")).unwrap();
        fs::write(path.join("sbin/init"), b"#!/bin/sh\n").unwrap();
        let mut binary = Vec::from(&b"smolvm-agent started, deferred boot work in progress"[..]);
        binary.extend_from_slice(agent.as_bytes());
        if capability {
            binary.extend_from_slice(b"quiesced-shutdown-v1");
        }
        fs::create_dir_all(path.join("usr/local/bin")).unwrap();
        fs::write(path.join("usr/local/bin/smolvm-agent"), binary).unwrap();
    }

    /// A smolvm stand-in that records every invocation and reports a version,
    /// so a test can prove which commands did and did not run.
    fn recording_smolvm(directory: &Path) -> PathBuf {
        let executable = directory.join("smolvm");
        write_executable(
            &executable,
            r##"#!/bin/sh
printf '%s\n' "$*" >> "$0.invocations"
if [ "${1-}" = "--version" ]; then
  printf 'smolvm 1.30.1\n'
fi
exit 0
"##,
        );
        executable
    }

    fn invocations(executable: &Path) -> String {
        fs::read_to_string(executable.with_extension("invocations")).unwrap_or_default()
    }

    /// The failure that motivated the guard: an operator pinned a stale guest
    /// agent (or an old data-directory copy outranked the shipped one), so the
    /// host would refuse to stop every guest it booted. The provider must
    /// refuse the boot instead, name both trees, and never launch smolvm.
    #[tokio::test]
    async fn a_guest_agent_that_does_not_match_the_binary_refuses_to_boot() {
        let _env_guard = TEST_ENV_MUTEX.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let executable = recording_smolvm(directory.path());
        write_agent_rootfs(&directory.path().join("agent-rootfs"), "1.30.1", true);
        let stale = directory.path().join("stale-rootfs");
        write_agent_rootfs(&stale, "1.14.2", false);

        let saved = std::env::var_os("SMOLVM_AGENT_ROOTFS");
        unsafe { std::env::set_var("SMOLVM_AGENT_ROOTFS", &stale) };
        let result = SmolVmProvider::new(&executable)
            .create(&valid_spec(MachineName::new("runner").unwrap()))
            .await;
        unsafe {
            match saved {
                Some(value) => std::env::set_var("SMOLVM_AGENT_ROOTFS", value),
                None => std::env::remove_var("SMOLVM_AGENT_ROOTFS"),
            }
        }

        let error = result.expect_err("a mismatched guest agent must refuse to boot");
        let VmError::GuestAgentMismatch { report } = error else {
            panic!("expected the guest-agent mismatch error, got {error}");
        };
        let shipped = directory.path().join("agent-rootfs");
        for needle in [
            stale.display().to_string(),
            shipped.display().to_string(),
            "does not match host smolvm 1.30.1".to_owned(),
            "preloop update --ensure-runtime".to_owned(),
        ] {
            assert!(report.contains(&needle), "{needle:?} missing from {report}");
        }
        let launched = invocations(&executable);
        assert!(
            !launched.contains("machine"),
            "the guard must refuse before any smolvm command runs; saw: {launched}"
        );
    }

    /// The matching layout booted by the engine itself: nothing pinned, the
    /// rootfs shipped with the binary is the one the engine exports, and the
    /// boot proceeds — the guard must never false-positive here.
    #[tokio::test]
    async fn the_binaries_own_guest_agent_boots_without_a_guard_finding() {
        let _env_guard = TEST_ENV_MUTEX.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let executable = recording_smolvm(directory.path());
        let shipped = directory.path().join("agent-rootfs");
        write_agent_rootfs(&shipped, "1.30.1", true);
        let saved = std::env::var_os("SMOLVM_AGENT_ROOTFS");
        unsafe { std::env::remove_var("SMOLVM_AGENT_ROOTFS") };

        let result = SmolVmProvider::new(&executable)
            .create(&valid_spec(MachineName::new("runner").unwrap()))
            .await;
        unsafe {
            match saved {
                Some(value) => std::env::set_var("SMOLVM_AGENT_ROOTFS", value),
                None => std::env::remove_var("SMOLVM_AGENT_ROOTFS"),
            }
        }

        result.expect("the agent that ships with the binary must be accepted");
        assert!(
            invocations(&executable).contains("machine create"),
            "the boot command must have run; saw: {}",
            invocations(&executable)
        );
    }

    /// When smolvm still refuses a stop (the machine was booted before the
    /// guard existed, or its agent is stale for another reason), the surfaced
    /// error must name the rootfs Preloop pinned and the agent version that
    /// actually booted, so the failure is diagnosable without a second
    /// investigation.
    #[tokio::test]
    async fn a_refused_stop_reports_the_pinned_rootfs_and_the_guest_agent_version() {
        let _env_guard = TEST_ENV_MUTEX.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("smolvm");
        write_executable(
            &executable,
            r##"#!/bin/sh
case "${1-}:${2-}" in
  machine:data-dir) cat "$0.dataroot" ;;
  machine:stop)
    printf '%s\n' 'running guest agent does not support safe shutdown; VM left unchanged; update the guest agent before retrying' >&2
    exit 1
    ;;
esac
exit 0
"##,
        );
        let rootfs = directory.path().join("stale-rootfs");
        write_agent_rootfs(&rootfs, "1.14.2", false);
        let machine_dir = directory.path().join("machine");
        fs::create_dir_all(&machine_dir).unwrap();
        fs::write(
            machine_dir.join("agent-console.log"),
            "{\"level\":\"INFO\",\"fields\":{\"message\":\"smolvm-agent started, deferred boot work in \
             progress\",\"version\":\"1.14.2\"},\"target\":\"smolvm_agent\"}\n",
        )
        .unwrap();
        fs::write(
            executable.with_extension("dataroot"),
            format!("{}\n", machine_dir.display()),
        )
        .unwrap();

        let saved = std::env::var_os("SMOLVM_AGENT_ROOTFS");
        unsafe { std::env::set_var("SMOLVM_AGENT_ROOTFS", &rootfs) };
        let result = SmolVmProvider::new(&executable)
            .stop(&MachineName::new("runner").unwrap())
            .await;
        unsafe {
            match saved {
                Some(value) => std::env::set_var("SMOLVM_AGENT_ROOTFS", value),
                None => std::env::remove_var("SMOLVM_AGENT_ROOTFS"),
            }
        }

        let error = result.expect_err("the stop was refused");
        let message = error.to_string();
        let rootfs_path = rootfs.display().to_string();
        for needle in [
            "does not support safe shutdown",
            "guest-agent diagnostics",
            rootfs_path.as_str(),
            "1.14.2",
            "preloop update --ensure-runtime",
        ] {
            assert!(
                message.contains(needle),
                "{needle:?} missing from {message}"
            );
        }
    }
}
