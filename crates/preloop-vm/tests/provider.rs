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
        ExecOutput, MachineSpec, MachineState, NetworkPolicy, SmolVmProvider, VmProvider,
        VolumeMount,
    };
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    fn fake_smolvm() -> (TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("smolvm");
        fs::write(
            &executable,
            r##"#!/bin/sh
set -eu

args="$0.args"
: > "$args"
for arg in "$@"; do
  printf '%s\n' "$arg" >> "$args"
done

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
    elif [ "${6-}" = "large-output" ]; then
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
        )
        .unwrap();
        let mut permissions = fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&executable, permissions).unwrap();
        (directory, executable)
    }

    fn captured_args(executable: &Path) -> Vec<String> {
        fs::read_to_string(executable.with_extension("args"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn valid_spec(name: MachineName) -> MachineSpec {
        MachineSpec {
            name,
            image: "ghcr.io/acme/runner:latest".to_owned(),
            cpus: 2,
            memory_mib: 256,
            storage_gib: 10,
            network: NetworkPolicy::Disabled,
            volumes: Vec::new(),
        }
    }

    #[tokio::test]
    async fn create_emits_exact_network_and_volume_arguments() {
        let (directory, executable) = fake_smolvm();
        let host_rw = directory.path().join("workspace");
        let host_ro = directory.path().join("cache");
        fs::create_dir_all(&host_rw).unwrap();
        fs::create_dir_all(&host_ro).unwrap();
        let spec = MachineSpec {
            name: MachineName::new("ci-01").unwrap(),
            image: "ghcr.io/acme/runner:latest".to_owned(),
            cpus: 4,
            memory_mib: 512,
            storage_gib: 20,
            network: NetworkPolicy::Restricted {
                hosts: vec!["example.com".to_owned(), "api.example.com".to_owned()],
                cidrs: vec!["10.0.0.0/8".to_owned(), "2001:db8::/32".to_owned()],
            },
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
                "--allow-host".to_owned(),
                "example.com".to_owned(),
                "--allow-host".to_owned(),
                "api.example.com".to_owned(),
                "--allow-cidr".to_owned(),
                "10.0.0.0/8".to_owned(),
                "--allow-cidr".to_owned(),
                "2001:db8::/32".to_owned(),
                "--volume".to_owned(),
                format!("{}:/workspace", host_rw.display()),
                "--volume".to_owned(),
                format!("{}:/cache:ro", host_ro.display()),
            ]
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

        assert_eq!(
            captured_args(&executable),
            vec![
                "pack".to_owned(),
                "create".to_owned(),
                "--from-vm".to_owned(),
                "runner".to_owned(),
                "-o".to_owned(),
                output.display().to_string(),
            ]
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
                "--".to_owned(),
                "echo".to_owned(),
                hostile,
            ]
        );
    }
}
