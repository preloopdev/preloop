# Conformance: casey/just

Workflow-under-test: `.github/workflows/ci.yaml` at oracle head `7f4ef81bd6…`
(clone head matched the oracle checkout exactly).

| | Oracle | Local replay |
|---|---|---|
| Run | `30850375143` (github.com) | `968031b1-50c1-43ab-a10f-7359fbf701af` |
| Date | 2026-08-03 | 2026-08-06 |
| Runner | ubuntu-latest / macos-latest / windows-latest (hosted) | engine VM pool (arm64 ubuntu) |
| Outcome | all 6 jobs success | 4/6 success; macos/windows cells fail by design (see below) |

## Job matrix

| Job | Oracle | Local | Notes |
|---|---|---|---|
| lint | success | success | same steps: `echo ::add-matcher`, `cargo fmt --all -- --check`, `cargo clippy --all --all-targets`, `./bin/forbid`, `cargo check` |
| msrv | success | success | dtolnay/rust-toolchain + `cargo check` (MSRV pin) |
| pages | success | success | mdbook 0.4.52 + mdbook-linkcheck 0.7.7 installs, `generate-book`, `shellcheck www/install.sh`, `www/install.sh` build |
| test (ubuntu-latest) | success | **success** | see step detail below |
| test (macos-latest) | success | failure | expected: pool registers no macos runner ("no macos runner is registered; failing the job") |
| test (windows-latest) | success | failure | expected: pool registers no windows runner |

## test (ubuntu-latest) step detail

Oracle steps: `Runner Image Provisioner` (hosted infra), `Runner Image`,
`actions/checkout@v6`, `Swatinem/rust-cache@v2`, `cargo test --all` (1832
passed / 18 ignored).

Local steps: `actions/checkout@v6` (snapshot redirection), `Swatinem/rust-cache@v2`
(cache miss, keys `v0-rust-test-Linux-arm64-…`), `cargo test --all`
(1832 passed / 18 ignored — identical pass/fail split). The hosted
`Runner Image Provisioner`/`Runner Image` steps are absent by design (no hosted
image; toolchains are baked into the engine VM).

## Divergences found and fixed

1. **`runtime_directory` failure (just `tests/directories.rs`)**: `dirs::runtime_dir()`
   returned `None` because the step environment had no `XDG_RUNTIME_DIR` (hosted
   ubuntu-latest runs under systemd with `/run/user/<uid>`). Fixed in
   `crates/aksh-runner/src/worker/job_extension.rs` (provision `/run/user/0`,
   mode 0700, `cfg(target_os = "linux")`) and
   `crates/aksh-runner/src/worker/execution_context.rs` (host-surface contract:
   `XDG_RUNTIME_DIR=/run/user/0`; container surfaces stay clean).
2. **`env_var('USER')` failure (just `tests/functions.rs`)**: step environment
   lacked `USER`/`LOGNAME` (hosted images run steps as the runner account).
   Fixed in `execution_context.rs` host-surface contract (`USER=root`,
   `LOGNAME=root`); explicit job/step values win.

Both were environment-fidelity gaps in the runner, not just bugs. The second
replay after each fix went green for the cell.

## Environment differences (documented, not fixed)

- Architecture: local pool is arm64; hosted oracle is x64. just derives its
  `env_var_functions_unix`/arch expectations from the runtime, so this is
  self-consistent; binaries under test are built locally either way.
- macos/windows cells cannot run on the local pool; GitHub's own run was green
  on all three platforms.
- No hosted-image provisioner steps (no image to provision); the image is the
  engine VM's baked toolchain snapshot.
