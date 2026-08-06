# Conformance: gin-gonic/gin

Workflow-under-test: `.github/workflows/gin.yml`. The clone head (`34dac209`)
diverges from the oracle head (`b72ce9ba`, run `31059276243`) because the
oracle's recent runs are schedule/webhook-triggered, not push runs; the
workflow file itself was verified byte-identical between the two heads, and
the engine redirects the primary checkout to the local workspace snapshot, so
the diff is zero.

| | Oracle | Local replay |
|---|---|---|
| Run | `31059276243` (github.com) | `251a37f6-114e-4688-8ab8-7a2767fcb9d0` |
| Date | 2026-08-06 | 2026-08-06 |
| Runner | ubuntu-latest / macos-latest (hosted) | engine VM pool (arm64 ubuntu) |
| Outcome | all 21 jobs success | lint success; matrix cells failed/cancelled (below) |

## Job matrix

`gin.yml` has `lint` + a `test` matrix (os × go-version × 5 flags variants =
20 cells; 10 ubuntu + 10 macos). Oracle: all success. Local:

| Cell group | Oracle | Local |
|---|---|---|
| lint | success | success (golangci-lint, 23 linters active, same step shape) |
| test (macos-latest, all) | success | failure by design — pool registers no macos runner |
| test (ubuntu-latest, 1.25, nomsgpack) | success | **failure** — fixed, see divergence 1 |
| test (ubuntu-latest, all other cells) | success | failure/cancelled — see divergence 2 (fail-fast cancelled the rest) |

Matrix fail-fast semantics were observed working correctly: the first failing
cell cancelled the remaining in-flight sibling cells (`⊘ Cancelled`), matching
GitHub behavior.

## Divergences found

### 1. `ref: ${{ github.ref }}` checkout bypassed the snapshot redirect (FIXED)

`gin.yml` checks out with `ref: ${{ github.ref }}` (test job). The primary
checkout redirect only rewrites checkouts that are *provably* default-branch
semantics (absent inputs, or `${{ github.repository }}`), and deliberately
skips template refs. With a `ref` input present, the job fetched
`refs/heads/master` from github.com authenticated with the engine's local
token — github.com answers 401, git falls into interactive credential
prompting, and the checkout fails ("could not read Username … terminal prompts
disabled").

Fix (`crates/aksh-runner-server/src/snapshots.rs`, `runs.rs`): the redirect now
also applies to `ref: ${{ github.ref }}` when the run's own ref provably
targets the snapshot's content — `refs/heads/<default branch>`. Feature-branch,
tag, and unknown refs keep the conservative non-redirect behavior (hijacking a
workflow-controlled target would silently test the wrong revision).

### 2. Runner executes as root; GitHub executes as uid 1001 (DOCUMENTED)

gin's `TestSaveUploadedFileWithPermissionFailed` writes an uploaded file into a
read-only directory and expects EACCES. GitHub's hosted runners run steps as
the unprivileged `runner` user (uid 1001) with passwordless sudo, so the write
fails as the test expects. Our engine VMs run the runner as root, so the write
succeeds and the test fails ("An error is expected but got nil").

Sources:
- Official runner refuses root by default: `src/Misc/layoutroot/config.sh`
  (`Must not run with sudo` without `RUNNER_ALLOW_RUNASROOT`).
- Official runner image: `images/Dockerfile` creates `runner` with `--uid 1001`
  in the `sudo` and `docker` groups and runs as that user.
- actions/runner-images#10936: the `runner` user "used to execute github jobs"
  is uid 1001 on standard hosted images.
- GitHub docs (about-github-hosted-runners): Linux and macOS VMs "run using
  passwordless sudo … when you need to execute commands or install tools that
  require more privileges than the current user".

This is an environment-fidelity gap with broad blast radius (any workflow
relying on permission semantics, ownership checks, or uid-sensitive behavior).
The faithful fix is to run the runner as a non-root user in the VM pool
(provision uid-1001 user + passwordless sudo + docker group, chown the runner
root and workspace, and flip the env contract from `USER=root`/
`/run/user/0` to `USER=runner`/`/run/user/1001`). Tracked as a follow-up; the
phase continues with the divergence documented.

## Environment differences (documented, not fixed)

- Architecture: local pool is arm64; hosted oracle is x64. gin's tests are
  arch-agnostic in the cells exercised.
- macos-latest cells cannot run on the local pool.
- No hosted-image provisioner steps (the engine VM is the baked image).
- setup-go@v6 works over egress (go 1.25.12 / 1.26.5 downloaded from
  actions/go-versions) — toolchain acquisition parity confirmed.
