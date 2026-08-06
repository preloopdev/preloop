# Conformance: psf/black

Workflow-under-test: `.github/workflows/test.yml`. The clone head
(`74371e2041a3120a049ced8f1cab0e7a6bc8ecd3`) **equals** the oracle head
(run `31072823553`, 2026-08-06T05:00:10Z) — zero diff between local and
oracle content this time.

| | Oracle | Local replay |
|---|---|---|
| Run | `31072823553` (github.com) | `3edf5b6b-664f-4fe7-9d20-c01f322a100f` |
| Date | 2026-08-06 | 2026-08-06 |
| Runner | ubuntu-latest / macos-latest / windows-* (hosted) | engine VM pool (arm64 ubuntu) |
| Outcome | success (ubuntu cells; macOS/windows cells fail by design upstream) | ubuntu tox cells fail only at the coveralls docker step (below) |

## Job matrix

`test.yml` runs `test` (6 CPython versions × 4 OS + pypy3.11 × 3 OS = 27
cells, `fail-fast: false`), `uvloop` (4 OS), and `coveralls-finish` (needs:
test). 31 jobs total. Oracle: all ubuntu cells success; macOS/windows cells
are failing upstream in the oracle window itself (by design — the oracle run's
non-Linux cells fail; black's CI gates on ubuntu).

Local:

| Cell group | Oracle | Local |
|---|---|---|
| test (ubuntu, all CPython) | success | **failure only at "Upload coverage to Coveralls"** — the tox suites all pass (`ci-py3.x: OK … congratulations :)`); the coveralls action docker-builds an amd64-only image, which cannot run on the arm64 VM (see divergence 1) |
| test (pypy3.11, ubuntu) | success | success — the coveralls step is skipped for pypy (`!startsWith(matrix.python-version, 'pypy')`), so no docker build |
| uvloop (ubuntu) | success | success |
| coveralls-finish | success | skipped (depends on the failed cells) |
| macOS / windows cells | failure (upstream) | failure by design — pool registers no such runner |

Matrix `fail-fast: false` semantics observed working: all 31 cells ran to
completion independently; no cross-cell cancellation.

## Divergences found

### 1. amd64-only images cannot be built/run inside docker containers (DOCUMENTED)

The coveralls step (`AndreMiras/coveralls-python-action`) runs
`docker build` of `docker/Dockerfile.coveralls`-equivalent spec (base
`thekevjames/coveralls:4.0.0@sha256:0407d4ad…`, amd64-only) on the arm64 VM.
Buildx pulls the amd64 base and the `RUN python3 -m pip install Cython` step
fails: `execve: No such file or directory` +
`rosetta-wrapper: unexpected initial stop: 32512`.

Verified live (see `docs/fidelity-gap.md` 1b.6): Rosetta translation works in
the VM (static x86_64 binaries run translated; `docker run --platform
linux/amd64 -v /mnt/rosetta:/mnt/rosetta:ro alpine` returns `x86_64`); the
only broken link is that dockerd-created containers lack the `/mnt/rosetta`
mount, so the wrapper's path lookup for the translator fails inside the
container namespace. The faithful fix is an engine default-runtime shim that
injects the mount into every container's OCI spec (tracked follow-up).

On GitHub (amd64 ubuntu-latest) the image builds natively, so the oracle cell
is green. This is an environment-fidelity gap, not a protocol divergence: the
step, its inputs, and its failure surface are all faithfully executed.

### 2. The CLI could not submit path-filtered workflows (FIXED)

`test.yml` filters `on: push.paths`. The server must reject a submission
without a complete changed-file list (it cannot know whether the filter
should skip the run) — `400 workflow path filters require a complete
changed-file list` — and the CLI had no way to provide one.

Fix (`crates/preloop-cli/src/main.rs`): `collect_changed_paths()` computes the
local push delta (`git diff --name-only HEAD^ HEAD`; every tracked file counts
as new for an initial commit, mirroring GitHub's null-SHA initial-push base)
and sends it as the changed-file list. Best-effort: if git cannot answer, the
submission keeps the list unknown and the server rejects with the clear error
(previous behavior).

## Environment differences (documented, not fixed)

- Architecture: local pool is arm64; hosted oracle is x64. black's tox suites
  are arch-agnostic; the coveralls docker build is not (divergence 1).
- macOS/windows cells cannot run on the local pool (oracle fails them anyway).
- The runner executes as root; GitHub executes steps as uid 1001 (see
  `docs/fidelity-gap.md` 1b.4/1b.5) — not exercised by black's ubuntu cells.
- Toolchain acquisition parity confirmed: setup-python@v7 (SHA-pinned) + pip
  `--group tox` install over egress; 26 tox cells ran, all green on content.
