# 10-Repo Conformance Campaign — Final Report

**Date:** 2026-09-11 · **Branch:** `improve/runner-watch-conformance` rebased onto
`origin/main@008893a3` · **Stack:** preloop server + preloop Rust runner,
both built from the branch (incl. 10 fixes below) · **Golden:** official
`ubuntu24-arm64-runner-large` packed image · **Isolation:** smolvm microVMs,
pool of 4×(4 vCPU/16 GB); nothing on host · **Labels:** X64 + ubuntu-* +
ubuntu-slim

All 10 repos are NEW (no overlap with the 41 previously covered). Real
`.github/workflows` files, submitted as push payloads, run to completion.

## Scoreboard (final validated state)

| Repo | Workflow | Verdict |
|---|---|---|
| spf13/hugo | test.yml | **ubuntu fully green** (codegen + asciidoc via fixes #13/#16, confirmed in full workflow rerun); windows starves (expected) |
| grpc/grpc-go | testing.yml | **7/8 PASS** (vet-proto, static-checks, extras, latest, latest-1, -race, arm64); i386 red (env) |
| pytest-dev/pytest | test.yml | **`package` fully green** (fetch-depth:0 checkout + hynek build via fixes #12/#16); 15 ubuntu tox legs starved on pool serialization (no test signal); macos/windows starved (expected) |

## Rerun with both fixes (2026-09-12, incomplete)

- pytest `package` checkout now succeeds in the real workflow (fetch +
  `checkout --force`); hugo codegen panic gone (`_work/hugo/hugo` in full
  logs); jekyll keeps the identical single profiler assert (rename applied,
  didn't fix it — open).
- Both fixes unveiled the next blocker: `setup-python` 3.14.7 is broken in
  later steps (`libpython3.14.so.1.0` missing; pip works in-step) — fails
  pytest `package` and hugo asciidoc. Upstream tarball intact; the drop is
  in GITHUB_ENV→later-step propagation (suspect composite-inner env).
- The rerun died of disk exhaustion during junit (8/10 snapshotted; junit
  and testcontainers unsalvaged). Late-run verdicts are contaminated by
  600s pool starvation (grpc latest/latest-1/-race/arm show steps `pending`,
  not build failures) — scheduling casualties, not regressions.
- Targeted proofs (not the noisy rerun) remain dispositive; see
  `pytest/validation/`, `hugo/validation/`, `jekyll/validation/`,
  `nushell/` (relabel experiment).
| django/django | tests.yml | 1/4 — scripts-tests green; JS red (chrome libs); 3.14t red (missing step output); windows starved |
| laravel/framework | tests.yml | 0 passing — runner bugs fixed (verified zero), now blocked on creds + mysql flake |
| fmtlib/fmt | linux.yml | 0 executed — 1 env leg (gcc-4.9 amd64-only) + fail-fast cascade cancels 18 |
| nushell/nushell | ci.yml | 0 runnable — all jobs pinned to Incredibuild custom fleet; submit fixed |
| jekyll/jekyll | ci.yml | profiler assert fixed (single-test proof `0 failures` + Ruby 2.7 Linux green in rerun); 3.3/3.4/profile/rubocop never scheduled (pool starvation); JRuby red (locale); windows starved |
| junit-team/junit5 | ci.yml | **6 green post-fix** (Build/Linux, docs, openjdk 26/27/28, reproducibility); windows/macOS starved; CodeQL+zizmor creds; openJ9 TBD; status cascade-correct |
| testcontainers-go | ci.yml | 1 green (detect-modules); 9 skipped + 1 failed (dynamic matrix gap) |

## Bugs found & fixed (branch, all with regression tests)

1. **Expression `Index{base,key}`** (`preloop-gha-expressions`): `env[matrix.target.options]`
   rejected (`unexpected token Dot`), nushell unsubmittable. Full bracket-expression
   parsing; literal fast path preserved.
2. **Composite `working-directory` never evaluated** (runner): literal
   `${{ inputs.working-directory }}` became the spawn cwd (junit).
3. **`$/` nested-local-action fallback** (runner): workspace-local composites
   errored (junit main-build). Falls back to workspace join.
4. **Node entry-point containment by repo root** (runner): gradle `../dist/...`
   + codeql `../lib/...` legit layouts rejected; unblocked gradle + codeql init.
5. **Per-action `INPUT_*` scoping** (runner composite+node+docker): parent inputs
   leaked into nested actions; setup-gradle died on its removed `arguments`.
6. **`github.action_path` empty in nested composites** (runner): produced
   `/setup-testlens.sh` → 127 (junit). Save/restore per composite level.
7. **Services-only jobs took container PATH branch** (runner): host-PATH fallback
   skipped, first GITHUB_PATH write replaced PATH with extras-only, all later
   spawns ENOENT (laravel). Flag now requires a real job container.
8. **Empty-PATH fallback** (runner): defense + warning log.
9. **Disk-backed `/tmp` provisioning** (orchestrator): 8 GB tmpfs killed heavy
   Go builds; GitHub uses disk-backed /tmp. Verified live on fresh forks.
10. **`preloop golden-path`** (hidden CLI): exact packed-artifact path from server
    code; hand-rolled fingerprint drift caused full 9 GB rebuilds.
11. **Spawn diagnostics** (runner, kept): program/cwd-exists/PATH in the error —
    decisive in two investigations.
12. **Snapshot git transport ignored `Content-Encoding: gzip`** (server):
    git gzip-encodes large smart-HTTP POST bodies; the full-history fetch
    (thousands of want lines, ~6 KB) arrived gzipped and was piped raw to
    `git http-backend`, which died with `bad line length character` while the
    client reported `fatal: expected 'packfile'`. This was pytest's
    `fetch-depth: 0` killer. Fixed with `decode_git_request_body` + regression
    test; verified live: depth-0 checkout succeeds (17732 commits), depth-1
    still succeeds. Evidence in `pytest/validation/`.
13. **Job workspace is now `<work>/<repo>/<repo>`** (runner): the claim
    message carries no work directory, so every job landed in
    `_work/default/default` while GitHub uses `/home/runner/work/<repo>/<repo>`.
    hugo's codegen test panics unless the absolute path contains `"hugo"`.
    `setup_workspace` derives both segments from
    `contextData.github.repository` (decoded from the AzDO typed-dict wire
    form), with strict `owner/repo` validation and the historical fallback.
    Shared helper `workspace_dir_for_repository` + regression tests (plain
    and tagged shapes; fails-without verified). Verified live: hugo checkout
    lands in `_work/hugo/hugo` and the previously panicking
    `TestMethods/MethodsFromTypes` passes (`ok codegen 1.020s`). Evidence in
    `hugo/validation/`.
14. **smolvm 1.8.2 → 1.15.0 (infra)**: clone boots died code 133 (`no
    active virtual interrupt`) and rollback-resume failed (`EINVAL`), so
    every job was a ~30 GB full materialization — the disk deaths and most
    600s starvations trace here. 1.14.6 was unpairable (demands
    `krun_set_cpu_template`, absent from both the 1.8.2-era shipped libs
    and Homebrew upstream libkrun). 1.15.0 + bundled libs forks correctly
    (second job boots in ~60s, clones ~1 GB). Harness pins
    `~/.smolvm/1.15.0` via `SMOLVM_HOME_DIR`.
15. **Node externals fallback to legacy baked path** (runner): packed VMs
    never mount externals (IRQ budget) — they use the golden-baked
    `/var/lib/preloop-runner/externals`. The `/home/runner` move put the
    upward walk out of reach and every node action died (`bundled node24
    is missing`). The lookup now falls back to the legacy root when the
    walk misses (seam unit-tested incl. precedence). Proven live: jekyll
    setup-ruby runs and the profiler test passes
    (`1 tests, 5 assertions, 0 failures`).
16. **Tool cache defaults to `/opt/hostedtoolcache`** (runner): version
    tarballs bake an absolute RUNPATH there, and actions with
    `update-environment: false` (hynek's composite, pytest `package`)
    rely on the interpreter loading without `LD_LIBRARY_PATH` — true on
    hosted, false with the old `_work/_tool` default (also fails on stock
    self-hosted runners). `RUNNER_TOOL_CACHE` + `AGENT_TOOLSDIRECTORY`
    now prefer the hosted path when present and writable (our provisioning
    already creates/chmods it), else the old default. Proven live: pytest
    `package` fully green (`Using CPython ... /opt/hostedtoolcache/...`,
    sdist + wheel built). Evidence in `pytest/validation/`.

## Fidelity findings for follow-up (not fixed)

- **P1 — dynamic matrix from `needs.<job>.outputs` never expands**
  (testcontainers): literal `${{ fromJSON(...) }}` job names; 9 skips.
- **P1 — snapshot lifecycle vs queue wait**: `repository not found` when jobs
  claim ~10 min after submit.
- **P1 — builder-stop EAGAIN exits the whole server** (seen once).
- **P2 — checkout dir naming**: `_work/default/default` vs repo name breaks
  path-assuming tests (hugo codegen `MethodsFromTypes`, jekyll profiler —
  identical signature on 3 rubies).
- **Possible log-capture gap**: django 3.14t ran 3.5 min with zero step output.
- **Env gaps (golden)**: qemu-i386 (grpc), chrome system libs (django JS),
  JRuby locale (`Malformed input`, bundler), gcc-4.9 amd64 (fmt),
  EA/OpenJ9 JDK legs, Incredibuild labels (nushell).
- **Credentials (local-only box, no GitHub token)**: fake local GITHUB_TOKEN
  poisons composer auth (laravel) and fails CodeQL/zizmor (401s).

## Infra notes

SUPERSEDED by fix #14: smolvm/libkrun must be a matched pair — 1.14.6
demands `krun_set_cpu_template` (absent from 1.8.2-era libs and Homebrew
upstream libkrun alike); 1.8.2's fork path is broken. Campaign now pins
`~/.smolvm/1.15.0` (binary + bundled libs + agent-rootfs) via
`SMOLVM_HOME_DIR`, still shadowed onto PATH through `$CAMPAIGN_HOME/bin`.
- Disk: 258 stale VMs + 85 GB stale store deleted (was 99%); storage cap
  160→100 GB; APFS clones share ~97% (6 VMs ≈ 6 GB real).
- Server restart wipes run logs (state under CAMPAIGN_HOME); `run.json`
  snapshots survive — diagnose live or lose logs.
- `cargo zigbuild` needs `rust-std aarch64-unknown-linux-gnu` (vanished twice);
  kache staleness linked two stale guests — verify rlib/binary mtimes.
- mtplx server (Qwen Flash Next) ate ~50 GB; killed + `bootout`ed its system
  daemon for the campaign; restore with
  `sudo launchctl bootstrap system /Library/LaunchDaemons/com.nuraydia.mtplx.plist`.

## Evidence

Per-target `run.json` snapshots + server logs under
`benchmarks/real-world/results/conformance-10repos/`; validation captures in
`junit/validation/` (5-green run + logs) and `laravel/validation/`
(composer-auth logs). Harness: `benchmarks/real-world/conformance-10repos.sh`.
