# Clean rerun benchmark results

Date: 2026-07-27. Measurements use representative filtered workflow slices at pinned commits, not full repository matrices.

## Inputs

| Repository | Commit | Slice |
|---|---|---|
| ripgrep | `f9c05a949d1a0dc8e16dee28ca9605d38611faeb` | `rustfmt` |
| flask | `36e4a824f340fdee7ed50937ba8e7f6bc7d17f81` | `main` |
| vite | `3ac77d9dd742968961af38a5a91ed6b061ceda7d` | `lint` |
| chi | `8b258c7bb28f97a5f2a856ff7ef962578fec9215` | `test` |
| testcontainers-go | `ea854ecb16425b6e77bc19e95080213fb69a6ac9` | `detect-modules` |

Commands:

- Preloop: `target/debug/preloop run --file .bench/benchmark.yml --no-debug`
- Agent CI: `npx --yes @redwoodjs/agent-ci run --workflow .bench/benchmark.yml --no-matrix --jobs 1 --quiet --json`
- Both were measured under `/usr/bin/time -l`; host process trees were sampled every 250 ms.

Vite's slice was normalized to stop after build/lint; its original formatting step rewrote the benchmark workflow itself. Chi's slice was normalized to a literal Go 1.22 version and repository-local checkout; its original matrix expression and absolute checkout path were not portable to either local executor.

## Primary clean runs

Times are wall seconds. `pass` means the slice completed successfully; failures are retained as workload outcomes, not hidden.

| Repository | Preloop cold | Preloop warm | Agent CI cold | Agent CI warm |
|---|---:|---:|---:|---:|
| ripgrep | 57.51 pass | 11.01 pass | 20.62 pass | 14.48 pass |
| flask | 136.89 pass | 131.16 pass | 10.23 fail | 9.91 fail |
| vite | 638.62 pass | 92.45 pass | 43.07 pass | 38.71 pass |
| chi | 52.06 pass | 49.99 pass | unavailable* | unavailable* |
| testcontainers-go | 19.23 pass | 7.36 pass | 10.73 pass | 10.47 pass |

`*` Agent CI could not rerun the corrected chi slice after the local Docker socket disappeared (`/var/run/docker.sock` missing/dangling). Earlier Agent CI chi attempts failed during checkout because the unnormalized workflow used an absolute checkout path; those are not comparable results. OrbStack could not be relaunched in this session.

Agent CI Flask failures occurred in the workload's pre-commit step after setup actions passed. They are retained as slice outcomes.

## Resource measurements

| System | CLI CPU | CLI max RSS | Sampled host peak RSS |
|---|---:|---:|---:|
| Preloop | 0.00–0.15 s | 20.2–20.5 MiB | 2.8–8.9 GiB in recorded primary runs |
| Agent CI | 6.15–12.55 s | 287–397 MiB | 2.7–4.0 GiB in recorded primary runs |

Preloop CLI metrics exclude guest work by design. Sampled Preloop host RSS includes the SmolVM host process and is not guest-internal RSS; CPU is an instantaneous sampled aggregate, not CPU time. Agent CI exposes no equivalent Preloop API phase telemetry. Raw stdout/stderr/JSON artifacts are under `/tmp/preloop-agent-ci-bench/results/clean-rerun/` and summarized in `clean-rerun-results.json`.

## Passed-slice time breakdown

These are the passing primary slices where both systems completed: ripgrep, Vite, and testcontainers-go. Preloop guest phases come from timestamped runner step logs; the residual is runner/VM startup and orchestration. Agent CI job phases come from its NDJSON `step.finish` events; the residual is CLI/container startup.

### Warm runs

| Slice | System | Startup/orchestration | Setup/action phases | Build/test phases | Total |
|---|---|---:|---:|---:|---:|
| ripgrep | Preloop | 0.30 s | checkout 0.15 + Rust 10.28 | fmt 0.00 | 11.01 s |
| ripgrep | Agent CI | 7.61 s | setup 0.16 + checkout 0.16 + Rust 6.27 | fmt 0.17 | 14.48 s |
| Vite | Preloop | 0.68 s | checkout 0.33 + pnpm 2.03 + Node/cache 34.24 + install 16.50 + post-cache 28.31 | build 5.12 + lint 4.79 | 92.45 s |
| Vite | Agent CI | 24.64 s | setup 0.31 + checkout 0.22 + pnpm 1.77 + Node 0.31 + install 0.84 | build 4.67 + lint 5.73 | 38.71 s |
| testcontainers-go | Preloop | 6.78 s | checkout 0.26 + changed-files 0.10 + module selection 0.05 | no modules selected | 7.36 s |
| testcontainers-go | Agent CI | 9.78 s | setup 0.24 + checkout 0.16 + changed-files 0.10 + module selection 0.07 | no modules selected | 10.47 s |

The Vite result identifies the main loss: build plus lint are effectively equal (`9.91 s` Preloop versus `10.40 s` Agent CI), while Preloop spends about `62.8 s` in Node/cache setup and cache save. Agent CI spends more startup time, but its setup-node cache hit and dependency install are much cheaper.

### Cold-run residuals

| Slice | Preloop startup/orchestration | Agent CI startup/orchestration |
|---|---:|---:|
| ripgrep | 48.54 s | 13.42 s |
| Vite | 545.84 s | 25.78 s |
| testcontainers-go | 18.59 s | 10.02 s |

The cold Vite and ripgrep gaps are therefore primarily Preloop VM/image/runner provisioning before the guest's first logged step. They are not caused by the build or formatting commands themselves.

## Preloop API phase telemetry

For successful clean runs, API queue time was nearly the complete wall time and recorded job execution time was approximately zero in the native API response. This reflects the current forked SmolVM/job accounting boundary, not zero guest CPU work. Agent CI reports runner/job/step durations in NDJSON but has no corresponding Preloop queue API.

## Five additional clean slices

One extra warm run per repository was executed on each system:

| Repository | Preloop extra | Agent CI extra |
|---|---:|---:|
| ripgrep | 8.48 pass | 17.03 pass |
| flask | 134.74 pass | 9.88 fail |
| vite | 64.06 fail (old formatting slice) | 32.84 fail (old formatting slice) |
| chi | 1.42 fail (old matrix/version slice) | 7.66 fail (old matrix/version slice) |
| testcontainers-go | 28.01 pass | 10.74 pass |

The Vite and chi extra rows intentionally preserve the originally captured five-extra request; their failures are explained by the workflow adaptations above. The corrected slices are represented by the primary reruns where the executor was available.

## Anomalies

- A Preloop ripgrep cold attempt took 1103.37 s because Docker Hub unauthenticated image pulls hit `TOOMANYREQUESTS` repeatedly. It is excluded from the normal cold table.
- The corrected Preloop Vite cold run took 638.62 s while provisioning/building, then the warm run took 92.45 s. It passed and is not silently discarded, but should not be treated as a steady-state cold-start baseline without repeated cold isolates.

## Code verification

`just test-ci` passed after the runtime fixes: formatting, workspace clippy with `-D warnings`, and all workspace tests. Runtime smoke runs also passed for the Flask and ripgrep slices; Vite's corrected build/lint slice passed on both systems. The runner log showed bundled Node 24 at `/var/lib/preloop-runner/externals/node24/bin/node`.
