# Early Preloop versus Agent CI results

Date: 2026-07-27

This is an early, reproducible slice benchmark—not the final 80-run campaign. Each repository uses one representative workflow job; matrix workflows use the first matrix combination. Each system received one cold and one warm run.

## Results

| Repository / slice | Preloop cold | Preloop warm | Agent CI cold | Agent CI warm |
|---|---:|---:|---:|---:|
| `BurntSushi/ripgrep` / `ci.yml → rustfmt` | pass, 7.83s | pass, 8.87s | pass, 13.57s wall / 8.24s job | pass, 12.54s wall / 6.68s job |
| `pallets/flask` / `pre-commit.yaml → main` | fail, 39.85s | fail, 32.51s | fail, 7.29s wall / 2.22s job | fail, 8.09s wall / 2.78s job |
| `vitejs/vite` / `ci.yml → lint` | fail, 68.52s | fail, 35.32s | fail, 29.15s wall / 17.81s job | fail, 30.31s wall / 18.31s job |
| `go-chi/chi` / `ci.yml → test`, first matrix entry | fail, 6.31s | fail, 107.16s | fail, 16.69s wall / 11.20s job | fail, 4.82s wall / 0.36s job |
| `testcontainers/testcontainers-go` / `ci.yml → detect-modules` | pass, 1.69s | pass, 126.85s | pass, 9.24s wall / 1.18s job | pass, 8.34s wall / 0.67s job |

Repository commits and raw normalized data are in `early-results.json`. Raw Agent CI NDJSON is in `/tmp/preloop-agent-ci-bench/results/agent-ci/`; raw Preloop run output is in `/tmp/preloop-agent-ci-bench/results/preloop/`.

## Early findings

1. Both systems agree on the cold pass/fail outcome for all five selected slices.
2. Preloop's local snapshot checkout works without GitHub credentials. The successful ripgrep and testcontainers runs fetched from `http://127.0.0.1:19090/snapshots/<run-id>`.
3. Preloop's Flask failure is a guest image compatibility issue: `astral-sh/setup-uv` crashes under Node.js 18.19.1 with `ReferenceError: File is not defined`.
4. Preloop's Vite failure is the same class of issue: `pnpm/action-setup` fails in its Node action self-installer under the guest's Node.js 18 runtime.
5. Agent CI's failures are workload/platform results, not checkout-authentication failures: Flask's pre-commit command fails, Vite's formatting check fails, and chi's setup/checkout paths fail on the sampled attempts.
6. The 126.85s warm Preloop `testcontainers-go` result is anomalous relative to its 1.69s cold result. It needs lifecycle trace inspection before drawing a cache conclusion.

## Limitations

- No peak CPU, memory, disk, network, or VM-count samples were collected in this first pass.
- The comparison uses representative slices rather than complete workflow matrices.
- Preloop and Agent CI wall time include different launcher overheads; job-level timings are the closer phase comparison where available.
- The guest image currently provides Node.js 18, while several current actions assume newer Node runtimes. That gap must be fixed or explicitly normalized before a final compatibility claim.
- The full benchmark remains incomplete until phase/resource instrumentation and repeated matrix runs are collected.
