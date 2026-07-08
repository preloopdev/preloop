# Runner-vs-runner GitHub control-plane parity report

Date: 2026-07-08
Repository: `preloopdev/aksh-conformance-sample`
Control plane: GitHub
Execution invariant: one runner process per SmolVM job VM; captures were taken from VM-local `mitmdump` so host-to-VM proxy reachability was not part of the runner path.

## What changed

- Added runner-flow capture harness: `benchmarks/real-world/runner-flow-capture.sh`.
  - Captures official-runner and aksh-runner HTTP traffic against GitHub through a VM-local MITM proxy.
  - Starts each runner inside its own SmolVM instance (`bench-aksh-N`) and uses `--once`/ephemeral runner registration.
  - Writes `summary.json`, `flows.jsonl`, `jobs.json`, `run.log`, and VM runner logs under `benchmarks/real-world/results/runner-flow/<scenario>/<runner>/<timestamp>/`.
- Added runner-flow diff harness: `benchmarks/real-world/runner-flow-diff.py`.
  - Compares endpoint sequence/counts, HTTP status, JSON schemas, redacted JSON values, and non-JSON payload size/hash.
  - Normalizes volatile GUIDs, IDs, tokens, run-actions shard hosts, signed Azure blob hosts/query strings, agent names, and time fields.
- Fixed aksh-runner worker HTTP trust propagation in `crates/aksh-runner/src/client/http.rs`.
  - Before: listener/configure clients honored `--ca-bundle`, but worker-created HTTP clients passed `None` and ignored `SSL_CERT_FILE`; MITM-captured GitHub runs executed the step body but GitHub cancelled because results-service/run-service calls failed TLS.
  - After: `HttpClient::new(None)` consults non-empty `SSL_CERT_FILE`, so worker-side run-service/results-service/action-download clients trust the capture CA.
- Fixed broker session teardown in `crates/aksh-runner/src/client/broker.rs`.
  - Before: aksh sent `DELETE /session/{sessionId}`.
  - After: aksh sends `DELETE /session`, matching official-runner capture.
- Previously fixed command-file failure semantics remain in `crates/aksh-runner/src/worker/steps_runner.rs` and `crates/aksh-runner/src/worker/file_commands.rs`.
  - Malformed `$GITHUB_OUTPUT` heredoc parse errors now fail the step/job unless `continue-on-error` applies, matching official-runner behavior for the 91/92 scenarios.

## Captured scenarios

| Scenario | Official run | Official conclusion | Official ms | Official flows | Aksh run | Aksh conclusion | Aksh ms | Aksh flows | Diff report |
|---|---:|---|---:|---:|---:|---|---:|---:|---|
| `91-large-output.yml` | `28908085693` | failure | 28721 | 47 | `28908114264` | failure | 28877 | 49 | `benchmarks/real-world/results/runner-flow/91-large-output/diff.md` |
| `92-unicode-special-chars.yml` | `28908142378` | failure | 28885 | 52 | `28908170117` | failure | 28791 | 54 | `benchmarks/real-world/results/runner-flow/92-unicode-special-chars/diff.md` |
| `93-empty-null-values.yml` | `28907425125` | success | 34412 | 70 | `28908350757` | success | 29379 | 88 | `benchmarks/real-world/results/runner-flow/93-empty-null-values/diff.md` |

## Findings

### Fixed during this pass

1. Worker-side CA propagation bug.
   - Evidence before fix: aksh capture for run `28907460632` concluded `cancelled`; `vm-mitm.log` contained `Client TLS handshake failed ... unknown ca` for `results-receiver.actions.githubusercontent.com` and `run-actions-...`.
   - Evidence after fix: aksh run `28908030492` for `93-empty-null-values.yml` concluded `success` with 89 captured flows; post-session-fix rerun `28908350757` also concluded `success`.
2. Malformed command-file behavior.
   - Evidence: both official and aksh now conclude `failure` for `91-large-output.yml` and `92-unicode-special-chars.yml`.
   - This matches the official parser behavior: malformed `$GITHUB_OUTPUT` heredocs fail the step/job rather than logging a warning and continuing.
3. Broker session delete path.
   - Evidence before fix: `93-empty-null-values/diff.md` showed `DELETE /session` official `1`, aksh `0`, and `DELETE /session/{guid}` official `0`, aksh `1`.
   - Evidence after fix: current `93-empty-null-values/diff.md` shows `DELETE broker.actions.githubusercontent.com/session` official `1`, aksh `1`.

### Remaining runner deltas

These are still real runner-vs-runner deltas against GitHub, not aksh-server replay artifacts:

1. Results-service log protocol shape differs.
   - Official calls `CreateJobLogsMetadata` and `CreateStepLogsMetadata`; aksh currently skips those and directly requests signed URLs.
   - Diff evidence:
     - `93-empty-null-values/diff.md`: `CreateJobLogsMetadata` official `1`, aksh `0`; `CreateStepLogsMetadata` official `14`, aksh `0`.
     - `91-large-output/diff.md`: `CreateStepLogsMetadata` official `6`, aksh `0`.
     - `92-unicode-special-chars/diff.md`: `CreateStepLogsMetadata` official `8`, aksh `0`.
2. Step update cadence differs.
   - Official sends fewer batched `WorkflowStepsUpdate` requests; aksh sends many incremental updates.
   - Diff evidence:
     - `93-empty-null-values/diff.md`: official `1`, aksh `26` before normalization/current post-fix report still has high aksh Busy polling/update volume.
     - `91-large-output/diff.md`: official `2`, aksh `11`.
     - `92-unicode-special-chars/diff.md`: official `1`, aksh `15`.
3. Log payload sizes/hashes differ.
   - Diff reports compare non-JSON PUT payloads by byte size and SHA-256.
   - Example: `93-empty-null-values/diff.md` shows multiple step log payload size/hash mismatches and fewer aksh step log PUTs (`14` official vs `11` aksh).
4. Health/ready probes differ.
   - Official probes `broker.actions.githubusercontent.com/health`, `run.actions.githubusercontent.com/health`, `results-receiver.actions.githubusercontent.com/_ws/ingest.sock`, and `token.actions.githubusercontent.com/ready`.
   - Aksh currently does not.
5. Registration/service-connection chatter differs.
   - Official performs repeated `connectionData?connectOptions=0&lastChangeId=...` requests; aksh performs a simpler `connectionData` request.
   - Aksh downloads Node 20/24 during configure; official-runner package already carries its externals in this environment.
6. Aksh unregisters the ephemeral runner with the AzDO distributed-task delete endpoint; official-runner capture for these ephemeral runs did not show that endpoint.
   - Current diffs show an aksh-only `DELETE .../_apis/distributedtask/pools/{n}/agents/{n}?api-version=6.0-preview`.

## Current diff verdicts

- `91-large-output/diff.md`: FAIL, 16 contract differences.
  - `endpoint-sequence`: 1
  - `request-schema`: 10
  - `request-value`: 1
  - `response-value`: 4
- `92-unicode-special-chars/diff.md`: FAIL, 16 contract differences.
  - `endpoint-sequence`: 1
  - `request-schema`: 9
  - `request-value`: 2
  - `response-value`: 4
- `93-empty-null-values/diff.md`: FAIL, 22 contract differences.
  - `endpoint-sequence`: 1
  - `request-schema`: 15
  - `request-value`: 2
  - `response-value`: 4

A failing diff verdict here means protocol-flow mismatch, not necessarily workflow failure. The three captured scenario conclusions now match official-runner conclusions.

## Verification

Commands run from `/Users/bnjoroge/macos-runners`:

- `cargo test -p aksh-runner client::http --quiet` → 7 passed.
- `cargo build --release --target aarch64-unknown-linux-musl -p aksh-runner` → succeeded; existing warnings only.
- `bash benchmarks/real-world/runner-flow-capture.sh 93-empty-null-values.yml aksh 1` after CA fix → run `28908030492`, conclusion `success`.
- `bash benchmarks/real-world/runner-flow-capture.sh 91-large-output.yml both 1` → official and aksh both `failure`.
- `bash benchmarks/real-world/runner-flow-capture.sh 92-unicode-special-chars.yml both 1` → official and aksh both `failure`.
- `bash benchmarks/real-world/runner-flow-capture.sh 93-empty-null-values.yml aksh 1` after broker delete fix → run `28908350757`, conclusion `success`; diff shows `DELETE /session` count `1 | 1`.
- `python3 -m py_compile benchmarks/real-world/runner-flow-diff.py` → no output.
- `bash -n benchmarks/real-world/runner-flow-capture.sh` → no output.
- `cargo test -p aksh-runner --quiet` → 291 passed.
- `cargo test --workspace --quiet` → 440 passed.

## Next concrete runner parity work

1. Implement official-style results-service log metadata flow: `CreateJobLogsMetadata` and `CreateStepLogsMetadata` before signed URL retrieval/upload.
2. Batch/coalesce `WorkflowStepsUpdate` payloads to match official-runner cadence and shape.
3. Add official health/ready probes at the same lifecycle point.
4. Revisit ephemeral unregister behavior against GitHub: decide whether aksh should avoid the AzDO distributed-task delete call on broker-path ephemeral runs or whether official hides it under a different lifecycle path.
5. Re-run the same three scenarios and then expand to medium-tier real-world workflows once result-log flow parity is closer.
