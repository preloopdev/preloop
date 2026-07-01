# runner-watch build log

This document records the implementation steps for `runner-watch`, the automated protocol sync pipeline described in `docs/runner-watch-plan.md`.

## 1. Repository discovery

- Read the plan in `docs/runner-watch-plan.md` and the validation target in `docs/fidelity-gap.md §1a.4`.
- Mapped existing aksh protocol surfaces:
  - `crates/aksh-gha-protocol/src/azdo.rs` for `TimelineRecord` and other DTOs.
  - `crates/aksh-runner-server/src/lib.rs` for connection data, agent request, timeline, log, action download, and broker/admin-adjacent handlers.
  - `experiments/mitm/bin/_compare.py` for deterministic capture comparison.
  - `experiments/mitm/bin/record-golden.sh` for golden capture creation.
- Added the static surface map required by the plan at `docs/aksh-surface.toml`.

## 2. Workspace integration

- Added a new Rust binary crate at `crates/runner-watch`.
- Added `crates/runner-watch` to the root Cargo workspace.
- Added the workspace dependency `toml = "0.8"` for config/spec parsing.
- The crate inherits workspace package metadata and lints.

## 3. Phase 0 — watch

Implemented:

```sh
cargo run -p runner-watch -- watch
```

Behavior:

- Polls `https://github.com/actions/runner/releases.atom`.
- Extracts the latest `v*` release tag.
- Updates `.runner-watch/state.json` with `last_known_tag` and phase status.
- Emits JSON containing `runner_version` and whether the tag changed.

Verification performed:

```sh
cargo run -p runner-watch -- watch
```

Observed latest tag: `v2.335.1`.

## 4. Phase 1 — diff

Implemented:

```sh
cargo run -p runner-watch -- diff --from v2.322.0 --to v2.335.1
cargo run -p runner-watch -- diff --from v2.334.0 --to v2.335.1
```

Behavior:

- Shallow-clones the requested `actions/runner` tags under `.runner-watch/repos/`.
- Scans the tracked upstream directories from the plan:
  - `src/Runner.Listener`
  - `src/Runner.Worker`
  - `src/Runner.Common`
  - `src/Runner.Sdk`
- Applies the configured skip patterns.
- Emits `.runner-watch/delta.json` entries for:
  - added/removed C# properties grouped by enclosing class/struct/enum;
  - route attribute additions;
  - feature flag additions;
  - env var additions;
  - message/ref type additions;
  - protocol-keyword additions for changes that are semantically important but not property/route declarations (`AcknowledgeRunnerRequestAsync`, `AgentRequest`, `ServerUrlV2`, `BrokerUrl`, `auth_url_v2`, DAP/debugger, background fields, Node warnings, action-resolution flags, etc.).

Verification performed:

- `v2.322.0 → v2.335.1`: produced 299 delta entries.
- `v2.334.0 → v2.335.1`: produced 130 delta entries.

## 5. Phase 2 — triage/spec generation

Implemented:

```sh
cargo run -p runner-watch -- triage --no-agents
```

Behavior:

- Reads `.runner-watch/delta.json` and `.runner-watch/state.json`.
- Cleans the target `.runner-watch/specs/v{to}/` directory before writing, preventing stale specs from earlier comparisons.
- Uses `docs/aksh-surface.toml` plus deterministic recognition of the hand-validated fidelity-gap items.
- Emits one self-contained TOML spec per change group under `.runner-watch/specs/v{to}/`.
- Writes `.runner-watch/triage-summary.json`.
- If `--no-agents` is omitted, writes an unknown-entry prompt and attempts to invoke the configured Claude triage command for entries that deterministic triage cannot classify.

Historical validation target (`v2.322.0 → v2.335.1`) produced specs for the fidelity-gap §1a.4 items, including:

- `background-step-timeline-fields`
- `request-ack`
- `v2-admin-broker-connection`
- `use-runner-admin-flow`
- `runner-version-deprecated`
- `dap-debugger-endpoint`
- `send-job-level-annotations`
- `batch-action-resolution`
- `use-bearer-token-for-codeload`
- `node20-deprecation-warning`
- `disable-stdout-multiline-log-prefixing`
- `server-enforced-runner-settings`

Current/latest delta (`v2.334.0 → v2.335.1`) produced 5 specs:

- `background-step-timeline-fields`
- `dap-debugger-endpoint`
- `disable-stdout-multiline-log-prefixing`
- `node20-deprecation-warning`
- `request-ack`

## 6. Phase 3 — implement loop

Implemented:

```sh
cargo run -p runner-watch -- implement --dry-run
cargo run -p runner-watch -- implement
```

Behavior:

- Reads generated specs for the current target version.
- Writes stateless Codex prompts under `.runner-watch/prompts/`.
- In dry-run mode, does not invoke Codex.
- In live mode, invokes `codex exec <prompt>` up to `max_implement_iterations` per spec.
- After every successful Codex invocation, the orchestrator runs:
  - `cargo check`
  - `cargo test --workspace`
- If verification fails, the exact compiler/test output is appended to the next stateless Codex prompt.
- Once a spec verifies, the orchestrator stages existing source/doc/version paths and commits one commit per spec (`runner-watch: implement <change_id>`).
- Writes `.runner-watch/implementation-log.md` and persists phase state.

## 7. Phase 4 — review loop

Implemented:

```sh
cargo run -p runner-watch -- review --dry-run
cargo run -p runner-watch -- review
```

Behavior:

- Reads generated specs and the current repo diff.
- Runs `cargo test --workspace` independently before review and includes that evidence in each review prompt/artifact.
- Writes stateless Claude review prompts/review artifacts under `.runner-watch/reviews/v{to}/`.
- In dry-run mode, writes a `verdict = "dry_run"` review artifact for each spec.
- In live mode, invokes `claude -p <prompt> --output-format json` and stores the result.
- Writes `.runner-watch/review.toml` and persists phase state.

## 8. Phase 5 — conformance

Implemented:

```sh
cargo run -p runner-watch -- record-golden --runner v2.335.1 --target official --non-interactive
cargo run -p runner-watch -- conform --runner v2.335.1 --aksh-url http://127.0.0.1:9090
```

Behavior:

- `record-golden` delegates to the mitm worktree script at `experiments/mitm/bin/record-golden.sh`, then copies captures into `.runner-watch/golden/v{runner}/`.
- `conform` optionally runs `cargo test --workspace` first.
- Instead of depending on `experiments/mitm/bin/replay.sh`, `conform` implements a direct `flows.jsonl` request replayer:
  - reads each recorded official request;
  - normalizes `/runner/server/_apis/...` and `/{org}/_apis/...` paths to aksh-compatible `/_apis/...` paths;
  - sends the request to `--aksh-url` using `reqwest`;
  - captures aksh responses as a fresh `flows.jsonl`;
  - invokes `experiments/mitm/bin/_compare.py` with `official` vs `aksh` labels;
  - writes per-scenario reports plus `.runner-watch/conformance-report.md` and `.runner-watch/conformance-fail.toml` on divergence.

This direct replayer is intentional: `experiments/mitm/bin/replay.sh` uses `mitmdump --server-replay`, which serves recorded responses to a runner rather than replaying recorded requests against aksh. The deterministic conformance gate needs actual aksh responses.

## 9. Phase 6 — draft PR artifacts

Implemented:

```sh
cargo run -p runner-watch -- pr --dry-run
cargo run -p runner-watch -- pr --base main --head <branch>
```

Behavior:

- Updates release metadata before PR creation:
  - creates/updates root `versions.toml` with `runner_version = "{version}"`;
  - rewrites the `README.md` Current Status block with the verified runner-watch target;
  - appends/replaces the generated scorecard section in `docs/fidelity-gap.md`;
- Groups specs by tier:
  - critical: `blocker` and `security`
  - high: `concern` and `feature`
  - low: `nit`
- Writes self-contained PR body markdown files under `.runner-watch/prs/`.
- In live mode, invokes `gh pr create --draft` with `protocol-sync` and priority labels.

## 10. End-to-end orchestration

Implemented:

```sh
cargo run -p runner-watch -- run --from v2.322.0 --to v2.335.1 --no-agents --skip-implementation --skip-review
cargo run -p runner-watch -- run --from v2.334.0 --to v2.335.1 --no-agents --skip-implementation --skip-review
```

Behavior:

- Runs diff → triage → optional implement → optional review → optional conformance → PR body generation.
- If `--aksh-url` is omitted, writes a conformance report explaining that replay was skipped.
- `--no-agents` is useful for deterministic validation in environments without Claude/Codex CLIs.

## 11. Verification commands run during this build

```sh
cargo check -p runner-watch
cargo test -p runner-watch
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p runner-watch -- watch
cargo run -p runner-watch -- diff --from v2.322.0 --to v2.335.1
cargo run -p runner-watch -- triage --no-agents
cargo run -p runner-watch -- diff --from v2.334.0 --to v2.335.1
cargo run -p runner-watch -- triage --no-agents
cargo run -p runner-watch -- run --from v2.334.0 --to v2.335.1 --no-agents --skip-implementation --skip-review --skip-cargo-test
cargo run -p runner-watch -- run --from v2.322.0 --to v2.335.1 --no-agents --skip-implementation --skip-review --skip-cargo-test
cargo run -p runner-watch -- review --dry-run
cargo run -p runner-watch -- implement --dry-run
```

Harness-only conformance smoke test:

```sh
cargo run -p aksh-runner-server -- serve --listen 127.0.0.1:19090
# synthetic smoke golden under .runner-watch/golden/vsmoke/01-connection/:
# POST /_apis/v1/AgentRequest/1/1 -> 204
cargo run -p runner-watch -- conform --runner smoke --aksh-url http://127.0.0.1:19090 --skip-cargo-test
```

The smoke golden is intentionally synthetic and aksh-derived. It verifies the request replay,
comparison, pass/fail parsing, and stale-failure cleanup path only. It is **not** official
runner/backend conformance evidence. True conformance requires golden captures recorded from
the official backend with `runner-watch record-golden`.

Official v2.335.1 conformance attempt:

```sh
# 8080 was occupied by a local Lima listener, so a temporary recorder copy used 18080.
gh api -X POST /repos/preloopdev/aksh/actions/runners/registration-token --jq .token
GITHUB_OWNER=preloopdev \
GITHUB_REPO=aksh \
GITHUB_REF=main \
GITHUB_RUNNER_TOKEN=<redacted> \
PATH="../mitm-proxy/experiments/mitm/.venv/bin:$PATH" \
  .runner-watch/record-18080.sh --backend official --scenario 01-register-and-idle --non-interactive

cargo run -p aksh-runner-server -- serve --listen 127.0.0.1:19090
cargo run -p runner-watch -- conform \
  --runner v2.335.1 \
  --aksh-url http://127.0.0.1:19090 \
  --scenario 01-register-and-idle \
  --skip-cargo-test
```

Evidence:

- Fresh official capture: `../mitm-proxy/experiments/mitm/captures/official/01-register-and-idle/latest/summary.json`
  - `status = ok`
  - `runner_version = 2.335.1`
  - `flows_count = 68`
- runner-watch copied that capture to `.runner-watch/golden/v2.335.1/01-register-and-idle/`.
- The conformance report is `.runner-watch/conformance/v2.335.1/01-register-and-idle.md`.
- The report compares 56 filtered/mapped control-plane flows. It intentionally excludes
  repeated readiness/health probes (`/ready`, `/health`) and rewrites official scale-unit
  aliases to aksh-compatible compat paths before replay. The Twirp signed-log/blob URL calls
  are included in the 56-flow replay.
- Result: **failed**. This is a 56-flow filtered/mapped replay, not a raw 68-flow capture
  comparison. Current evidence separates into:
  - remaining replay-mapping issues: pool discovery / agent lookup and registration are still
    replayed as raw `/_apis/distributedtask/pools...` root paths, while aksh exposes those
    compat routes under `/runner/server/_apis/distributedtask/...`; do not treat those 404s as
    proven server gaps yet;
  - mapped-but-mismatched behavior: OAuth token handling returns `415` where official returns
    `200`, and registration returns `401` for the recorded request shape;
  - likely real missing surfaces: broker acquire/renew/complete endpoints and results-service
    Twirp log/update endpoints return `404` and have no matching route surface in aksh.

## 12. Current limitations and operational notes

- Live Claude/Codex execution is implemented but was not required for deterministic validation; use `--no-agents` or `--dry-run` to inspect prompts safely.
- Golden captures are not present in this checkout by default. To run conformance, record or copy `.runner-watch/golden/v{runner}/{scenario}/flows.jsonl` first, then start aksh and run `conform`.
- `record-golden` may require GitHub runner registration environment variables used by `experiments/mitm/bin/record-golden.sh`.
- The conformance gate owns and reads golden bytes; implementing/review agents do not modify captures.
