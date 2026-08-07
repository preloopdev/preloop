# Contributing to preloop

Appreciate you looking to contribute. All kinds of feedback are welcome whether bug reports, feature requests, reproductions of official-runner divergence, docs, and code.

## Before you open an issue

Use the right template. Each one collects exactly the evidence a maintainer needs:

| Template | When | Where |
|---|---|---|
| **Bug report** | Something misbehaves, or preloop diverges from the official runner | [`.github/ISSUE_TEMPLATE/bug_report.yml`](.github/ISSUE_TEMPLATE/bug_report.yml) |
| **Feature request** | Something preloop should do that it doesn't yet | [`.github/ISSUE_TEMPLATE/feature_request.yml`](.github/ISSUE_TEMPLATE/feature_request.yml) |

**For divergence bugs, the fastest path to a fix is a reproduction showing what the official runner does.** preloop's compatibility target is `actions/runner` v2.336.0 (pinned in `versions.toml`). Two ways to record it:

- **Easy:** run the same workflow on GitHub (or with the official runner binary) and paste `gh run view --log <run-id>` — job/step names, order, and conclusions.
- **Wire-accurate (protocol issues):** the repo ships `runner-watch`, which records the official runner's exact request/response exchange and replays it against preloop:

  ```sh
  runner-watch record-golden --runner /path/to/actions-runner --scenario <name>
  runner-watch conform --runner 2.336.0 --preloop-url http://127.0.0.1:9090
  ```

  Attach the generated `.runner-watch/golden/v2.336.0/<scenario>/flows.jsonl` and the `conform` output.

## Before you open a PR

1. **`just test-ci`** must pass locally (fmt-check + clippy `-D` + full test suite).
2. **Protocol changes** (anything under `/_apis/`, `/broker/`, `/twirp/`, or runner-facing JSON shapes) must be validated against the **official `actions/runner`**, not only unit tests. Use the conformance suite:
   - `just conform` — committed official-runner flow replay
   - `just conform-server-light` / `just conform-server-deep` — server fidelity gates
   - `just conform-runner-light` / `just conform-runner-deep` — Rust runner fidelity gates
   - `just dogfood` — live E2E with the real runner
3. **Wire-shape changes** to DTOs in `preloop-gha-protocol/src/azdo/` must preserve serde round-trip fidelity. Check golden captures in `.runner-watch/golden/`.
4. Read the PR template ([`.github/pull_request_template.md`](.github/pull_request_template.md)) and fill in every gate that applies — protocol changes **require** the official-runner validation gate.

## Compatibility checklist

- [ ] Does this change any runner-facing JSON field name, casing, or default?
- [ ] Does this change an HTTP status code a runner might use for retry/terminal decisions?
- [ ] Does this change lease timing, session lifetime, or message delivery order?
- [ ] Does this change check-run or OAuth wire behavior?

If any answer is **yes**, verify against the official runner source
(`/Users/bnjoroge/mitm-proxy/experiments/mitm/.cache/runner.server/src`) and golden wire
captures before merging.

## Conformance documentation

- [`docs/conformance.md`](docs/conformance.md) — conformance harness, fixture expansion, command comparison, and the provider integration gate.
- `benchmarks/compatibility/` — the evidence index: server fidelity (official runner against GitHub vs. preloop), runner fidelity, live behavior, MITM captures, and replay gates.
- `.runner-watch/golden/` — golden wire captures by runner version. New protocol behaviors should ship with a golden.
- `docs/fidelity-gap.md` — known protocol gaps and roadmap. If you're fixing a gap, link it.

## Project structure

- `docs/architecture.md` — crate map + module map.
- `docs/setup.md` — setup, credentials, secrets, config.
- `docs/debug-sessions.md` — the step debugger (pause, inspect, retry).
- `docs/github-app-webhook.md` / `docs/github-tokens.md` — GitHub integration surface.
- `AGENTS.md` — agent/LLM-specific conventions.

## Quick start

```sh
# Required: Rust 1.97 with rustfmt + clippy (pinned in rust-toolchain.toml)
just check        # cargo check --workspace
just fmt          # cargo fmt --all
just test         # cargo test --workspace --quiet
just clippy       # cargo clippy --workspace --all-targets -- -D warnings
just test-ci      # fmt-check + clippy + test (the full CI gate)
```

## E2E testing

The official runner strips non-default HTTP ports, so local E2E requires a port-80
redirect:

```sh
just e2e-setup     # sudo: redirects :80 → :9090
just serve         # start preloop on :9090
# In another terminal: configure + run the official runner against http://127.0.0.1
just e2e-status    # check redirect is active
just e2e-teardown  # remove redirect
```
