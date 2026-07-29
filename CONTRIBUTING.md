# Contributing to aksh

## Quick start

```sh
# Required: Rust 1.86 with rustfmt + clippy (pinned in rust-toolchain.toml)
just check        # cargo check --workspace
just fmt           # cargo fmt --all
just test          # cargo test --workspace --quiet
just clippy        # cargo clippy --workspace --all-targets -- -D warnings
just test-ci       # fmt-check + clippy + test (the full CI gate)
```

## Before submitting

1. **`just test-ci`** must pass locally.
2. **Protocol changes** (anything under `/_apis/`, `/broker/`, `/twirp/`, or runner-facing
   JSON shapes) must be validated against the **official `actions/runner`**, not only unit
   tests. Use `just dogfood` for live validation and `just conform` for the
   committed official-runner flow replay.
3. **Wire-shape changes** to DTOs in `aksh-gha-protocol/src/azdo/` must preserve serde
   round-trip fidelity. Check golden captures in `.runner-watch/golden/v2.335.1/`.

## Compatibility checklist

- [ ] Does this change any runner-facing JSON field name, casing, or default?
- [ ] Does this change an HTTP status code a runner might use for retry/terminal decisions?
- [ ] Does this change lease timing, session lifetime, or message delivery order?

If any answer is **yes**, verify against the official runner source
(`/Users/bnjoroge/mitm-proxy/experiments/mitm/.cache/runner.server/src`) and golden wire
captures before merging.

## Project structure

See `docs/architecture.md` for crate responsibilities and the module map.
See `AGENTS.md` for agent/LLM-specific conventions.

## E2E testing

The official runner strips non-default HTTP ports, so local E2E requires a port-80
redirect:

```sh
just e2e-setup     # sudo: redirects :80 → :9090
just serve         # start aksh on :9090
# In another terminal: configure + run the official runner against http://127.0.0.1
just e2e-status    # check redirect is active
just e2e-teardown  # remove redirect
```
