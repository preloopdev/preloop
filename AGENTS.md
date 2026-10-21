# Repository Guidelines

`preloop` reimplements the GitHub Actions control plane and the official `actions/runner` in Rust. The unmodified official runner registers, polls, executes, and reports against it; `runner-watch` checks protocol conformance.

## Crates

| Crate | Role |
| --- | --- |
| `preloop-runner-server` | Control plane: `/_apis/…` (runner protocol), `/api/v1/…` (native REST), `/broker/…` |
| `preloop-gha-parser` / `-expressions` / `-protocol` | Workflow YAML → job DAG/matrix; `${{ }}` evaluator; wire DTOs, session crypto, `SecretString`, NDJSON events |
| `preloop-runner` / `-runner-client` | Rust runner (Listener + Worker); CLI that submits workflows |
| `preloop-cli` | The `preloop` binary (run/serve/debug/push) |
| `preloop-orchestrator` / `preloop-vm` | Golden/env provisioning + runner pool; `VmProvider` backends (SmolVM default, AgentENV opt-in via `PRELOOP_VM_BACKEND`). |
| `preloop-cache` / `-artifacts` / `-dap` / `-observability` / `-socket-activation` | File-backed storage, DAP bridge, logging/OTel, systemd activation |
| `preloop-conformance` / `runner-watch` | Conformance harnesses and protocol-diff tooling |

Crate/module map: `docs/control-plane/architecture/overview.mdx`. Pinned official runner: `runner_version` in `versions.toml`.

## Commands

```sh
just test-ci    # fmt-check + clippy + zizmor + tests + control migrations + conform (the full gate)
just serve      # engine on 127.0.0.1:9090
just dogfood    # E2E with the real runner
```

## Conventions

- **Toolchain**: Rust per `rust-toolchain.toml` (1.99); `cargo fmt`, `cargo clippy --workspace --all-targets`.
- **Errors**: `anyhow` at top level; `ApiError` in HTTP handlers; `thiserror` in libraries.
- **Secrets**: `SecretString`; call `expose()` only at protocol boundaries.
- **Wire compatibility**: `/_apis/…` is the source of truth. Validate protocol changes against the **official runner**, not only unit tests. Broker + Twirp results protocol only (v2.329.0+).
- **State**: the control database is authoritative. All durable control mutations go through the `ControlBackend` trait (`control/backend.rs`); backends are SQLite (default) and Postgres, chosen by `PRELOOP_STORE_URL`. `state.rs::InnerState` is node-local only. See [API and state](docs/control-plane/architecture/api-and-state.mdx) before touching `control/`.
- **At-rest encryption is obfuscation**: the key sits beside the DB, so it stops a stolen DB file, not a compromised state dir.
- **VM substrates**: `PRELOOP_VM_BACKEND` selects SmolVM (default) or AgentENV (opt-in); the orchestrator branches on `VmProvider::capabilities()`. Read [Sandbox architecture](docs/control-plane/architecture/sandboxes.mdx) before touching pool or provider code.

## Agent preferences

- **Be critical.** Push back with evidence when a plan hides risk or a claim is wrong.
- **Composability**: any runner works with any server. Never introduce protocol divergences.
- **Drop-in workflows**: users run their workflows unmodified.
- **Run `just test-ci`** after each large chunk of work.
- **`[skip ci]`** in a commit message skips the CI gate (including `contrib/pre-push`); use it only for docs/non-code changes.
- **Supply-chain policy isolation**: strict policy files (`deny.toml`, `.cargo/audit.toml`, `.github/workflows/supply-chain.yml`, `supply-chain/`) may ship only with dependency/policy files, and `Cargo.toml`/`Cargo.lock` join that set only when a third-party lock entry (a `[[package]]` block with a `source` line) changes; first-party manifest and lockfile edits may ship with their code, while a new third-party dependency still needs its own PR first. There is no exception: check with `just supply-chain-guard` (`supply-chain/README.md` holds the rule and the guard).

## Dev VM (per-worktree Linux VM)

A frozen `preloop-dev` SmolVM golden (Rust, nextest, cargo-vet, preloop, OMP) is forked per worktree; the worktree is mounted at `/workspace` and `target/` is VM-local. Details: `scripts/dev-vm/README.md`.

```sh
just dev-vm-check            # exit 0 only if the golden exists; otherwise use the host toolchain
just dev-vm-up               # fork for this worktree (idempotent)
just dev-vm-exec cargo nextest run --locked -p <crate>
just dev-vm-shell omp        # interactive; just dev-vm-down to delete
```

- macOS SmolVM guests have **no nested KVM**; tests that boot another VM must run elsewhere.
- Guest `localhost` is **not** relayed to the host: publish a port (`PRELOOP_DEV_PORTS=18080:8080 just dev-vm-up`, or `just dev-vm-publish`) and bind `0.0.0.0` in the guest.
- Provider logins/API keys stay on the host via the OMP auth gateway (`PRELOOP_DEV_GATEWAY_URL` + `PRELOOP_DEV_GATEWAY_TOKEN_FILE`); never put raw provider keys in the VM. After adding a login or key, `just dev-vm-omp-sync`.

## Debugging CI behavior (use preloop's own tools first)

For queue stalls, job failures, check-run state, or runner churn, dogfood the debug features before log-diving ([debugging guide](docs/control-plane/guides/debugging.mdx)); any divergence from that doc is a bug to report.

- **Sessions**: failed jobs pause into debug sessions. List with `preloop debug` or `GET /api/v1/debug/sessions`; attach with `preloop debug [<ref>]` (`:retry`, `:retry --sync`, `:retry --from <step>`, `:sync`, `:export`); non-interactive: `preloop debug --verdict retry|continue|abort`.
- **Agent API** (native bearer): `POST /api/v1/agent/debug/sessions/<id>/lease`, then `GET .../events`, `POST .../operations`, `GET .../audit`.
- **DAP**: WebSocket `GET /api/v1/runs/<run_id>/debug`; `preloop-dap` bridges to the session state machine.
- **Hold VMs**: `preloop run --preserve-on-failure` keeps the VM for a later `preloop shell` when nothing can answer interactively.
- Leaked sessions (`abandoned`, `aborted`, or never closing) are findings, not noise.

Also: `docs/contributing/fidelity-gap.md` (protocol gaps), `CONTRIBUTING.md` (workflow, compatibility checklist), `fixtures/workflows/dogfood.yml` (local validation), `.runner-watch/golden/` (protocol golden captures).
