# MITM runner/control-plane experiment

Captures exact HTTP traffic between the official `actions/runner` binary and control planes using mitmproxy, then compares the official GitHub control plane against custom ones (`ChristopherHX/runner.server` and `aksh`).

## Use cases

### 1. Protocol fidelity testing

You're building `aksh` — a custom GitHub Actions control plane. How do you know it speaks the same protocol as the real GitHub?

Record the exact HTTP traffic between the official `actions/runner` binary and GitHub, then compare it against what aksh produces for the same scenario:

```
runner ──→ mitmproxy ──→ GitHub        (golden capture)
runner ──→ mitmproxy ──→ aksh          (target capture)
                    ↓
              compare.sh               (diff report)
```

The comparison report shows every endpoint side-by-side: which ones match, which are missing, response body diffs, timing differences, status code mismatches.

### 2. Conformance regression gate

You change something in aksh. Did you break protocol compatibility?

Replay a golden capture (recorded once against real GitHub) against aksh without needing a live runner or GitHub access:

```sh
bin/conform.sh --golden golden/v2.329.0/01-register-and-idle --target aksh --scenario 01-register-and-idle
```

This uses `mitmdump --server-replay` to feed the exact recorded requests to aksh and compare responses. No network access to GitHub needed — fast enough for CI.

### 3. Protocol documentation

The Actions runner protocol is undocumented. What endpoints does the runner hit? In what order? What payloads does it expect?

The captured `flows.jsonl` files are a machine-readable record of the entire protocol lifecycle — registration, session creation, message long-polling, job assignment, artifact upload, cache operations, job completion. The scenarios cover different protocol paths:

| Scenario | Protocol path |
|---|---|
| `01-register-and-idle` | Registration + long-poll idle loop |
| `02-trivial-job` | Full job lifecycle: assign → run → complete |
| `03-cancellation` | Job cancellation mid-run |
| `04-request-ack` | Job acknowledgment flow (v2.329.0+) |
| `05-multi-job` | Session reuse across sequential jobs |

## Prerequisites

- macOS arm64 (for `linux-x64` swap runner asset in `versions.toml` and `record.sh`)
- Python 3.11+ with `pip`
- `mitmproxy` CLI (`brew install mitmproxy` or `pip install mitmproxy`)
- `dotnet-sdk` 8.0+ (`brew install --cask dotnet-sdk`) — for runner.server only
- Rust toolchain (`rustup.rs`) — for aksh only
- `gh` CLI authenticated (`brew install gh && gh auth login`) — for official backend only
- A throwaway GitHub repo with Actions enabled and a registration token — for official backend only

## Quick start

```sh
cd experiments/mitm
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
```

### Official capture (requires GitHub)

```sh
export GITHUB_OWNER=your-org-or-user
export GITHUB_REPO=your-repo
export GITHUB_REF=main
export GITHUB_RUNNER_TOKEN=$(gh api -X POST \
  /repos/$GITHUB_OWNER/$GITHUB_REPO/actions/runners/registration-token \
  --jq .token)
bin/record.sh --backend official --scenario 01-register-and-idle
```

### runner.server capture

```sh
bin/up-runner-server.sh
bin/record.sh --backend runner-server --scenario 01-register-and-idle
bin/down-runner-server.sh
```

### aksh capture

```sh
bin/up-aksh.sh
bin/record.sh --backend aksh --scenario 01-register-and-idle
bin/down-aksh.sh
```

### Compare

```sh
# Default: official vs runner-server
bin/compare.sh --scenario 01-register-and-idle

# Custom backends
bin/compare.sh --scenario 01-register-and-idle --left official --right aksh
bin/compare.sh --scenario 01-register-and-idle --left runner-server --right aksh
```

Repeat for `02-trivial-job`, `03-cancellation`, `04-request-ack`, and `05-multi-job`.

## Golden capture workflow

Golden captures record the official GitHub control plane's behavior as a baseline for conformance testing.

### Record golden captures

```sh
# Record all scenarios against official GitHub.
bin/record-golden.sh

# Record a single scenario.
bin/record-golden.sh --scenario 01-register-and-idle
```

Golden captures are stored in `golden/v<runner-version>/` per scenario.

### List golden captures

```sh
bin/list-goldens.sh
```

### Replay golden captures

Replay recorded traffic against a custom backend without needing GitHub or a live runner:

```sh
# Start the target backend.
bin/up-aksh.sh

# Replay golden traffic against aksh.
bin/replay.sh --golden golden/v2.329.0/01-register-and-idle --target aksh

# Or use the one-command conformance test.
bin/conform.sh --golden golden/v2.329.0/01-register-and-idle --target aksh --scenario 01-register-and-idle
```

### Record all scenarios for a backend

```sh
bin/record-all.sh --backend aksh
bin/record-all.sh --backend runner-server
```

## Backends

| Backend | Port | Startup | Notes |
|---|---|---|---|
| `official` | n/a (GitHub) | Requires env vars | Uses real GitHub Actions |
| `runner-server` | 5000 | `bin/up-runner-server.sh` | ChristopherHX/runner.server (.NET) |
| `aksh` | 9090 | `bin/up-aksh.sh` | aksh-runner-server (Rust) |

## TLS / mitmproxy CA

If the official capture fails with a TLS validation error, import the mitmproxy CA cert into the macOS user keychain:

```sh
security add-trusted-cert -k ~/Library/Keychains/login.keychain-db \
  experiments/mitm/.cache/mitmproxy/mitmproxy-ca-cert.pem
```

Cleanup afterward:

```sh
security delete-certificate -c mitmproxy ~/Library/Keychains/login.keychain-db
```

## Cleanup

```sh
# Remove stale runner binary caches (keeps current version).
bin/clean-cache.sh

# Remove captures older than 7 days.
bin/clean-captures.sh --older-than 7

# Dry run — see what would be deleted.
bin/clean-captures.sh --older-than 30 --dry-run
```

## Output

- `captures/<backend>/<scenario>/latest/flows.jsonl` — all captured flows
- `captures/<backend>/<scenario>/latest/runner.log` — runner stdout/stderr
- `reports/<scenario>/<timestamp>.md` — comparison report
- `golden/v<version>/<scenario>/` — golden captures per runner version

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Configuration or scenario failure |
| 2 | Port conflict |
| 3 | Missing prerequisite |
| 4 | Missing capture directory |
| 5 | Empty capture (comparison impossible) |
| 8 | Unknown scenario step |
| 9 | Missing scenario step parameter |
| 10 | Scenario timeout |

## CI usage

All scripts support `--non-interactive` for non-TTY environments:

```sh
bin/record.sh --backend aksh --scenario 01-register-and-idle --non-interactive
bin/record-golden.sh --non-interactive
bin/record-all.sh --backend aksh --non-interactive
```

## Tests

```sh
. .venv/bin/activate
python -m pytest tests/ -v
```
