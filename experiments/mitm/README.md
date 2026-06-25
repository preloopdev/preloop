# MITM runner/control-plane experiment

Captures exact HTTP traffic between the official `actions/runner` binary and control planes using mitmproxy, then compares the official GitHub control plane against a custom one (`ChristopherHX/runner.server`).

## Prerequisites

- macOS arm64 (for `linux-x64` swap runner asset in `versions.toml` and `record.sh`)
- Python 3.11+ with `pip`
- `mitmproxy` CLI (`brew install mitmproxy` or `pip install mitmproxy`)
- `dotnet-sdk` 8.0+ (`brew install --cask dotnet-sdk`)
- `gh` CLI authenticated (`brew install gh && gh auth login`)
- A throwaway GitHub repo with Actions enabled and a registration token

## Quick start

```sh
cd experiments/mitm
python3 -m venv .venv
. .venv/bin/activate
pip install -e .

# --- Official capture (requires GitHub) ---
export GITHUB_OWNER=your-org-or-user
export GITHUB_REPO=your-repo
export GITHUB_REF=main
export GITHUB_RUNNER_TOKEN=$(gh api -X POST \
  /repos/$GITHUB_OWNER/$GITHUB_REPO/actions/runners/registration-token \
  --jq .token)
bin/record.sh --backend official --scenario 01-register-and-idle

# --- runner.server capture ---
bin/up-runner-server.sh
bin/record.sh --backend runner-server --scenario 01-register-and-idle
bin/down-runner-server.sh

# --- Compare ---
bin/compare.sh --scenario 01-register-and-idle
```

Repeat for `02-trivial-job` and `03-cancellation`.

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

## Output

- `captures/<backend>/<scenario>/latest/flows.jsonl` — all captured flows
- `captures/<backend>/<scenario>/latest/runner.log` — runner stdout/stderr
- `reports/<scenario>/<timestamp>.md` — comparison report
