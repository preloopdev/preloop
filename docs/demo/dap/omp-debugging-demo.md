# Demo: an omp agent drives a preloop DAP debug session

A recorded terminal session in which an AI coding agent (Oh My Pi / `omp`,
DeepSeek V4 Flash) attaches the DAP debugger to a running preloop CI job,
reads the live job context, finds the root cause of a failing deploy
workflow, fixes the run, and re-runs until green.

## What happens in the recording

1. The agent reads the failing deploy workflow (`demo.yml`).
2. It submits a run with `enable_debugger: true`; the engine holds the job
   until a debugger client attaches (the runner's DAP server registers its
   port with the engine, which proxies the client over WebSocket).
3. The agent attaches `dapctl` (a small DAP client), sends `initialize` and
   `configurationDone`, and receives the `stopped` event at job entry.
4. It lists the DAP scopes (`github`, `env`, `runner`, `job`, `steps`,
   `secrets`) and reads the `github` scope: `event.inputs.target = "release"`.
   That is the bug: the workflow was triggered with the wrong input, and the
   production guard rejects anything but `prod`. The value is invisible in
   the workflow YAML and only visible at runtime, which is what makes DAP
   the right tool for the diagnosis.
5. It resumes the job, watches it fail, confirms via the engine API.
6. Fix: resubmit with `payload-prod.json` (`target=prod`), attach again,
   resume, and the run reaches `conclusion: success`.

Evidence from the actual runs: run with `inputs.target=release` failed with
`refusing to deploy target='release' to production`; rerun with
`inputs.target=prod` passed.

## Replay

```sh
asciinema play docs/demo/dap/demo.cast
# or open docs/demo/dap/demo.gif in a browser/image viewer
# or open docs/demo/dap/demo.mp4 to pause and seek
# For the short version, use `demo-highlight.mp4` or `demo-highlight.gif`.
```

## Re-run the demo yourself

Prerequisites: the current checkout (including the DAP transport support),
built binaries, `asciinema`, `omp`, and Python 3. The demo's helper prompt
expects its files under `/tmp/dapdemo`; create that layout from the repository
root before starting the engine:

```sh
cargo build --release -p preloop-runner-server -p preloop-runner
mkdir -p /tmp/dapdemo/runner
cp docs/demo/dap/demo.yml docs/demo/dap/payload-release.json \
  docs/demo/dap/payload-prod.json docs/demo/dap/submit.sh \
  docs/demo/dap/dapctl docs/demo/dap/agent-prompt.md /tmp/dapdemo/
cp target/release/preloop-runner /tmp/dapdemo/runner/
chmod +x /tmp/dapdemo/submit.sh /tmp/dapdemo/dapctl
```

Start the engine in one terminal. This demo-only permissive registration
setting is restricted to loopback and must not be used on an exposed listener.
Set the system token in every client terminal; do not print it.

```sh
export PRELOOP_SYSTEM_TOKEN="${PRELOOP_SYSTEM_TOKEN:-$(python3 -c 'import secrets; print(secrets.token_hex(32))')}"
PRELOOP_REGISTRATION_POLICY=permissive \
PRELOOP_PUBLIC_URL=http://127.0.0.1:9191 \
PRELOOP_CONFIG=/tmp/dapdemo/server-config.toml \
target/release/preloop-server serve --listen 127.0.0.1:9191 \
  --state-dir /tmp/dapdemo/server-state --enable-test-api --test-api-token dev-token
```

In a second terminal, start the runner:

```sh
cd /tmp/dapdemo/runner
ACTIONS_RUNNER_DAP_CONNECTION_TIMEOUT=30 \
  ./preloop-runner configure --url http://127.0.0.1:9191 \
  --token dummy-token --name demo-runner --unattended --no-externals
ACTIONS_RUNNER_DAP_CONNECTION_TIMEOUT=30 \
  ./preloop-runner run
```

In a third terminal, start the agent from the copied demo directory:

```sh
cd /tmp/dapdemo
export PRELOOP_SYSTEM_TOKEN='<same token as the engine terminal>'
PATH="/tmp/dapdemo:$PATH" asciinema rec demo-again.cast --command \
  "omp --auto-approve --cwd . @agent-prompt.md"
```

The `agent-prompt.md` reads the token from the environment; it does not
contain a credential.

## Files

| File | Purpose |
|---|---|
| `demo.cast` | asciinema recording of the agent session |
| `demo.gif` | slower rendered GIF, with thinking gaps compressed and output slowed |
| `demo.mp4` | pausable/seekable video of the same slowed rendering |
| `demo-highlight.mp4` | 25-second DAP-focused highlight, pausable/seekable |
| `demo-highlight.gif` | 25-second DAP-focused highlight |
| `demo.yml` | the failing deploy workflow under test |
| `payload-release.json` | failing trigger: `inputs.target=release` |
| `payload-prod.json` | fixed trigger: `inputs.target=prod` |
| `submit.sh` | submits a run with the DAP debugger enabled, prints the run id |
| `dapctl` | DAP client daemon + CLI the agent drives |
| `agent-prompt.md` | the prompt handed to the omp agent |

The repository CLI now includes the same client in Rust:

```sh
preloop dap <run-id>
```

`dapctl` remains as a tiny Python reference client for the recording and is
not required by users.
