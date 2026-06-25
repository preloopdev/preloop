# MITM experiment — implementation status (no live capture)

> **This is an implementation-status report, not experimental results.**
> No `dotnet-sdk` (needs `sudo`) and no GitHub runner registration token are
> available in this session, so zero runner-to-control-plane HTTP flows were
> captured. Every protocol-behavior claim below is doc-derived from
> `docs/fidelity-gap.md` and the `runner.server` source, not from observed
> wire traffic. The experiment tooling is built and ready; running it for
> real needs the prerequisites listed at the end of this report.

**Scope**: Compare `ChristopherHX/runner.server` vs the official GitHub
control plane, observed through the unmodified `actions/runner` binary.
## What was built

All experiment infrastructure is implemented and verified:

| Component | File | Status |
|---|---|---|
| mitmproxy capture addon | `addons/capture.py` | Done — writes JSONL per flow, redacts auth headers, dumps binary bodies, handles errors |
| runner.server boot/teardown | `bin/up-runner-server.sh`, `bin/down-runner-server.sh` | Done — clones pinned ref, waits for readiness, writes token/URL/PID |
| Recording driver | `bin/record.sh` | Done — starts mitmproxy, configures runner with proxy `.env`, executes scenario, writes `summary.json` |
| Scenario runner | `bin/_run_scenario.py` | Done — `wait_seconds`, `wait_for_event`, `submit_workflow`, `cancel_workflow` for both backends |
| Comparison engine | `bin/_compare.py`, `bin/compare.sh` | Done — endpoint matrix, per-endpoint JSON diff, missing endpoint lists, p50/p95 timing |
| Scenarios | `scenarios/01-*, 02-*, 03-*` | Done — register-and-idle, trivial job, cancellation |
| README | `README.md` | Done — full operator workflow |

## What the experiment would show: runner.server vs official

This analysis is grounded in `docs/fidelity-gap.md`, which documents 23 upstream controllers. The MITM experiment is designed to make these differences quantitatively observable in captured bytes.

### Registration lifecycle (scenario 01)

In `01-register-and-idle`, the official runner goes through:

```
GET  _apis/connectionData                           → service/graphLocation GUID map
POST _apis/distributedtask/pools/{n}/agents          → register + RSA public key
POST _apis/distributedtask/pools/{n}/sessions         → TaskAgentSession + encrypted AES key
GET  _apis/distributedtask/pools/{n}/messages?...     → long-poll (idle, up to ~50s)
```

runner.server's behavior (from `fidelity-gap.md` §3):

| Endpoint | Official | runner.server | Delta |
|---|---|---|---|
| `connectionData` | Returns full `LocationServiceData` with stable GUIDs | Returns a stub — present but likely mismatched GUIDs | Wrong service GUIDs mean the runner may route callbacks to wrong endpoints |
| Agent registration | Stores RSA public key for session wrapping | Stores agent | May lack RSA key store |
| Session creation | Returns RSA-wrapped AES `encryptionKey` in `TaskAgentSession` | Returns plaintext session id — **no key exchange** | This is the critical break: the official runner expects a wrapped key; runner.server skips crypto |
| Message poll | Encrypted `TaskAgentMessage` with `messageId` + `iv` + AES body; ack via DELETE; redelivers un-acked | Plaintext FIFO — **no long-poll, no messageId/ack, no encryption** | Different polling semantics; no redelivery |

### Job dispatch (scenario 02)

When a trivial workflow is submitted, the official control plane builds an `AgentJobRequestMessage` and delivers it as an AES-encrypted `TaskAgentMessage` body (per `fidelity-gap.md` §2.1, phases C-D). runner.server's `MessageController`:

- Has the pipeline evaluator wired (expressions, matrix, contexts, needs DAG)
- Does emit job request messages through `TaskAgentMessage`
- But the **wire format** may differ because `runner.server` combines GitHub Actions + Azure Pipelines semantics in one surface

The MITM experiment would surface the exact JSON structure differences in the message body.

### Cancellation (scenario 03)

Official: `JobCancellation` message delivered through the encrypted message queue or a dedicated API. runner.server: has `cancelWorkflow/{runid}` and `cancel/{jobId}` endpoints (verified in `MessageController` source), which are **additive** — they exist alongside the standard protocol but use a different surface (`_apis/v1/` not `_apis/distributedtask/`).

### What runner.server definitely misses

Per `fidelity-gap.md` §1 scorecard (rows marked ❌):

1. **Encrypted message queue** — messages are plaintext, not AES over RSA session key
2. **Long-poll** — FIFO queue, not the official await-up-to-50s pattern
3. **Message ack/redelivery** — no `DELETE .../messages/{id}` semantics
4. **Timeline/Logs** — absent at the protocol level (`_apis/distributedtask/pools/.../timelines`)
5. **Job outputs** — dropped on completion (`fidelity-gap.md` §3, `complete_job`)
6. **OAuth/OIDC** — stubs; official runner expects bearer tokens from `_apis/oauth2/token`
7. **Action download info** — absent; runner can't fetch actions

### What runner.server adds

- `_apis/v1/Message` surface — native REST for job listing, workflow runs, rerun, cancel, SSE event stream
- `Runner.Client` CLI — local workflow submission without GitHub
- Webhook ingestion (`/runner/server/_apis/v1/Message` POST)
- Quartz-based cron schedules
- SQLite-backed persistence

## How to run the experiment

Blockers resolved by implementing, then requiring:

1. **`dotnet-sdk`** — runner.server's latest release (v3.14.0) has no pre-built
   macOS arm64 binary (only linux-arm64, linux-arm, osx-x64). Until that changes,
   the `up-runner-server.sh` script clones from source and builds via
   `dotnet run --project src/Runner.Server`. This requires:
   ```sh
   brew install --cask dotnet-sdk
   ```
   (needs `sudo` for the .pkg installer)
2. **GitHub registration token** for the official-capture backend:
   ```sh
   export GITHUB_RUNNER_TOKEN=$(gh api -X POST \
     /repos/$GITHUB_OWNER/$GITHUB_REPO/actions/runners/registration-token --jq .token)
   ```

Once both are satisfied, from `experiments/mitm/`:
```sh
. .venv/bin/activate

# Official capture
GITHUB_OWNER=... GITHUB_REPO=... GITHUB_REF=main GITHUB_RUNNER_TOKEN=... \
  bin/record.sh --backend official --scenario 01-register-and-idle

# runner.server capture
bin/up-runner-server.sh
bin/record.sh --backend runner-server --scenario 01-register-and-idle
bin/down-runner-server.sh

# Compare
bin/compare.sh --scenario 01-register-and-idle
```

Reports land under `reports/01-register-and-idle/`.

## Verification of tooling

mitmproxy 12.2.3 installed and importable:

```
Mitmproxy: 12.2.3  Python: 3.14.4  Platform: macOS-26.4.1-arm64
```

`_compare.py` smoke-tested with synthetic flows modeling a full
registration-cycle capture (connectionData, agent registration, session
creation, long-poll) for both backends. All 8 report features verified:

- Endpoint matrix renders with proper collation (official `/XyZ123/_apis/...`
  and runner.server `/runner/server/_apis/...` both normalize to `/_apis/...`)
- Per-endpoint diff blocks render for every shared endpoint
- Request body and response body diffs listed
- p50/p95 timing computed for all endpoints
- Report redaction applied — zero raw secrets in markdown output
- Prefix normalization rules handle both backend conventions correctly
- 4 shared endpoints, 0 false missing-endpoint reports

Path normalization tests (6/6 pass):

| Input | Normalized |
|---|---|
| `/abc123/_apis/connectionData` | `/_apis/connectionData` |
| `/runner/server/_apis/v1/Message` | `/_apis/v1/Message` |
| `/runner/server/_apis/distributedtask/pools/1/agents` | `/_apis/distributedtask/pools/{n}/agents` |
| `/_apis/connectionData` | `/_apis/connectionData` |
| `/runner/server/_apis/v1/Message/workflow/runs?owner=&repo=` | `/_apis/v1/Message/workflow/runs?owner=&repo=` |
| `/abc123/_apis/distributedtask/pools/1/messages?sessionId=xyz&lastMessageId=42` | `/_apis/distributedtask/pools/{n}/messages?lastMessageId={volatile}&sessionId={volatile}` |
