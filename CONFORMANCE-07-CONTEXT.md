# Conformance Scenario 07 — Step Failure Protocol Fidelity

## Goal

Fix all protocol fidelity gaps in the `acquirejob` response so scenario 07 (step-failure)
passes the `runner-watch conform` strict comparison against the official GitHub Actions
runner v2.335.1.

## Background

The `runner-watch conform` tool replays captured protocol exchanges against the aksh server
and diffs every request/response field. Scenario 07 (step-failure) is the only one failing
out of 11 scenarios. The failure is entirely in the `POST /broker/{n}/acquirejob` response
payload — aksh returns a simplified message while GitHub returns the full broker payload.

## Fidelity Gaps to Fix

The conformance report is at:
`/tmp/conf/v2.335.1/07-step-failure.md` (also uploaded as CI artifact)

### 1. acquirejob response — missing fields

GitHub returns these fields that aksh omits:

| Field | GitHub value | aksh value |
|---|---|---|
| `billingOwnerId` | `"O_kgDOEbddog"` | missing |
| `defaults` | `[]` | missing |
| `environmentVariables` | `[]` | missing |
| `fileTable` | `["string"]` | missing |
| `jobContainer` | `null` | missing |
| `jobName` | `"string"` | missing |
| `jobOutputs` | `null` | missing |
| `jobServiceContainers` | `null` | missing |
| `lockedUntil` | `"string"` | missing |
| `snapshot` | `null` | missing |
| `mask` | `[{"type":"string","value":"string"}]` | `maskHints: []` (different field name) |
| `displayName` | missing | `"string"` (extra field) |
| ~40 `variables` entries | present | missing |

### 2. acquirejob response — type/value differences

| Field | GitHub | aksh |
|---|---|---|
| `messageId` | `number` (i64) | `string` (UUID) |
| `contextData.github` | ~50 nested k/v pairs | ~5 nested k/v pairs |
| `contextData.inputs` | `{d: [], t: 2}` | `{t: 2}` |
| `contextData.matrix` | `null` | `{t: 2}` |
| `contextData.needs` | `{d: [], t: 2}` | `{t: 2}` |
| `contextData.strategy` | `{d: [{k:"fail-fast",v:false},{k:"max-parallel",v:1}], t: 2}` | `{t: 2}` |
| `contextData.vars` | `{d: [], t: 2}` | `{t: 2}` |
| `contextData.env` | missing | `{t: 2}` (extra) |
| `contextData.system` | missing | `{d: [{k:"string",v:"string"}], t: 2}` (extra) |
| `steps[].continueOnError` | `null` | missing |
| `steps[].environment` | missing | `{type: 2}` (extra) |
| `steps[].name` | `"string"` | missing |
| `steps[].timeoutInMinutes` | `null` | missing |
| `steps[].inputs.Value.col/file/line` | present | missing |
| `timeline.changeId` | `number` | missing |
| `timeline.location` | `null` | missing |
| `plan.artifactLocation` | `"string"` | missing |
| `plan.artifactUri` | `"string"` | missing |
| `plan.version` | `number` | missing |
| `resources.endpoints[].isReady` | `boolean` | missing |
| `resources.endpoints[].data.ConnectivityChecks` | `"string"` | missing |
| `resources.endpoints[].data.GenerateIdTokenUrl` | `"string"` | missing |
| `resources.endpoints[].data.ServerId` | `"string"` | missing |
| `resources.endpoints[].data.ServerName` | `"string"` | missing |
| `resources.endpoints[].data.serviceOwner` | missing | `"string"` (extra) |
| `resources.endpoints[].type` | missing | `"string"` (extra) |
| `resources.endpoints[].url` | GitHub URL | `http://127.0.0.1:9090/runner/server` |
| `resources.repositories` | missing | `[]` (extra) |
| `variables` | ~40 entries including `DistributedTask.*`, `actions.runner.*`, `github_token`, etc. | only `system.github.launch_endpoint`, `system.github.token`, `system.pullRequestTargetBranch` |

### 3. Other endpoints with minor diffs

These are NOT the cause of the conformance failure (the tool tolerates them) but are noted:

- `DELETE /agents/{n}` and `DELETE /sessions`: response `null` vs `{}`
- `PATCH /AgentRequest/{n}/{n}`: response `null` vs `{}`
- `POST /completejob`: response `null` vs `{}`
- `POST /oauth2/token`: 400 vs 401 status code difference
- `GET /messages`: `messageId` number vs string
- `POST /agents`: missing `createdOn`, `owningTenant`, label `id` fields
- `POST /sessions`: extra `encryptionKey` field
- `GET /connectionData`: missing `serviceOwner` field
- `GET /pools`: missing second pool entry, extra `poolType` field

## Key Source Files

- `crates/aksh-runner-server/src/lib.rs` — `broker_acquire_job` handler (~line 3528)
- `crates/aksh-runner-server/src/lib.rs` — `agent_request_json` helper (~line 3764)
- `crates/aksh-runner-server/src/lib.rs` — route registration (~line 752)
- `crates/aksh-gha-protocol/src/lib.rs` — `AgentJobRequestMessage`, `PipelineContextData`
- `crates/runner-watch/src/compare.rs` — conformance comparison logic

## Approach

1. Read the `broker_acquire_job` handler and understand what it currently returns
2. Read the official runner's `RunServiceHttpClient.cs` acquirejob response parsing
3. Add the missing fields to the response, using placeholder/empty values where the
   real GitHub values are environment-specific (IDs, URLs, tokens)
4. The key insight: the runner doesn't *use* most of these fields — it just expects
   them to be present in the JSON. Empty/null/placeholder values are fine as long as
   the schema matches.
5. Run `cargo test -p aksh-runner-server` to verify no regressions
6. Run `runner-watch conform --runner v2.335.1 --aksh-url http://127.0.0.1:9090`
   to verify scenario 07 passes

## Constraints

- Do NOT break the existing 10 passing conformance scenarios
- Do NOT break any existing tests
- The `variables` map should include the keys the runner checks at startup — at minimum
  `system.github.token` and `system.github.launch_endpoint`
- Placeholder values for environment-specific fields (IDs, URLs) are acceptable
- Match the exact JSON key names and types from the official response

## Verification

```bash
cargo test -p aksh-runner-server --lib
cargo run -p runner-watch -- conform --runner v2.335.1 --aksh-url http://127.0.0.1:9090 --skip-cargo-test
```

The conformance tool should report 11/11 scenarios passing (currently 10/11).
