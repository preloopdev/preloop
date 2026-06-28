# MITM experiment — official GitHub control plane capture

**Run**: 2026-06-25T18:46:05Z  
**Backend**: official (`actions/runner` v2.317.0 → GitHub.com)  
**Scenario**: 01-register-and-idle  
**Status**: 22 flows captured successfully
> ⚠️ **Stale baseline**: This capture used runner v2.317.0. The current official runner is
> v2.335.1 and GitHub enforces v2.329.0+ minimum since March 2026. The protocol has changed
> significantly — see `docs/fidelity-gap.md §1a` for the deep diff. Re-captures against
> v2.329.0+ are needed before relying on these findings.

## Captured protocol flow

The unmodified official runner, configured against `github.com/Bnjoroge1/Docktree`, made these calls through mitmproxy:

| # | method | target | endpoint | status | duration (ms) |
|---|---|---|---|---|---|
| 1 | POST | api.github.com | `/actions/runner-registration` | 200 | 258 |
| 3 | GET | pipelinesghubeus5 | `/{session}/_apis/connectionData?connect` | 200 | 50 |
| 4 | GET | pipelinesghubeus5 | `/{session}/_apis/connectionData?connect` | 200 | 72 |
| 5 | GET | pipelinesghubeus5 | `/{session}/_apis/connectionData?connect` | 200 | 75 |
| 6 | GET | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools?` | 200 | 20 |
| 7 | GET | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools/{n}/` | 200 | 21 |
| 8 | POST | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools/{n}/{n}/agents` | **201** | 223 |
| 9-11 | GET | pipelinesghubeus5 | `/{session}/_apis/connectionData?connect` | 200 | 15-20 |
| 12 | GET | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools?` | 401 | 15 |
| 13 | POST | tokenghub | `/_apis/oauth2/token/{guid}` | 200 | 24 |
| 14 | GET | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools?` | 200 | 20 |
| 15-17 | GET | pipelinesghubeus5 | `/{session}/_apis/connectionData?connect` | 200 | 124-125 |
| 18 | POST | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools/{n}/{n}/sessions` | 200 | 208 |
| 19 | POST | tokenghub | `/_apis/oauth2/token/{guid}` | 200 | 23 |
| 20 | GET | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools/{n}/messages?...` | 200 | 109 |

## Key discoveries

### 1. The runner contacts github.com FIRST, not the pipelines service

Flow #1 is a `POST` to `api.github.com/actions/runner-registration`. Only AFTER this does the runner talk to `pipelinesghubeus5.actions.githubusercontent.com`. This is a two-phase registration.

### 2. connectionData is fetched 7+ times

The runner doesn't just fetch connectionData once — it re-fetches it at every phase transition: config, agent registration, session creation, and message polling. Each fetch may carry different `connect` query parameters.

### 3. OAuth2 is the auth mechanism

The runner gets bearer tokens from `tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}`. When a 401 is returned (flow #12), it re-authenticates before proceeding (flow #13). This confirms `fidelity-gap.md` §2.1: OAuth/OIDC is mandatory.

### 4. The session prefix is a long random string

The official runner's session path segment is 58 characters (`PuoS6BwhaJy073Fs8tfAA75cfJtVulJXwl1KDQ1EIvj3eC3do0`). This is NOT a GUID — it's likely a base64-encoded token or tenant identifier. runner.server uses GUIDs.

### 5. Agent registration returns 201 (not 200)

Flow #8 (agent POST) returns `201 Created`, not `200 OK`. runner.server's agent registration returns 200, which may cause the official runner to retry or reject.

### 6. The runner contacts GitHub even when configured against runner.server

Even when `--url http://192.168.1.221/runner/server` was passed, the official runner still made calls to `api.github.com`, `pipelinesghubeus5.actions.githubusercontent.com`, and `broker.actions.githubusercontent.com`. This means the runner has hardcoded fallback behavior — it always contacts the real GitHub control plane for certain operations (token refresh, broker notifications), even when configured against a different service.

## What runner.server must implement (from this capture)

| Requirement | Source | runner.server status |
|---|---|---|
| Two-phase registration (api.github.com → pipelines) | Flow #1 → #3 | runner.server uses single URL |
| connectionData with `connect` long-poll | Flows #3-5 | runner.server returns stub without connect semantics |
| OAuth2 token endpoint (`/_apis/oauth2/token/{guid}`) | Flows #13, #19, #22 | Not implemented (per fidelity-gap) |
| Agent registration with 201 status | Flow #8 | Returns 200 |
| Session-scoped URL prefix (58-char random) | All pipeline flows | Uses `/runner/server` prefix |
| Message DELETE ack | Long-poll cycle | Not implemented (per fidelity-gap) |
| Broker notifications | broker.actions.githubusercontent.com | Not implemented |

## Runner-server capture: status

Runner-server capture attempted but failed because:
1. The runner sends `POST /api/v3/actions/runner-registration` (Gitea API path) when configured against runner.server — this endpoint doesn't respond
2. Even with runner.server on port 80, the Gitea-style endpoint hangs without response
3. The official runner simultaneously contacts GitHub during runner-server config, creating cross-contamination

## How to reproduce

```sh
cd experiments/mitm && . .venv/bin/activate
GITHUB_OWNER=Bnjoroge1 GITHUB_REPO=Docktree GITHUB_REF=main \
  GITHUB_RUNNER_TOKEN=<fresh-token> \
  bash bin/record.sh --backend official --scenario 01-register-and-idle
```

The capture appears at `captures/official/01-register-and-idle/latest/`.
