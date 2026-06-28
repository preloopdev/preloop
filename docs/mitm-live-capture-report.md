# MITM experiment — official GitHub control plane capture

**Run**: 2026-06-25T18:46:05Z  
**Backend**: official (`actions/runner` v2.317.0 → GitHub.com)  
**Scenario**: 01-register-and-idle  
**Status**: 24 flows captured successfully
> ⚠️ **Stale baseline**: This capture used runner v2.317.0. The current official runner is
> v2.335.1 and GitHub enforces v2.329.0+ minimum since March 2026. The protocol has changed
> significantly — see `docs/fidelity-gap.md §1a` for the deep diff. Re-captures against
> v2.329.0+ are needed before relying on these findings.

## Captured protocol flow

The unmodified official runner, configured against `github.com/Bnjoroge1/Docktree`, made these calls through mitmproxy:

| # | method | target | endpoint | status | duration (ms) |
|---|---|---|---|---|---|
| 1 | POST | api.github.com | `/actions/runner-registration` | 200 | 276 |
| 2-4 | GET | pipelinesghubeus5 | `/{session}/_apis/connectionData?connect` | 200 | 16–37 |
| 5 | GET | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools?` | 200 | 39 |
| 6 | GET | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools/{n}/agents?` | 200 | 22 |
| 7 | PUT | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools/{n}/agents/{n}` | 200 | 160 |
| 8-11 | GET | pipelinesghubeus5 | `/{session}/_apis/connectionData?connect` | 200 | 17–39 |
| 12 | GET | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools?` | 401 | 84 |
| 13 | POST | tokenghub | `/_apis/oauth2/token/{guid}` | 200 | 25 |
| 14 | GET | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools?` | 200 | 21 |
| 15-17 | GET | pipelinesghubeus5 | `/{session}/_apis/connectionData?connect` | 200 | 19–21 |
| 18 | POST | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools/{n}/sessions` | 401 | 18 |
| 19 | POST | tokenghub | `/_apis/oauth2/token/{guid}` | 200 | 25 |
| 20 | POST | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools/{n}/sessions` | 200 | 89 |
| 21 | GET | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools/{n}/messages?` | 200 | 95 |
| 22 | POST | tokenghub | `/_apis/oauth2/token/{guid}` | 200 | 25 |
| 23 | GET | pipelinesghubeus5 | `/{session}/_apis/connectionData?connect` | 200 | 16 |
| 24 | DELETE | pipelinesghubeus5 | `/{session}/_apis/distributedtask/pools/{n}/messages/{n}` | 200 | 1114 |

## Key discoveries

### 1. The runner contacts github.com FIRST, not the pipelines service

Flow #1 is a `POST` to `api.github.com/actions/runner-registration`. Only AFTER this does the runner talk to `pipelinesghubeus5.actions.githubusercontent.com`. This is a two-phase registration.

### 2. connectionData is fetched 7+ times

The runner doesn't just fetch connectionData once — it re-fetches it at every phase transition: config, agent registration, session creation, and message polling. Each fetch may carry different `connect` query parameters.

### 3. OAuth2 is the auth mechanism

The runner gets bearer tokens from `tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}`. When a 401 is returned (flow #12), it re-authenticates before proceeding (flow #13). This confirms `fidelity-gap.md` §2.1: OAuth/OIDC is mandatory.

### 4. The session prefix is a long random string

The official runner's session path segment is 58 characters (`PuoS6BwhaJy073Fs8tfAA75cfJtVulJXwl1KDQ1EIvj3eC3do0`). This is NOT a GUID — it's likely a base64-encoded token or tenant identifier. runner.server uses GUIDs.

### 5. Agent registration uses PUT, not POST; returns 200 not 201

Flow #7 (agent PUT to `pools/{n}/agents/{n}`) returns status `200`, establishing the agent in the pool. Earlier analysis showed a `201` response, but the canonical form is `PUT` to `pools/{poolId}/agents/{agentId}`. The runner then re-fetches connectionData before proceeding to session creation.

### 6. The runner contacts GitHub even when configured against runner.server

Even when `--url http://192.168.1.221/runner/server` was passed, the official runner still made calls to `api.github.com`, `pipelinesghubeus5.actions.githubusercontent.com`, and `broker.actions.githubusercontent.com`. This means the runner has hardcoded fallback behavior — it always contacts the real GitHub control plane for certain operations (token refresh, broker notifications), even when configured against a different service.

| Requirement | Source | runner.server status |
|---|---|---|
| Two-phase registration (api.github.com → pipelines) | Flow #1 → #2 | runner.server uses single URL |
| connectionData with `connect` long-poll | Flows #2-4, #8-11, #15-17 | runner.server returns stub without connect semantics |
| OAuth2 token endpoint (`/_apis/oauth2/token/{guid}`) | Flows #13, #19, #22 | Not implemented (per fidelity-gap) |
| Agent registration with PUT semantics | Flow #7 | Uses different path structure |
| Session-scoped URL prefix (58-char random) | All pipeline flows | Uses `/runner/server` prefix |
| Message DELETE ack | Flow #24 | Not implemented (per fidelity-gap) |

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
