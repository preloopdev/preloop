# MITM comparison: 01-connection

**official**: captured — 1 flows
**aksh**: captured — 1 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| POST | `/_apis/v1/AgentRequest/{n}/{n}` | 1 | 1 | 1.0 | 4.7 | 204 | 204 |

## Missing endpoints

_No endpoints present only in official._

_No endpoints present only in aksh._

## Per-endpoint comparison

### `POST /_apis/v1/AgentRequest/{n}/{n}`

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 1.0 / aksh 4.7 | p95: official 1.0 / aksh 4.7
