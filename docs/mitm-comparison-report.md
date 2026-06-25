# MITM comparison: 01-register-and-idle

**Official backend**: ok — 24 flows
**Runner.server backend**: config_failed — 3 flows

## Endpoint matrix

| method | normalized path | official # | rs # | official mean ms | rs mean ms | official statuses | rs statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/_apis/distributedtask/pools/{n}/messages/{n}?sessionId={volatile}` | 1 | 0 | 1114.3 | - | 200 |  |
| GET | `/_apis/connectionData?connectOptions=0&lastChangeId=6184961&lastChangeId64=6184961` | 10 | 0 | 23.7 | - | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official&includeCapabilities=False` | 1 | 0 | 22.4 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/messages?architecture=ARM64&disableUpdate=false&os=macOS&runnerVersion=2.317.0&sessionId={volatile}&status=Online` | 2 | 0 | 106.3 | - | 200, 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 3 | 0 | 48.1 | - | 200, 200, 401 |  |
| POST | `/_apis/distributedtask/pools/{n}/sessions` | 2 | 0 | 54.0 | - | 200, 401 |  |
| POST | `/_apis/oauth2/token/{guid}` | 3 | 0 | 24.8 | - | 200, 200, 200 |  |
| POST | `/actions/runner-registration` | 1 | 0 | 276.4 | - | 200 |  |
| POST | `/api/v3/actions/runner-registration` | 0 | 3 | - | 0 |  | None, None, None |
| PUT | `/_apis/distributedtask/pools/{n}/agents/{n}` | 1 | 0 | 160.3 | - | 200 |  |

## Missing endpoints

### Official only

- `DELETE /_apis/distributedtask/pools/{n}/messages/{n}?sessionId={volatile}`
- `GET /_apis/connectionData?connectOptions=0&lastChangeId=6184961&lastChangeId64=6184961`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=mitm-official&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/messages?architecture=ARM64&disableUpdate=false&os=macOS&runnerVersion=2.317.0&sessionId={volatile}&status=Online`
- `GET /_apis/distributedtask/pools?poolType=Automation`
- `POST /_apis/distributedtask/pools/{n}/sessions`
- `POST /_apis/oauth2/token/{guid}`
- `POST /actions/runner-registration`
- `PUT /_apis/distributedtask/pools/{n}/agents/{n}`

### Runner.server only

- `POST /api/v3/actions/runner-registration`

_No shared endpoints to compare._
