# MITM comparison: 10-uses-checkout

**official**: ok — 36 flows
**aksh**: N/A — 45 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/session` | 0 | 1 | - | 34.4 |  | 204 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1` | 0 | 1 | - | 19.1 |  | 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 6 | 5 | 36.6 | 21.0 | 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-10-uses-checkout-1783540919&includeCapabilities=False` | 0 | 1 | - | 22.8 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official-10-uses-checkout-2026-06-30T16-07-16Z&includeCapabilities=False` | 1 | 0 | 21.5 | - | 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 19.7 | 106.8 | 200 | 200 |
| GET | `/_ws/ingest.sock` | 0 | 1 | - | 112.4 |  | 401 |
| GET | `/actions/checkout/tar.gz/***REDACTED***` | 1 | 1 | 144.0 | 177.1 | 200 | 200 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 | - | 2734.6 |  | 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 | - | 3339.5 |  | 200 |
| GET | `/health` | 2 | 2 | 75.2 | 111.5 | 200, 200 | 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 0 |  | None |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 2 | 0 | 25014.9 | - | 202, None |  |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 7187.9 |  | 200 |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 2 | 0 | 1333.3 | - | 200, None |  |
| GET | `/ready` | 1 | 1 | 18.1 | 105.9 | 204 | 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 153.7 | 94.5 | 200 | 200 |
| POST | `/_apis/oauth2/token` | 0 | 2 | - | 121.9 |  | 200, 200 |
| POST | `/_apis/oauth2/token/{guid}` | 2 | 0 | 25.0 | - | 200, 200 |  |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 0 | 1 | - | 139.9 |  | 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 1 | 0 | 59.1 | - | 200 |  |
| POST | `/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 1 | 1 | 97.2 | 161.4 | 200 | 200 |
| POST | `/actions/runner-registration` | 1 | 1 | 184.4 | 187.2 | 200 | 200 |
| POST | `/session` | 1 | 1 | 34.5 | 127.9 | 201 | 201 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 5 | 2 | 60.1 | 80.3 | 200, 200, 200, 200, 200 | 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 0 | 1 | - | 132.9 |  | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 0 | 4 | - | 80.7 |  | 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 | 29.2 | 119.1 | 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 4 | 4 | 32.5 | 99.3 | 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/{n}/acquirejob` | 1 | 1 | 417.5 | 494.6 | 200 | 200 |
| POST | `/{n}/completejob` | 1 | 1 | 38.1 | 34.6 | 204 | 204 |
| POST | `/{n}/renewjob` | 1 | 1 | 44.0 | 41.7 | 200 | 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A02%3A52Z&sig=UXKJnygT2G%2BK6KWcg3%2BNM9VRNfD7OGajXfMSj7szbrk%3D&ske=2026-07-08T21%3A11%3A09Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A11%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A47Z&sv=2025-11-05` | 0 | 1 | - | 31.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-__post_{guid}.txt?se=2026-07-08T21%3A02%3A51Z&sig=4XjcM9kSSuwa59u6E%2FJRxnhRYhdm8%2BcJwtMLrEwg0FQ%3D&ske=2026-07-08T21%3A10%3A57Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A10%3A57Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A46Z&sv=2025-11-05` | 0 | 1 | - | 45.5 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A02%3A17Z&sig=IIIN5x0Osr3%***REDACTED***%3D&ske=2026-07-08T21%3A12%3A45Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A12%3A45Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A12Z&sv=2025-11-05` | 0 | 1 | - | 34.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A02%3A51Z&sig=***REDACTED***%3D&ske=2026-07-08T21%3A10%3A55Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A10%3A55Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A46Z&sv=2025-11-05` | 0 | 1 | - | 36.6 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A02%3A52Z&sig=iY3sHXP8n%***REDACTED***%3D&ske=2026-07-08T21%3A10%3A56Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A10%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A47Z&sv=2025-11-05` | 0 | 1 | - | 79.8 |  | 201 |

## Missing endpoints

### official only

- `GET /_apis/distributedtask/pools/{n}/agents?agentName=mitm-official-10-uses-checkout-2026-06-30T16-07-16Z&includeCapabilities=False`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token/{guid}`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64`

### aksh only

- `DELETE /session`
- `GET /_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-10-uses-checkout-1783540919&includeCapabilities=False`
- `GET /_ws/ingest.sock`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`
- `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`
- `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A02%3A52Z&sig=UXKJnygT2G%2BK6KWcg3%2BNM9VRNfD7OGajXfMSj7szbrk%3D&ske=2026-07-08T21%3A11%3A09Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A11%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A47Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-__post_{guid}.txt?se=2026-07-08T21%3A02%3A51Z&sig=4XjcM9kSSuwa59u6E%2FJRxnhRYhdm8%2BcJwtMLrEwg0FQ%3D&ske=2026-07-08T21%3A10%3A57Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A10%3A57Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A46Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A02%3A17Z&sig=IIIN5x0Osr3%***REDACTED***%3D&ske=2026-07-08T21%3A12%3A45Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A12%3A45Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A12Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A02%3A51Z&sig=***REDACTED***%3D&ske=2026-07-08T21%3A10%3A55Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A10%3A55Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A46Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A02%3A52Z&sig=iY3sHXP8n%***REDACTED***%3D&ske=2026-07-08T21%3A10%3A56Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A10%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A47Z&sv=2025-11-05`

## Per-endpoint comparison

### `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`

**Header key differences:**

- official only: `{'accept-language', 'x-tfs-fedauthredirect', 'accept-encoding', 'authorization'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -5,8 +5,8 @@
   "locationServiceData": {
     "clientCacheFresh": true,
     "defaultAccessMappingMoniker": "ScaleUnitMapping",
-    "lastChangeId": 13922305,
-    "lastChangeId64": 13922305,
+    "lastChangeId": 14049281,
+    "lastChangeId64": 14049281,
     "serviceOwner": "0000005a-0000-8888-8000-000000000000"
   }
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200]

**Timing (ms):** p50: official 22.0 / aksh 21.4 | p95: official 113.9 / aksh 21.7

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{'accept-language', 'x-tfs-fedauthredirect', 'accept-encoding'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -10,7 +10,7 @@
       "isInternal": true,
       "name": "Default",
       "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 6,
+      "size": 0,
       "targetSize": null
     },
     {
@@ -22,7 +22,7 @@
       "isInternal": false,
       "name": "GitHub Actions",
       "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 1,
+      "size": 20,
       "targetSize": 1
     }
   ]
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 19.7 / aksh 106.8 | p95: official 19.7 / aksh 106.8

### `GET /actions/checkout/tar.gz/***REDACTED***`

**Header key differences:**

- official only: `{'authorization'}`
- aksh only: `{'accept'}`

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 144.0 / aksh 177.1 | p95: official 144.0 / aksh 177.1

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 126.4 / aksh 122.7 | p95: official 126.4 / aksh 122.7

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 18.1 / aksh 105.9 | p95: official 18.1 / aksh 105.9

### `POST /_apis/distributedtask/pools/{n}/agents`

**Header key differences:**

- official only: `{'accept-language', 'x-tfs-fedauthredirect', 'accept-encoding'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,12 +2,12 @@
   "authorization": {
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "***REDACTED***/1NK1hnsD8TQV3d0wYmhMk2h/zpOPuWfu+iV+quyI8V4GnUFEdGyiE8S/rvd/***REDACTED***/ClsIVWGTmK1yWYR+NzCjKWWjQyBuJO7Pnd0x+***REDACTED***+eFceOjE911XfmUfOZ4kFqM6W1EI8P+PlvYi7wDLUZ8wWbh+mkovO110pG0iOQAwve72GaA/K+/A6yCbPb7Kw=="
+      "modulus": "rfKwxawANYKkSlXW/JPbzuPBrIKfZow+0kUAjG9RYbLh6JB7O/***REDACTED***+MfpK+uSLtZBx3ozINxpN3wb9/xj22oI/***REDACTED***/8oq/umpSGR7e0oWtW/***REDACTED***/hEKH4PBVu/bUnVqTpgYIOkf+3M+9Y3OzcEdWP384syJ+***REDACTED***=="
     }
   },
   "createdOn": "0001-01-01T00:00:00",
-  "disableUpdate": false,
-  "ephemeral": false,
+  "disableUpdate": true,
+  "ephemeral": true,
   "id": 0,
   "labels": [
     {
@@ -17,7 +17,7 @@
     },
     {
       "id": 0,
-      "name": "macOS",
+      "name": "Linux",
       "type": "system"
     },
     {
@@ -27,13 +27,28 @@
     },
     {
       "id": 0,
+      "name": "self-hosted",
+      "type": "user"
+    },
+    {
+      "id": 0,
+      "name": "linux",
+      "type": "user"
+    },
+    {
+      "id": 0,
+      "name": "x64",
+      "type": "user"
+    },
+    {
+      "id": 0,
       "name": "mitm",
       "type": "user"
     }
   ],
   "maxParallelism": 1,
-  "name": "mitm-official-10-uses-checkout-2026-06-30T16-07-16Z",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-10-uses-checkout-1783540919",
+  "osDescription": "linux aarch64",
   "provisioningState": "Provisioned",
   "status": 0,
   "version": "2.335.1"
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,45 +1,50 @@
 {
   "authorization": {
-    "authorizationUrl": "https://tokenghub.actions.githubusercontent.com/_apis/oauth2/token/5e4d430c-d710-4b62-aed8-555ffd0f7592",
-    "clientId": "40f8c974-691f-470d-8345-62a8b036990b",
+    "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/_apis/oauth2/token",
+    "clientId": "74558aa1-69d6-4e3d-9f37-30ef9e30ae3f",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "***REDACTED***/1NK1hnsD8TQV3d0wYmhMk2h/zpOPuWfu+iV+quyI8V4GnUFEdGyiE8S/rvd/***REDACTED***/ClsIVWGTmK1yWYR+NzCjKWWjQyBuJO7Pnd0x+***REDACTED***+eFceOjE911XfmUfOZ4kFqM6W1EI8P+PlvYi7wDLUZ8wWbh+mkovO110pG0iOQAwve72GaA/K+/A6yCbPb7Kw=="
+      "modulus": "rfKwxawANYKkSlXW/JPbzuPBrIKfZow+0kUAjG9RYbLh6JB7O/***REDACTED***+MfpK+uSLtZBx3ozINxpN3wb9/xj22oI/***REDACTED***/8oq/umpSGR7e0oWtW/***REDACTED***/hEKH4PBVu/bUnVqTpgYIOkf+3M+9Y3OzcEdWP384syJ+***REDACTED***=="
     }
   },
-  "createdOn": "2026-06-30T16:07:26.48Z",
+  "createdOn": "2026-07-08T20:02:00.57Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
-  "ephemeral": false,
-  "id": 13,
+  "ephemeral": true,
+  "id": 680,
   "isElastic": false,
   "isVirtual": false,
   "labels": [
     {
-      "id": 1,
+      "id": 37,
       "name": "self-hosted",
       "type": "system"
     },
     {
-      "id": 2,
-      "name": "macOS",
+      "id": 38,
+      "name": "Linux",
       "type": "system"
     },
     {
-      "id": 3,
+      "id": 39,
       "name": "ARM64",
       "type": "system"
     },
     {
-      "id": 4,
+      "id": 40,
+      "name": "x64",
+      "type": "user"
+    },
+    {
+      "id": 41,
       "name": "mitm",
       "type": "user"
     }
   ],
   "maxParallelism": 1,
-  "name": "mitm-official-10-uses-checkout-2026-06-30T16-07-16Z",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-10-uses-checkout-1783540919",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -60,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-13",
+  "queueName": "taskagent-680",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 153.7 / aksh 94.5 | p95: official 153.7 / aksh 94.5

### `POST /actions/build/{guid}/jobs/{guid}/runnerresolve/actions`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

_identical_

**Response body diff:**

_identical_

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 97.2 / aksh 161.4 | p95: official 97.2 / aksh 161.4

### `POST /actions/runner-registration`

**Header key differences:**

- aksh only: `{'accept'}`

**Request body diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "token": "***REDACTED***",
+  "token": "***REDACTED***",
   "token_schema": "OAuthAccessToken",
   "url": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 184.4 / aksh 187.2 | p95: official 184.4 / aksh 187.2

### `POST /session`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,14 +1,14 @@
 {
   "agent": {
     "ephemeral": null,
-    "id": 13,
-    "name": "mitm-official-10-uses-checkout-2026-06-30T16-07-16Z",
-    "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+    "id": 680,
+    "name": "aksh-capture-10-uses-checkout-1783540919",
+    "osDescription": "linux aarch64",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "Nuraydias-Mac-Studio (PID: 51687)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 322)",
+  "sessionId": "66ad2bd0-a6bf-4082-b887-95c97e50cc3e",
   "useFipsEncryption": false
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,6 +1,6 @@
 {
   "assignmentQueued": false,
   "orchestrationId": "",
-  "ownerName": "Nuraydias-Mac-Studio (PID: 51687)",
-  "sessionId": "ed058bfd-6364-4f8d-a161-a608da63cd5a"
+  "ownerName": "container (PID: 322)",
+  "sessionId": "579a28a9-f380-47ac-8ef4-d5743c11bde8"
 }
```

**Status codes:** official: [201] | aksh: [201]

**Timing (ms):** p50: official 34.5 / aksh 127.9 | p95: official 34.5 / aksh 127.9

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,15 +2,24 @@
   "change_order": 1,
   "steps": [
     {
-      "completed_at": null,
-      "conclusion": 0,
-      "external_id": "b128d954-b38a-497d-9eb4-2fab761f23c1",
+      "completed_at": "2026-07-08T20:02:17.113Z",
+      "conclusion": 2,
+      "external_id": "e79777bd-7e53-4ff9-9179-452f76ea01e1",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T16:07:31.334Z",
-      "status": 3
+      "started_at": "2026-07-08T20:02:17.113Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-08T20:02:51.431Z",
+      "conclusion": 3,
+      "external_id": "07fe06ab-4e4e-4f33-85b3-885fbdfee036",
+      "name": "actions/checkout@v4",
+      "number": 2,
+      "started_at": "2026-07-08T20:02:17.797Z",
+      "status": 6
     }
   ],
-  "workflow_job_run_backend_id": "78b9a738-12ff-5193-bb90-edbf331a95f9",
-  "workflow_run_backend_id": "b542cff1-bb6a-41e2-9435-e62c1755362d"
+  "workflow_job_run_backend_id": "aaee4f87-9706-518f-850e-8281bd1f29eb",
+  "workflow_run_backend_id": "5ece732c-0b25-43de-84f0-2a022fb0f149"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 41.5 / aksh 106.9 | p95: official 133.3 / aksh 106.9

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "78b9a738-12ff-5193-bb90-edbf331a95f9",
-  "workflow_run_backend_id": "b542cff1-bb6a-41e2-9435-e62c1755362d"
+  "workflow_job_run_backend_id": "aaee4f87-9706-518f-850e-8281bd1f29eb",
+  "workflow_run_backend_id": "5ece732c-0b25-43de-84f0-2a022fb0f149"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa13.blob.core.windows.net/actions-results/b542cff1-bb6a-41e2-9435-e62c1755362d/workflow-job-run-78b9a738-12ff-5193-bb90-edbf331a95f9/logs/job/job-logs.txt?se=2026-06-30T17%3A08%3A15Z&sig=***REDACTED***%3D&ske=2026-06-30T19%3A51%3A27Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A51%3A27Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T16%3A08%3A10Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa5.blob.core.windows.net/actions-results/5ece732c-0b25-43de-84f0-2a022fb0f149/workflow-job-run-aaee4f87-9706-518f-850e-8281bd1f29eb/logs/job/job-logs.txt?se=2026-07-08T21%3A02%3A52Z&sig=UXKJnygT2G%2BK6KWcg3%2BNM9VRNfD7OGajXfMSj7szbrk%3D&ske=2026-07-08T21%3A11%3A09Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T17%3A11%3A09Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A47Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 29.2 / aksh 119.1 | p95: official 29.2 / aksh 119.1

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "b128d954-b38a-497d-9eb4-2fab761f23c1",
-  "workflow_job_run_backend_id": "78b9a738-12ff-5193-bb90-edbf331a95f9",
-  "workflow_run_backend_id": "b542cff1-bb6a-41e2-9435-e62c1755362d"
+  "step_backend_id": "e79777bd-7e53-4ff9-9179-452f76ea01e1",
+  "workflow_job_run_backend_id": "aaee4f87-9706-518f-850e-8281bd1f29eb",
+  "workflow_run_backend_id": "5ece732c-0b25-43de-84f0-2a022fb0f149"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa13.blob.core.windows.net/actions-results/b542cff1-bb6a-41e2-9435-e62c1755362d/workflow-job-run-78b9a738-12ff-5193-bb90-edbf331a95f9/logs/steps/step-logs-b128d954-b38a-497d-9eb4-2fab761f23c1.txt?se=2026-06-30T17%3A07%3A32Z&sig=qb5SEx%2BXX4u2KSEfZDM1xVjodS%2BcLTwyomruFhlv1Rg%3D&ske=2026-06-30T19%3A51%3A24Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A51%3A24Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T16%3A07%3A27Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa5.blob.core.windows.net/actions-results/5ece732c-0b25-43de-84f0-2a022fb0f149/workflow-job-run-aaee4f87-9706-518f-850e-8281bd1f29eb/logs/steps/step-logs-e79777bd-7e53-4ff9-9179-452f76ea01e1.txt?se=2026-07-08T21%3A02%3A17Z&sig=IIIN5x0Osr3%***REDACTED***%3D&ske=2026-07-08T21%3A12%3A45Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T17%3A12%3A45Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A02%3A12Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 33.5 / aksh 117.1 | p95: official 36.1 / aksh 120.9

### `POST /{n}/acquirejob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "billingOwnerId": "O_kgDOEbddog",
-  "jobMessageId": "78b9a738-12ff-5193-bb90-edbf331a95f9",
-  "runnerOS": "macOS"
+  "jobMessageId": "aaee4f87-9706-518f-850e-8281bd1f29eb",
+  "runnerOS": "Linux"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -9,7 +9,7 @@
         },
         {
           "k": "sha",
-          "v": "***REDACTED***"
+          "v": "***REDACTED***"
         },
         {
           "k": "repository",
@@ -29,11 +29,11 @@
         },
         {
           "k": "run_id",
-          "v": "28458579513"
+          "v": "28971869955"
         },
         {
           "k": "run_number",
-          "v": "2"
+          "v": "18"
         },
         {
           "k": "retention_days",
@@ -49,7 +49,7 @@
         },
         {
           "k": "repository_visibility",
-          "v": "private"
+          "v": "public"
         },
         {
           "k": "actor_id",
@@ -141,7 +141,7 @@
                     },
                     {
                       "k": "private",
-                      "v": true
+                      "v": false
                     },
                     {
                       "k": "owner",
@@ -393,11 +393,11 @@
                     },
                     {
                       "k": "updated_at",
-                      "v": "2026-06-30T15:39:53Z"
+                      "v": "2026-07-08T13:47:43Z"
                     },
                     {
                       "k": "pushed_at",
-                      "v": "2026-06-30T15:36:45Z"
+                      "v": "2026-07-08T13:46:20Z"
                     },
                     {
                       "k": "git_url",
@@ -421,7 +421,7 @@
                     },
                     {
                       "k": "size",
-                      "v": 0
+                      "v": 127
                     },
                     {
                       "k": "stargazers_count",
@@ -485,7 +485,7 @@
                     },
                     {
                       "k": "allow_forking",
-                      "v": false
+                      "v": true
                     },
                     {
                       "k": "is_template",
@@ -512,7 +512,7 @@
                     },
                     {
                       "k": "visibility",
-                      "v": "private"
+                      "v": "public"
                     },
                     {
                       "k": "forks",
@@ -691,7 +691,7 @@
         },
         {
           "k": "workflow_sha",
-          "v": "***REDACTED***"
+          "v": "***REDACTED***"
         },
         {
           "k": "repository_id",
@@ -712,7 +712,7 @@
       "d": [
         {
           "k": "check_run_id",
-          "v": 84339982990
+          "v": 85969310399
         },
         {
           "k": "workflow_ref",
@@ -720,7 +720,7 @@
         },
         {
           "k": "workflow_sha",
-          "v": "***REDACTED***"
+          "v": "***REDACTED***"
         },
         {
           "k": "workflow_repository",
@@ -771,7 +771,7 @@
   ],
   "jobContainer": null,
   "jobDisplayName": "build",
-  "jobId": "78b9a738-12ff-5193-bb90-edbf331a95f9",
+  "jobId": "aaee4f87-9706-518f-850e-8281bd1f29eb",
   "jobName": "__default",
   "jobOutputs": null,
   "jobServiceContainers": null,
@@ -851,34 +851,30 @@
     },
     {
       "type": "regex",
-      "value": "***REDACTED***\\.***REDACTED***"
-    },
-    {
-      "type": "regex",
-      "value": "***REDACTED***"
-    },
-    {
-      "type": "regex",
-      "value": "***REDACTED***"
-    },
-    {
-      "type": "regex",
-      "value": "***REDACTED***"
-    },
-    {
-      "type": "regex",
-      "value": "***REDACTED***\\.***REDACTED***"
-    },
-    {
-      "type": "regex",
-      "value": "GS4dajBzZAbW063CoG6pS-e__zcmNyXYtThRVdXpCoIUg"
+      "value": "***REDACTED***\\.***REDACTED***"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***\\.mnME-1p3p6FdrgP"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***"
     }
   ],
   "messageType": "RunnerJobRequest",
   "plan": {
     "artifactLocation": "",
     "artifactUri": "",
-    "planId": "b542cff1-bb6a-41e2-9435-e62c1755362d",
+    "planId": "5ece732c-0b25-43de-84f0-2a022fb0f149",
     "planType": "actions",
     "version": 0
   },
@@ -888,7 +884,7 @@
       {
         "authorization": {
           "parameters": {
-            "AccessToken": "***REDACTED***"
+            "AccessToken": "***REDACTED***"
           },
           "scheme": "OAuth"
         },
@@ -905,7 +901,7 @@
         "isReady": true,
         "isShared": false,
         "name": "SystemVssConnection",
-        "url": "https://run-actions-2-azure-eastus.actions.githubusercontent.com/90/"
+        "url": "https://run-actions-1-azure-eastus.actions.githubusercontent.com/138/"
       }
     ]
   },
@@ -915,7 +911,7 @@
       "condition": "success()",
       "contextName": "__actions_checkout",
       "continueOnError": null,
-      "id": "938019a6-95d6-4417-9d07-f7a9add2bcba",
+      "id": "07fe06ab-4e4e-4f33-85b3-885fbdfee036",
       "name": "__actions_checkout",
       "reference": {
         "name": "actions/checkout",
@@ -930,7 +926,7 @@
       "condition": "success()",
       "contextName": "__run",
       "continueOnError": null,
-      "id": "dee90d70-b242-4fd6-a015-9e36e35cbf03",
+      "id": "4a96de01-57e1-4299-8ea9-4f3ac8e3a726",
       "inputs": {
         "map": [
           {
@@ -959,7 +955,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "b542cff1-bb6a-41e2-9435-e62c1755362d",
+    "id": "5ece732c-0b25-43de-84f0-2a022fb0f149",
     "location": null
   },
   "variables": {
@@ -1079,7 +1075,7 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
@@ -1098,13 +1094,13 @@
     },
     "system.github.token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.github.token.permissions": {
       "value": "{\"Contents\":\"read\",\"Metadata\":\"read\",\"Packages\":\"read\"}"
     },
     "system.orchestrationId": {
-      "value": "b542cff1-bb6a-41e2-9435-e62c1755362d.build.__default"
+      "value": "5ece732c-0b25-43de-84f0-2a022fb0f149.build.__default"
     },
     "system.phaseDisplayName": {
       "value": "build"
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 417.5 / aksh 494.6 | p95: official 417.5 / aksh 494.6

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,138 +2,83 @@
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
   "conclusion": "failed",
-  "jobId": "78b9a738-12ff-5193-bb90-edbf331a95f9",
+  "jobId": "aaee4f87-9706-518f-850e-8281bd1f29eb",
   "outputs": {},
-  "planId": "b542cff1-bb6a-41e2-9435-e62c1755362d",
+  "planId": "5ece732c-0b25-43de-84f0-2a022fb0f149",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-06-30T16:07:31.86091Z",
+      "completed_at": "2026-07-08T20:02:52.589Z",
       "conclusion": "succeeded",
-      "external_id": "b128d954-b38a-497d-9eb4-2fab761f23c1",
+      "external_id": "e79777bd-7e53-4ff9-9179-452f76ea01e1",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T16:07:31.334125Z",
+      "started_at": "2026-07-08T20:02:52.589Z",
       "status": "completed",
       "type": "runner"
     },
     {
-      "action_name": "actions/checkout",
+      "action_name": "actions/checkout@v4",
       "annotations": [
         {
-          "endLine": 37,
+          "endLine": 1,
           "level": "failure",
-          "message": "ambiguous argument 'HEAD': unknown revision or path not in the working tree.",
-          "startLine": 37,
-          "stepNumber": 2
-        },
-        {
-          "endLine": 41,
-          "level": "warning",
-          "message": "Unable to clean or reset the repository. The repository will be recreated instead.",
-          "startLine": 41,
-          "stepNumber": 2
-        },
-        {
-          "endLine": 62,
-          "level": "failure",
-          "message": "unable to access 'https://github.com/preloopdev/aksh-conformance-sample/': SSL certificate problem: unable to get local issuer certificate",
-          "startLine": 62,
-          "stepNumber": 2
-        },
-        {
-          "endLine": 66,
-          "level": "failure",
-          "message": "unable to access 'https://github.com/preloopdev/aksh-conformance-sample/': SSL certificate problem: unable to get local issuer certificate",
-          "startLine": 66,
-          "stepNumber": 2
-        },
-        {
-          "endLine": 70,
-          "level": "failure",
-          "message": "unable to access 'https://github.com/preloopdev/aksh-conformance-sample/': SSL certificate problem: unable to get local issuer certificate",
-          "startLine": 70,
-          "stepNumber": 2
-        },
-        {
-          "endLine": 71,
-          "level": "failure",
-          "message": "The process '/opt/homebrew/bin/git' failed with exit code 128",
-          "startLine": 71,
+          "message": "node action exited with code 1",
+          "startLine": 1,
           "stepNumber": 2
         }
       ],
-      "completed_at": "2026-06-30T16:07:56.508295Z",
+      "completed_at": "2026-07-08T20:02:52.589Z",
       "conclusion": "failed",
-      "external_id": "938019a6-95d6-4417-9d07-f7a9add2bcba",
-      "name": "Run actions/checkout@v4",
+      "external_id": "07fe06ab-4e4e-4f33-85b3-885fbdfee036",
+      "name": "actions/checkout@v4",
       "number": 2,
-      "ref": "v4",
-      "started_at": "2026-06-30T16:07:31.869053Z",
+      "started_at": "2026-07-08T20:02:52.589Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
+      "action_name": "sh",
       "annotations": [],
-      "completed_at": "2026-06-30T16:07:56.510374Z",
+      "completed_at": "2026-07-08T20:02:52.589Z",
       "conclusion": "skipped",
-      "external_id": "dee90d70-b242-4fd6-a015-9e36e35cbf03",
+      "external_id": "4a96de01-57e1-4299-8ea9-4f3ac8e3a726",
       "name": "Run echo checked-out",
       "number": 3,
-      "started_at": "2026-06-30T16:07:56.509991Z",
-      "status": "completed"
+      "started_at": "2026-07-08T20:02:52.589Z",
+      "status": "completed",
+      "type": "run"
     },
     {
-      "action_name": "actions/checkout",
+      "action_name": "actions/checkout@v4",
       "annotations": [],
-      "completed_at": "2026-06-30T16:07:56.735183Z",
+      "completed_at": "2026-07-08T20:02:52.589Z",
       "conclusion": "succeeded",
-      "external_id": "ee794d33-05b4-495f-8527-134fc8f779ae",
-      "name": "Post Run actions/checkout@v4",
-      "number": 6,
-      "ref": "v4",
-      "started_at": "2026-06-30T16:07:56.511811Z",
+      "external_id": "__post_07fe06ab-4e4e-4f33-85b3-885fbdfee036",
+      "name": "Post actions/checkout@v4",
+      "number": 4,
+      "started_at": "2026-07-08T20:02:52.589Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
       "action_name": "complete_job",
-      "annotations": [
-        {
-          "endLine": 2,
-          "level": "warning",
-          "message": "Node.js 20 is deprecated. The following actions target Node.js 20 but are being forced to run on Node.js 24: actions/checkout@v4. For more information see: https://github.blog/changelog/2025-09-19-deprecation-of-node-20-on-github-actions-runners/",
-          "startLine": 2,
-          "stepNumber": 7
-        }
-      ],
-      "completed_at": "2026-06-30T16:07:56.754843Z",
+      "annotations": [],
+      "completed_at": "2026-07-08T20:02:52.589Z",
       "conclusion": "succeeded",
-      "external_id": "469e4b4b-93e0-485e-9c44-2fc34a614787",
+      "external_id": "6a3bac6c-e787-46c1-8306-91a19d6b505e",
       "name": "Complete job",
-      "number": 7,
-      "started_at": "2026-06-30T16:07:56.738055Z",
+      "number": 5,
+      "started_at": "2026-07-08T20:02:52.589Z",
       "status": "completed",
       "type": "runner"
     }
   ],
   "telemetry": [
     {
-      "message": "Action archive cache usage: actions/checkout@***REDACTED*** use cache False has cache False",
-      "type": "General"
-    },
-    {
-      "message": "https://broker.actions.githubusercontent.com/health: OK",
-      "type": "ConnectivityCheck"
-    },
-    {
-      "message": "https://token.actions.githubusercontent.com/ready: NoContent",
-      "type": "ConnectivityCheck"
-    },
-    {
-      "message": "https://run.actions.githubusercontent.com/health: OK",
-      "type": "ConnectivityCheck"
+      "message": "{\"ClassType\":\"StepsRunner\",\"FinishResult\":\"failed\"}",
+      "type": "task"
     }
   ]
 }
```

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 38.1 / aksh 34.6 | p95: official 38.1 / aksh 34.6

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "78b9a738-12ff-5193-bb90-edbf331a95f9",
-  "planId": "b542cff1-bb6a-41e2-9435-e62c1755362d"
+  "jobId": "aaee4f87-9706-518f-850e-8281bd1f29eb",
+  "planId": "5ece732c-0b25-43de-84f0-2a022fb0f149"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-06-30T16:17:31.132197902Z"
+  "lockedUntil": "2026-07-08T20:12:17.126926317Z"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 44.0 / aksh 41.7 | p95: official 44.0 / aksh 41.7
