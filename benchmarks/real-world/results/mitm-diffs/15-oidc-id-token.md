# MITM comparison: 15-oidc-id-token

**official**: ok — 30 flows
**aksh**: N/A — 40 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/session` | 0 | 1 | - | 31.2 |  | 204 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1` | 0 | 1 | - | 18.1 |  | 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 6 | 5 | 67.4 | 18.4 | 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-15-oidc-id-token-1783541832&includeCapabilities=False` | 0 | 1 | - | 20.0 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official-15-oidc-id-token-2026-06-30T16-05-05Z&includeCapabilities=False` | 1 | 0 | 52.1 | - | 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 21.3 | 22.0 | 200 | 200 |
| GET | `/_ws/ingest.sock` | 0 | 1 | - | 43.8 |  | 401 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 | - | 2691.9 |  | 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 | - | 3333.2 |  | 200 |
| GET | `/health` | 2 | 2 | 22.1 | 83.6 | 200, 200 | 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 0 |  | None |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 1 | 0 | 0 | - | None |  |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 6425.7 |  | 200 |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 2 | 0 | 1414.8 | - | 200, None |  |
| GET | `/ready` | 1 | 1 | 20.5 | 32.5 | 204 | 204 |
| GET | `/{n}//idtoken/{guid}/{guid}?api-version=2.0&audience=api://aksh` | 1 | 0 | 67.3 | - | 200 |  |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 234.3 | 61.1 | 200 | 200 |
| POST | `/_apis/oauth2/token` | 0 | 2 | - | 81.0 |  | 200, 200 |
| POST | `/_apis/oauth2/token/{guid}` | 2 | 0 | 26.6 | - | 200, 200 |  |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 0 | 1 | - | 136.5 |  | 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 1 | 0 | 89.8 | - | 200 |  |
| POST | `/actions/runner-registration` | 1 | 1 | 181.6 | 206.4 | 200 | 200 |
| POST | `/session` | 1 | 1 | 53.7 | 36.4 | 201 | 201 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 2 | 2 | 90.3 | 53.8 | 200, 200 | 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 0 | 1 | - | 55.9 |  | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 0 | 3 | - | 64.2 |  | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 | 31.2 | 41.4 | 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 3 | 3 | 39.5 | 65.4 | 200, 200, 200 | 200, 200, 200 |
| POST | `/{n}/acquirejob` | 1 | 1 | 549.8 | 503.7 | 200 | 200 |
| POST | `/{n}/completejob` | 1 | 1 | 136.2 | 46.4 | 204 | 204 |
| POST | `/{n}/renewjob` | 1 | 1 | 38.3 | 49.4 | 200 | 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A17%3A30Z&sig=KzA6v0rKazIc7u0b4%2FMQC0tv1HjYHOkbrmTYP4Xl8uU%3D&ske=2026-07-09T00%3A09%3A43Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A17%3A25Z&sv=2025-11-05` | 0 | 1 | - | 77.2 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A17%3A29Z&sig=4LEgapq7ueciVe0ATAym%2FKV%2BVuKSCRMmRBKpr2vFmak%3D&ske=2026-07-09T00%3A10%3A48Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A48Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A17%3A24Z&sv=2025-11-05` | 0 | 1 | - | 44.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A17%3A29Z&sig=8N4F3El9cWMHU9h8UqVcx9jk%2Bk%2FiJctliK9kkZgdqmI%3D&ske=2026-07-09T00%3A10%3A16Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A16Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A17%3A24Z&sv=2025-11-05` | 0 | 1 | - | 85.5 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A17%3A29Z&sig=uyG6eA0Nkuke%***REDACTED***%3D&ske=2026-07-09T00%3A09%3A35Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A35Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A17%3A24Z&sv=2025-11-05` | 0 | 1 | - | 84.7 |  | 201 |

## Missing endpoints

### official only

- `GET /_apis/distributedtask/pools/{n}/agents?agentName=mitm-official-15-oidc-id-token-2026-06-30T16-05-05Z&includeCapabilities=False`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `GET /{n}//idtoken/{guid}/{guid}?api-version=2.0&audience=api://aksh`
- `POST /_apis/oauth2/token/{guid}`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64`

### aksh only

- `DELETE /session`
- `GET /_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-15-oidc-id-token-1783541832&includeCapabilities=False`
- `GET /_ws/ingest.sock`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`
- `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`
- `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A17%3A30Z&sig=KzA6v0rKazIc7u0b4%2FMQC0tv1HjYHOkbrmTYP4Xl8uU%3D&ske=2026-07-09T00%3A09%3A43Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A17%3A25Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A17%3A29Z&sig=4LEgapq7ueciVe0ATAym%2FKV%2BVuKSCRMmRBKpr2vFmak%3D&ske=2026-07-09T00%3A10%3A48Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A48Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A17%3A24Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A17%3A29Z&sig=8N4F3El9cWMHU9h8UqVcx9jk%2Bk%2FiJctliK9kkZgdqmI%3D&ske=2026-07-09T00%3A10%3A16Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A16Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A17%3A24Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A17%3A29Z&sig=uyG6eA0Nkuke%***REDACTED***%3D&ske=2026-07-09T00%3A09%3A35Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A35Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A17%3A24Z&sv=2025-11-05`

## Per-endpoint comparison

### `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`

**Header key differences:**

- official only: `{'accept-language', 'authorization', 'accept-encoding', 'x-tfs-fedauthredirect'}`

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

**Timing (ms):** p50: official 107.5 / aksh 19.0 | p95: official 110.4 / aksh 19.6

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{'accept-language', 'accept-encoding', 'x-tfs-fedauthredirect'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -10,7 +10,7 @@
       "isInternal": true,
       "name": "Default",
       "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 5,
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

**Timing (ms):** p50: official 21.3 / aksh 22.0 | p95: official 21.3 / aksh 22.0

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 22.4 / aksh 137.5 | p95: official 22.4 / aksh 137.5

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 20.5 / aksh 32.5 | p95: official 20.5 / aksh 32.5

### `POST /_apis/distributedtask/pools/{n}/agents`

**Header key differences:**

- official only: `{'accept-language', 'accept-encoding', 'x-tfs-fedauthredirect'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,12 +2,12 @@
   "authorization": {
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "sEKqlp/CYa0SQQB8VDIqZjuBRfsKaypkgBN/***REDACTED***/***REDACTED***+MfJLiT+kls3/O9b1vR3ldlA6t98/***REDACTED***+tvXVzIckJB/lZ+AvOTU1/e6LBv6jDQH/RawFjQeQ73K04czwycmoWo3d45eiQ/l1YM9LKvfkNC66QHRStkS8Zu3/5VnhLgScu/L/***REDACTED***=="
+      "modulus": "pLoewDsBgGVJenT//***REDACTED***+***REDACTED***+***REDACTED***+k7q4HsZY1faAr6BfD+XnXbapLthDoAkGy388+u/PBDPnUMKi7ZMpaDoGJJMu7D+***REDACTED***/O6ESO2Wby32aZjiXksu4fY/Un04F8MaUhai/VZQ=="
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
-  "name": "mitm-official-15-oidc-id-token-2026-06-30T16-05-05Z",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-15-oidc-id-token-1783541832",
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
-    "clientId": "d17ff198-ab81-4d11-904d-6c2358fb7a73",
+    "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/_apis/oauth2/token",
+    "clientId": "f146fdb7-3ed6-447a-a073-6145c453a20b",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "sEKqlp/CYa0SQQB8VDIqZjuBRfsKaypkgBN/***REDACTED***/***REDACTED***+MfJLiT+kls3/O9b1vR3ldlA6t98/***REDACTED***+tvXVzIckJB/lZ+AvOTU1/e6LBv6jDQH/RawFjQeQ73K04czwycmoWo3d45eiQ/l1YM9LKvfkNC66QHRStkS8Zu3/5VnhLgScu/L/***REDACTED***=="
+      "modulus": "pLoewDsBgGVJenT//***REDACTED***+***REDACTED***+***REDACTED***+k7q4HsZY1faAr6BfD+XnXbapLthDoAkGy388+u/PBDPnUMKi7ZMpaDoGJJMu7D+***REDACTED***/O6ESO2Wby32aZjiXksu4fY/Un04F8MaUhai/VZQ=="
     }
   },
-  "createdOn": "2026-06-30T16:05:14.993Z",
+  "createdOn": "2026-07-08T20:17:14.113Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
-  "ephemeral": false,
-  "id": 12,
+  "ephemeral": true,
+  "id": 689,
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
-  "name": "mitm-official-15-oidc-id-token-2026-06-30T16-05-05Z",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-15-oidc-id-token-1783541832",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -60,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-12",
+  "queueName": "taskagent-689",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 234.3 / aksh 61.1 | p95: official 234.3 / aksh 61.1

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

**Timing (ms):** p50: official 181.6 / aksh 206.4 | p95: official 181.6 / aksh 206.4

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
-    "id": 12,
-    "name": "mitm-official-15-oidc-id-token-2026-06-30T16-05-05Z",
-    "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+    "id": 689,
+    "name": "aksh-capture-15-oidc-id-token-1783541832",
+    "osDescription": "linux aarch64",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "Nuraydias-Mac-Studio (PID: 49714)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 3608)",
+  "sessionId": "b766cec7-351c-4b79-92d7-d45d140d0124",
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
-  "ownerName": "Nuraydias-Mac-Studio (PID: 49714)",
-  "sessionId": "a7dc5875-10fd-465a-a2a1-f5a4c454dff7"
+  "ownerName": "container (PID: 3608)",
+  "sessionId": "900ca249-aeb5-48b3-aeec-90db22c7e0ea"
 }
```

**Status codes:** official: [201] | aksh: [201]

**Timing (ms):** p50: official 53.7 / aksh 36.4 | p95: official 53.7 / aksh 36.4

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,24 +2,24 @@
   "change_order": 1,
   "steps": [
     {
-      "completed_at": "2026-06-30T16:05:20.357Z",
+      "completed_at": "2026-07-08T20:17:29.237Z",
       "conclusion": 2,
-      "external_id": "d491cbee-9787-4779-8277-fb2e5ad55722",
+      "external_id": "2358048e-58d6-4b87-bb38-cf660bd0993f",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T16:05:20.323Z",
+      "started_at": "2026-07-08T20:17:29.237Z",
       "status": 6
     },
     {
-      "completed_at": null,
-      "conclusion": 0,
-      "external_id": "414b7970-0c8a-4eb5-961e-9c668de9a138",
-      "name": "Run curl -sS -H \"Authorization: ***\" \\",
+      "completed_at": "2026-07-08T20:17:29.685Z",
+      "conclusion": 3,
+      "external_id": "3d76d29e-c827-42f2-9eb3-7db2a57df418",
+      "name": "Run curl -sS -H \"Authorization: Bearer $***REDACTED***\" \\",
       "number": 2,
-      "started_at": null,
-      "status": 5
+      "started_at": "2026-07-08T20:17:29.684Z",
+      "status": 6
     }
   ],
-  "workflow_job_run_backend_id": "41a72348-ef4e-5335-a79d-b2c1226f6b43",
-  "workflow_run_backend_id": "2ae53917-f98c-4a21-b282-e370780cbfc4"
+  "workflow_job_run_backend_id": "34a6a17c-b7ff-5e55-a7e8-5f9e42644b08",
+  "workflow_run_backend_id": "fabc93a1-b568-4580-af3c-078aa6f57066"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 147.0 / aksh 55.1 | p95: official 147.0 / aksh 55.1

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "41a72348-ef4e-5335-a79d-b2c1226f6b43",
-  "workflow_run_backend_id": "2ae53917-f98c-4a21-b282-e370780cbfc4"
+  "workflow_job_run_backend_id": "34a6a17c-b7ff-5e55-a7e8-5f9e42644b08",
+  "workflow_run_backend_id": "fabc93a1-b568-4580-af3c-078aa6f57066"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa12.blob.core.windows.net/actions-results/2ae53917-f98c-4a21-b282-e370780cbfc4/workflow-job-run-41a72348-ef4e-5335-a79d-b2c1226f6b43/logs/job/job-logs.txt?se=2026-06-30T17%3A05%3A39Z&sig=***REDACTED***%3D&ske=2026-06-30T19%3A51%3A01Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A51%3A01Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T16%3A05%3A34Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa18.blob.core.windows.net/actions-results/fabc93a1-b568-4580-af3c-078aa6f57066/workflow-job-run-34a6a17c-b7ff-5e55-a7e8-5f9e42644b08/logs/job/job-logs.txt?se=2026-07-08T21%3A17%3A30Z&sig=KzA6v0rKazIc7u0b4%2FMQC0tv1HjYHOkbrmTYP4Xl8uU%3D&ske=2026-07-09T00%3A09%3A43Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A09%3A43Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A17%3A25Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 31.2 / aksh 41.4 | p95: official 31.2 / aksh 41.4

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "d491cbee-9787-4779-8277-fb2e5ad55722",
-  "workflow_job_run_backend_id": "41a72348-ef4e-5335-a79d-b2c1226f6b43",
-  "workflow_run_backend_id": "2ae53917-f98c-4a21-b282-e370780cbfc4"
+  "step_backend_id": "2358048e-58d6-4b87-bb38-cf660bd0993f",
+  "workflow_job_run_backend_id": "34a6a17c-b7ff-5e55-a7e8-5f9e42644b08",
+  "workflow_run_backend_id": "fabc93a1-b568-4580-af3c-078aa6f57066"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa12.blob.core.windows.net/actions-results/2ae53917-f98c-4a21-b282-e370780cbfc4/workflow-job-run-41a72348-ef4e-5335-a79d-b2c1226f6b43/logs/steps/step-logs-d491cbee-9787-4779-8277-fb2e5ad55722.txt?se=2026-06-30T17%3A05%3A21Z&sig=BDGG5MYq%2F75ciV5SFC%2F0X8HJQ3QRpALNHxEZYCzIjCU%3D&ske=2026-06-30T19%3A50%3A51Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A50%3A51Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T16%3A05%3A16Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa18.blob.core.windows.net/actions-results/fabc93a1-b568-4580-af3c-078aa6f57066/workflow-job-run-34a6a17c-b7ff-5e55-a7e8-5f9e42644b08/logs/steps/step-logs-2358048e-58d6-4b87-bb38-cf660bd0993f.txt?se=2026-07-08T21%3A17%3A29Z&sig=4LEgapq7ueciVe0ATAym%2FKV%2BVuKSCRMmRBKpr2vFmak%3D&ske=2026-07-09T00%3A10%3A48Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A10%3A48Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A17%3A24Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 33.3 / aksh 42.2 | p95: official 55.2 / aksh 119.7

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
-  "jobMessageId": "41a72348-ef4e-5335-a79d-b2c1226f6b43",
-  "runnerOS": "macOS"
+  "jobMessageId": "34a6a17c-b7ff-5e55-a7e8-5f9e42644b08",
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
-          "v": "28458434874"
+          "v": "28972783313"
         },
         {
           "k": "run_number",
-          "v": "1"
+          "v": "14"
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
-          "v": 84339483255
+          "v": 85972394983
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
-  "jobId": "41a72348-ef4e-5335-a79d-b2c1226f6b43",
+  "jobId": "34a6a17c-b7ff-5e55-a7e8-5f9e42644b08",
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
-      "value": "kJDrjVb4qvdfS9I94S6UH9lN5-kJwhDx3H0huWqLtJ1PA"
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
+      "value": "***REDACTED***\\.10edUboo6sBsBDh"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***--Fx2cx9HhEXZUoLaOyQDdic3OoBB-bzd1AK4DA"
     }
   ],
   "messageType": "RunnerJobRequest",
   "plan": {
     "artifactLocation": "",
     "artifactUri": "",
-    "planId": "2ae53917-f98c-4a21-b282-e370780cbfc4",
+    "planId": "fabc93a1-b568-4580-af3c-078aa6f57066",
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
@@ -896,7 +892,7 @@
           "CacheServerUrl": "https://artifactcache.actions.githubusercontent.com/***REDACTED***/",
           "ConnectivityChecks": "[\"https://broker.actions.githubusercontent.com/health\",\"https://token.actions.githubusercontent.com/ready\",\"https://run.actions.githubusercontent.com/health\"]",
           "FeedStreamUrl": "wss://results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
-          "GenerateIdTokenUrl": "https://run-actions-2-azure-eastus.actions.githubusercontent.com/124//idtoken/2ae53917-f98c-4a21-b282-e370780cbfc4/41a72348-ef4e-5335-a79d-b2c1226f6b43?api-version=2.0",
+          "GenerateIdTokenUrl": "https://run-actions-3-azure-eastus.actions.githubusercontent.com/163//idtoken/fabc93a1-b568-4580-af3c-078aa6f57066/34a6a17c-b7ff-5e55-a7e8-5f9e42644b08?api-version=2.0",
           "PipelinesServiceUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/",
           "ResultsServiceUrl": "https://results-receiver.actions.githubusercontent.com/",
           "ServerId": "",
@@ -905,7 +901,7 @@
         "isReady": true,
         "isShared": false,
         "name": "SystemVssConnection",
-        "url": "https://run-actions-2-azure-eastus.actions.githubusercontent.com/124/"
+        "url": "https://run-actions-3-azure-eastus.actions.githubusercontent.com/163/"
       }
     ]
   },
@@ -915,7 +911,7 @@
       "condition": "success()",
       "contextName": "__run",
       "continueOnError": null,
-      "id": "414b7970-0c8a-4eb5-961e-9c668de9a138",
+      "id": "3d76d29e-c827-42f2-9eb3-7db2a57df418",
       "inputs": {
         "map": [
           {
@@ -944,7 +940,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "2ae53917-f98c-4a21-b282-e370780cbfc4",
+    "id": "fabc93a1-b568-4580-af3c-078aa6f57066",
     "location": null
   },
   "variables": {
@@ -1064,7 +1060,7 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
@@ -1083,13 +1079,13 @@
     },
     "system.github.token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.github.token.permissions": {
       "value": "{\"Contents\":\"read\",\"Metadata\":\"read\"}"
     },
     "system.orchestrationId": {
-      "value": "2ae53917-f98c-4a21-b282-e370780cbfc4.build.__default"
+      "value": "fabc93a1-b568-4580-af3c-078aa6f57066.build.__default"
     },
     "system.phaseDisplayName": {
       "value": "build"
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 549.8 / aksh 503.7 | p95: official 549.8 / aksh 503.7

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,60 +1,67 @@
 {
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
-  "conclusion": "succeeded",
-  "jobId": "41a72348-ef4e-5335-a79d-b2c1226f6b43",
+  "conclusion": "failed",
+  "jobId": "34a6a17c-b7ff-5e55-a7e8-5f9e42644b08",
   "outputs": {},
-  "planId": "2ae53917-f98c-4a21-b282-e370780cbfc4",
+  "planId": "fabc93a1-b568-4580-af3c-078aa6f57066",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-06-30T16:05:20.35774Z",
+      "completed_at": "2026-07-08T20:17:30.425Z",
       "conclusion": "succeeded",
-      "external_id": "d491cbee-9787-4779-8277-fb2e5ad55722",
+      "external_id": "2358048e-58d6-4b87-bb38-cf660bd0993f",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T16:05:20.323623Z",
+      "started_at": "2026-07-08T20:17:30.425Z",
       "status": "completed",
       "type": "runner"
     },
     {
       "action_name": "sh",
-      "annotations": [],
-      "completed_at": "2026-06-30T16:05:21.28594Z",
-      "conclusion": "succeeded",
-      "external_id": "414b7970-0c8a-4eb5-961e-9c668de9a138",
-      "name": "Run curl -sS -H \"Authorization: ***\" \\",
+      "annotations": [
+        {
+          "endLine": 1,
+          "level": "failure",
+          "message": "Process completed with exit code 127.",
+          "startLine": 1,
+          "stepNumber": 2
+        },
+        {
+          "endLine": 1,
+          "level": "failure",
+          "message": "process exit code 127",
+          "startLine": 1,
+          "stepNumber": 2
+        }
+      ],
+      "completed_at": "2026-07-08T20:17:30.425Z",
+      "conclusion": "failed",
+      "external_id": "3d76d29e-c827-42f2-9eb3-7db2a57df418",
+      "name": "Run curl -sS -H \"Authorization: Bearer $***REDACTED***\" \\",
       "number": 2,
-      "started_at": "2026-06-30T16:05:21.088055Z",
+      "started_at": "2026-07-08T20:17:30.425Z",
       "status": "completed",
       "type": "run"
     },
     {
       "action_name": "complete_job",
       "annotations": [],
-      "completed_at": "2026-06-30T16:05:21.305301Z",
+      "completed_at": "2026-07-08T20:17:30.425Z",
       "conclusion": "succeeded",
-      "external_id": "d2a8e90b-92cc-4b2d-9f0a-c307b6d5496d",
+      "external_id": "9e89e1d9-70bc-4323-8600-10eac2f4ae2b",
       "name": "Complete job",
       "number": 3,
-      "started_at": "2026-06-30T16:05:21.292082Z",
+      "started_at": "2026-07-08T20:17:30.425Z",
       "status": "completed",
       "type": "runner"
     }
   ],
   "telemetry": [
     {
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

**Timing (ms):** p50: official 136.2 / aksh 46.4 | p95: official 136.2 / aksh 46.4

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "41a72348-ef4e-5335-a79d-b2c1226f6b43",
-  "planId": "2ae53917-f98c-4a21-b282-e370780cbfc4"
+  "jobId": "34a6a17c-b7ff-5e55-a7e8-5f9e42644b08",
+  "planId": "fabc93a1-b568-4580-af3c-078aa6f57066"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-06-30T16:15:20.09261225Z"
+  "lockedUntil": "2026-07-08T20:27:29.259622906Z"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 38.3 / aksh 49.4 | p95: official 38.3 / aksh 49.4
