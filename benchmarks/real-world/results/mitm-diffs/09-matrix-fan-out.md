# MITM comparison: 09-matrix-fan-out

**official**: ok — 73 flows
**aksh**: N/A — 39 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/_apis/distributedtask/pools/{n}/agents/{n}` | 1 | 0 | 136.8 | - | 204 |  |
| DELETE | `/session` | 1 | 1 | 50.4 | 40.5 | 204 | 204 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1` | 0 | 1 | - | 22.9 |  | 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 9 | 5 | 21.9 | 38.1 | 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-09-matrix-fan-out-1783541547&includeCapabilities=False` | 0 | 1 | - | 21.2 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official&includeCapabilities=False` | 2 | 0 | 28.4 | - | 200, 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 20.9 | 24.2 | 200 | 200 |
| GET | `/_ws/ingest.sock` | 0 | 1 | - | 62.0 |  | 401 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 | - | 2651.5 |  | 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 | - | 3284.8 |  | 200 |
| GET | `/health` | 6 | 2 | 24.7 | 36.0 | 200, 200, 200, 200, 200, 200 | 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 0 |  | None |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 3 | 0 | 0 | - | None, None, None |  |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 1758.1 |  | 200 |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 4 | 0 | 941.4 | - | 200, 200, 200, 404 |  |
| GET | `/ready` | 3 | 1 | 16.2 | 26.0 | 204, 204, 204 | 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 165.8 | 69.7 | 200 | 200 |
| POST | `/_apis/oauth2/token` | 0 | 2 | - | 91.9 |  | 200, 200 |
| POST | `/_apis/oauth2/token/{guid}` | 12 | 0 | 24.4 | - | 200, 200, 200, 200, 400, 400, 400, 400, 400, 400, 400, 400 |  |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 0 | 1 | - | 42.5 |  | 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 3 | 0 | 38.5 | - | 200, 200, 200 |  |
| POST | `/actions/runner-registration` | 2 | 1 | 193.7 | 208.9 | 200, 200 | 200 |
| POST | `/session` | 1 | 1 | 40.0 | 47.6 | 201 | 201 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 4 | 1 | 43.8 | 58.5 | 200, 200, 200, 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 0 | 1 | - | 42.4 |  | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 0 | 3 | - | 188.4 |  | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 3 | 1 | 31.0 | 42.6 | 200, 200, 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 9 | 3 | 29.7 | 46.8 | 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200 |
| POST | `/{n}/acquirejob` | 3 | 1 | 543.4 | 359.7 | 200, 200, 200 | 200 |
| POST | `/{n}/completejob` | 2 | 1 | 30.5 | 49.7 | 204, 204 | 204 |
| POST | `/{n}/renewjob` | 3 | 1 | 135.5 | 54.2 | 200, 200, 200 | 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A12%3A40Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A10Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A10Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A35Z&sv=2025-11-05` | 0 | 1 | - | 140.6 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A39Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A11%3A05Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A11%3A05Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A34Z&sv=2025-11-05` | 0 | 1 | - | 98.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A39Z&sig=***REDACTED***%2BySyo%3D&ske=2026-07-09T00%3A11%3A30Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A11%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A34Z&sv=2025-11-05` | 0 | 1 | - | 76.8 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A40Z&sig=cnZ0Pjt%2B%***REDACTED***%2BSkY0%3D&ske=2026-07-09T00%3A10%3A53Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A35Z&sv=2025-11-05` | 0 | 1 | - | 79.1 |  | 201 |

## Missing endpoints

### official only

- `DELETE /_apis/distributedtask/pools/{n}/agents/{n}`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=mitm-official&includeCapabilities=False`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token/{guid}`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64`

### aksh only

- `GET /_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-09-matrix-fan-out-1783541547&includeCapabilities=False`
- `GET /_ws/ingest.sock`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`
- `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`
- `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A12%3A40Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A10Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A10Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A35Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A39Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A11%3A05Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A11%3A05Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A34Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A39Z&sig=***REDACTED***%2BySyo%3D&ske=2026-07-09T00%3A11%3A30Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A11%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A34Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A40Z&sig=cnZ0Pjt%2B%***REDACTED***%2BSkY0%3D&ske=2026-07-09T00%3A10%3A53Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A35Z&sv=2025-11-05`

## Per-endpoint comparison

### `DELETE /session`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 50.4 / aksh 40.5 | p95: official 50.4 / aksh 40.5

### `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`

**Header key differences:**

- official only: `{'x-tfs-fedauthredirect', 'authorization', 'accept-language', 'accept-encoding'}`

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

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200]

**Timing (ms):** p50: official 21.8 / aksh 21.3 | p95: official 25.1 / aksh 109.4

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{'x-tfs-fedauthredirect', 'accept-language', 'accept-encoding'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -10,7 +10,7 @@
       "isInternal": true,
       "name": "Default",
       "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 0,
+      "size": 1,
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

**Timing (ms):** p50: official 20.9 / aksh 24.2 | p95: official 20.9 / aksh 24.2

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200, 200, 200, 200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 23.4 / aksh 51.6 | p95: official 38.6 / aksh 51.6

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204, 204, 204] | aksh: [204]

**Timing (ms):** p50: official 16.8 / aksh 26.0 | p95: official 16.9 / aksh 26.0

### `POST /_apis/distributedtask/pools/{n}/agents`

**Header key differences:**

- official only: `{'x-tfs-fedauthredirect', 'accept-language', 'accept-encoding'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,12 +2,12 @@
   "authorization": {
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "qGGWanYUK5Uvkxiaz4IIvw3akv+5+Snwms+***REDACTED***/20OWNBWwkbLEEQqIitopBPLI/tjLLEUcY+iMPGUmGMVk+KJNu75PjQa8tA1N1+AMJ4PvWHeOTx0OEURIy/29JAKM6KQ2Sd3VAluU0P2leNichVa/l9I+QZpmFuHlcbBINasLWUwfd+4wYDd7dsjTtMXyB6uh+iTTKrqyOa8Yy0aniS3QL+WTr7yYtSfEUhcscj/T/mIZfk+***REDACTED***/4v/ZBQT++Ci+Q=="
+      "modulus": "***REDACTED***/DpsHxLy77+***REDACTED***/j+8dfwsyxECySwcMKPtJylOKGCMW5+***REDACTED***+/***REDACTED***/***REDACTED***+mBItw=="
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
-  "name": "mitm-official",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-09-matrix-fan-out-1783541547",
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
-    "clientId": "7d02f7eb-5aea-4a88-81ac-baa02eceeb18",
+    "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/_apis/oauth2/token",
+    "clientId": "81df6bc5-40c9-493f-b806-b43a1a5324cb",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "qGGWanYUK5Uvkxiaz4IIvw3akv+5+Snwms+***REDACTED***/20OWNBWwkbLEEQqIitopBPLI/tjLLEUcY+iMPGUmGMVk+KJNu75PjQa8tA1N1+AMJ4PvWHeOTx0OEURIy/29JAKM6KQ2Sd3VAluU0P2leNichVa/l9I+QZpmFuHlcbBINasLWUwfd+4wYDd7dsjTtMXyB6uh+iTTKrqyOa8Yy0aniS3QL+WTr7yYtSfEUhcscj/T/mIZfk+***REDACTED***/4v/ZBQT++Ci+Q=="
+      "modulus": "***REDACTED***/DpsHxLy77+***REDACTED***/j+8dfwsyxECySwcMKPtJylOKGCMW5+***REDACTED***+/***REDACTED***/***REDACTED***+mBItw=="
     }
   },
-  "createdOn": "2026-06-30T15:40:33.947Z",
+  "createdOn": "2026-07-08T20:12:28.97Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
-  "ephemeral": false,
-  "id": 7,
+  "ephemeral": true,
+  "id": 683,
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
-  "name": "mitm-official",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-09-matrix-fan-out-1783541547",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -60,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-7",
+  "queueName": "taskagent-683",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 165.8 / aksh 69.7 | p95: official 165.8 / aksh 69.7

### `POST /actions/runner-registration`

**Header key differences:**

- aksh only: `{'accept'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "runner_event": "remove",
+  "runner_event": "register",
   "url": "https://github.com/preloopdev/aksh-conformance-sample"
 }
```

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

**Status codes:** official: [200, 200] | aksh: [200]

**Timing (ms):** p50: official 223.4 / aksh 208.9 | p95: official 223.4 / aksh 208.9

### `POST /session`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,14 +1,14 @@
 {
   "agent": {
     "ephemeral": null,
-    "id": 7,
-    "name": "mitm-official",
-    "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+    "id": 683,
+    "name": "aksh-capture-09-matrix-fan-out-1783541547",
+    "osDescription": "linux aarch64",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "Nuraydias-Mac-Studio (PID: 25761)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 3501)",
+  "sessionId": "59920bdd-f60c-4082-b9c9-16fa61df1390",
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
-  "ownerName": "Nuraydias-Mac-Studio (PID: 25761)",
-  "sessionId": "a396f00c-c473-44e7-8003-63cef1d0fe98"
+  "ownerName": "container (PID: 3501)",
+  "sessionId": "be7b987f-e222-42d4-b00f-8f89a3f912f1"
 }
```

**Status codes:** official: [201] | aksh: [201]

**Timing (ms):** p50: official 40.0 / aksh 47.6 | p95: official 40.0 / aksh 47.6

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,24 +2,33 @@
   "change_order": 1,
   "steps": [
     {
-      "completed_at": "2026-06-30T15:40:39.621Z",
+      "completed_at": "2026-07-08T20:12:39.230Z",
       "conclusion": 2,
-      "external_id": "7b9f8413-4e15-47f8-aa80-a2acd8b66b0e",
+      "external_id": "470a6429-6636-4734-8c4b-65dde0cdf809",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T15:40:39.583Z",
+      "started_at": "2026-07-08T20:12:39.230Z",
       "status": 6
     },
     {
-      "completed_at": null,
-      "conclusion": 0,
-      "external_id": "44a4d1de-865b-40e1-81ce-0995bce6ef1c",
-      "name": "Run if [ \"2\" = \"1\" ]; then exit 1; fi",
+      "completed_at": "2026-07-08T20:12:39.776Z",
+      "conclusion": 2,
+      "external_id": "212c6014-9b60-45b1-b338-180dc4e57f7f",
+      "name": "Run echo \"got \"",
       "number": 2,
-      "started_at": "2026-06-30T15:40:39.625Z",
-      "status": 3
+      "started_at": "2026-07-08T20:12:39.771Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-08T20:12:40.118Z",
+      "conclusion": 2,
+      "external_id": "275e9709-cb6d-4889-a409-ba01584ce090",
+      "name": "Complete job",
+      "number": 3,
+      "started_at": "2026-07-08T20:12:40.118Z",
+      "status": 6
     }
   ],
-  "workflow_job_run_backend_id": "d09e731b-f2db-5de0-886c-c08c022279ea",
-  "workflow_run_backend_id": "83b43d6a-2d8f-40e2-ab90-43e3de620e60"
+  "workflow_job_run_backend_id": "fbe08957-6e89-5d99-8f92-91af91a30464",
+  "workflow_run_backend_id": "e54c29bc-b8b6-4759-9311-b471c548a8f5"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200] | aksh: [200]

**Timing (ms):** p50: official 43.1 / aksh 58.5 | p95: official 49.6 / aksh 58.5

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "d09e731b-f2db-5de0-886c-c08c022279ea",
-  "workflow_run_backend_id": "83b43d6a-2d8f-40e2-ab90-43e3de620e60"
+  "workflow_job_run_backend_id": "fbe08957-6e89-5d99-8f92-91af91a30464",
+  "workflow_run_backend_id": "e54c29bc-b8b6-4759-9311-b471c548a8f5"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa4.blob.core.windows.net/actions-results/83b43d6a-2d8f-40e2-ab90-43e3de620e60/workflow-job-run-d09e731b-f2db-5de0-886c-c08c022279ea/logs/job/job-logs.txt?se=2026-06-30T16%3A41%3A11Z&sig=gwL7xvUG%2BC%***REDACTED***%3D&ske=2026-06-30T19%3A09%3A44Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A09%3A44Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A41%3A06Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa12.blob.core.windows.net/actions-results/e54c29bc-b8b6-4759-9311-b471c548a8f5/workflow-job-run-fbe08957-6e89-5d99-8f92-91af91a30464/logs/job/job-logs.txt?se=2026-07-08T21%3A12%3A40Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A10Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A10%3A10Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A35Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200]

**Timing (ms):** p50: official 31.7 / aksh 42.6 | p95: official 35.3 / aksh 42.6

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "7b9f8413-4e15-47f8-aa80-a2acd8b66b0e",
-  "workflow_job_run_backend_id": "d09e731b-f2db-5de0-886c-c08c022279ea",
-  "workflow_run_backend_id": "83b43d6a-2d8f-40e2-ab90-43e3de620e60"
+  "step_backend_id": "470a6429-6636-4734-8c4b-65dde0cdf809",
+  "workflow_job_run_backend_id": "fbe08957-6e89-5d99-8f92-91af91a30464",
+  "workflow_run_backend_id": "e54c29bc-b8b6-4759-9311-b471c548a8f5"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa4.blob.core.windows.net/actions-results/83b43d6a-2d8f-40e2-ab90-43e3de620e60/workflow-job-run-d09e731b-f2db-5de0-886c-c08c022279ea/logs/steps/step-logs-7b9f8413-4e15-47f8-aa80-a2acd8b66b0e.txt?se=2026-06-30T16%3A40%3A40Z&sig=7CNNThQUez9J%2Bi%2F1SZzKameG5Nze8nov%2B9ebSj35gVY%3D&ske=2026-06-30T19%3A10%3A48Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A10%3A48Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A40%3A35Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa12.blob.core.windows.net/actions-results/e54c29bc-b8b6-4759-9311-b471c548a8f5/workflow-job-run-fbe08957-6e89-5d99-8f92-91af91a30464/logs/steps/step-logs-470a6429-6636-4734-8c4b-65dde0cdf809.txt?se=2026-07-08T21%3A12%3A39Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A11%3A05Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A11%3A05Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A34Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 31.5 / aksh 41.5 | p95: official 36.4 / aksh 61.1

### `POST /{n}/acquirejob`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "billingOwnerId": "O_kgDOEbddog",
-  "jobMessageId": "d09e731b-f2db-5de0-886c-c08c022279ea",
-  "runnerOS": "macOS"
+  "jobMessageId": "fbe08957-6e89-5d99-8f92-91af91a30464",
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
-          "v": "28456817317"
+          "v": "28972487507"
         },
         {
           "k": "run_number",
-          "v": "1"
+          "v": "12"
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
@@ -61,7 +61,7 @@
         },
         {
           "k": "workflow",
-          "v": "mitm matrix"
+          "v": "mitm job outputs"
         },
         {
           "k": "head_ref",
@@ -113,7 +113,7 @@
               },
               {
                 "k": "workflow",
-                "v": ".github/workflows/09-matrix-fan-out.yml"
+                "v": ".github/workflows/08-job-outputs-needs.yml"
               },
               {
                 "k": "inputs",
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
@@ -687,11 +687,11 @@
         },
         {
           "k": "workflow_ref",
-          "v": "preloopdev/aksh-conformance-sample/.github/workflows/09-matrix-fan-out.yml@refs/heads/main"
+          "v": "preloopdev/aksh-conformance-sample/.github/workflows/08-job-outputs-needs.yml@refs/heads/main"
         },
         {
           "k": "workflow_sha",
-          "v": "***REDACTED***"
+          "v": "***REDACTED***"
         },
         {
           "k": "repository_id",
@@ -712,15 +712,15 @@
       "d": [
         {
           "k": "check_run_id",
-          "v": 84333771981
+          "v": 85971420913
         },
         {
           "k": "workflow_ref",
-          "v": "preloopdev/aksh-conformance-sample/.github/workflows/09-matrix-fan-out.yml@refs/heads/main"
+          "v": "preloopdev/aksh-conformance-sample/.github/workflows/08-job-outputs-needs.yml@refs/heads/main"
         },
         {
           "k": "workflow_sha",
-          "v": "***REDACTED***"
+          "v": "***REDACTED***"
         },
         {
           "k": "workflow_repository",
@@ -728,24 +728,36 @@
         },
         {
           "k": "workflow_file_path",
-          "v": ".github/workflows/09-matrix-fan-out.yml"
+          "v": ".github/workflows/08-job-outputs-needs.yml"
         }
       ],
       "t": 2
     },
-    "matrix": {
+    "matrix": null,
+    "needs": {
       "d": [
         {
-          "k": "n",
-          "v": 2
+          "k": "producer",
+          "v": {
+            "d": [
+              {
+                "k": "result",
+                "v": "success"
+              },
+              {
+                "k": "outputs",
+                "v": {
+                  "d": [],
+                  "t": 2
+                }
+              }
+            ],
+            "t": 2
+          }
         }
       ],
       "t": 2
     },
-    "needs": {
-      "d": [],
-      "t": 2
-    },
     "strategy": {
       "d": [
         {
@@ -754,15 +766,15 @@
         },
         {
           "k": "job-index",
+          "v": 0
+        },
+        {
+          "k": "job-total",
           "v": 1
         },
         {
-          "k": "job-total",
-          "v": 3
-        },
-        {
           "k": "max-parallel",
-          "v": 3
+          "v": 1
         }
       ],
       "t": 2
@@ -775,12 +787,12 @@
   "defaults": [],
   "environmentVariables": [],
   "fileTable": [
-    ".github/workflows/09-matrix-fan-out.yml"
+    ".github/workflows/08-job-outputs-needs.yml"
   ],
   "jobContainer": null,
-  "jobDisplayName": "build (2)",
-  "jobId": "d09e731b-f2db-5de0-886c-c08c022279ea",
-  "jobName": "_2",
+  "jobDisplayName": "consumer",
+  "jobId": "fbe08957-6e89-5d99-8f92-91af91a30464",
+  "jobName": "__default",
   "jobOutputs": null,
   "jobServiceContainers": null,
   "lockedUntil": "0001-01-01T00:00:00",
@@ -859,34 +871,30 @@
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
-      "value": "***REDACTED***\\.4sgOdzHFPfKU-_AkW-Ye_uCGcIu6lJLyG749xGn9n"
-    },
-    {
-      "type": "regex",
-      "value": "ZqhQsErQQ1-PtMpIY5FyT-CqJAWbFazoI64-KXu92cZiA"
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
+      "value": "***REDACTED***\\.kjzozkP4k98M9Kd"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***-J0qA1iUMw"
     }
   ],
   "messageType": "RunnerJobRequest",
   "plan": {
     "artifactLocation": "",
     "artifactUri": "",
-    "planId": "83b43d6a-2d8f-40e2-ab90-43e3de620e60",
+    "planId": "e54c29bc-b8b6-4759-9311-b471c548a8f5",
     "planType": "actions",
     "version": 0
   },
@@ -896,7 +904,7 @@
       {
         "authorization": {
           "parameters": {
-            "AccessToken": "***REDACTED***"
+            "AccessToken": "***REDACTED***"
           },
           "scheme": "OAuth"
         },
@@ -913,7 +921,7 @@
         "isReady": true,
         "isShared": false,
         "name": "SystemVssConnection",
-        "url": "https://run-actions-1-azure-eastus.actions.githubusercontent.com/23/"
+        "url": "https://run-actions-1-azure-eastus.actions.githubusercontent.com/29/"
       }
     ]
   },
@@ -923,7 +931,7 @@
       "condition": "success()",
       "contextName": "__run",
       "continueOnError": null,
-      "id": "44a4d1de-865b-40e1-81ce-0995bce6ef1c",
+      "id": "212c6014-9b60-45b1-b338-180dc4e57f7f",
       "inputs": {
         "map": [
           {
@@ -933,9 +941,9 @@
             },
             "Value": {
               "col": 14,
-              "expr": "format('if [ \"{0}\" = \"1\" ]; then exit 1; fi\nsleep 20\n', matrix.n)",
+              "expr": "format('echo \"got {0}\"', needs.producer.outputs.value)",
               "file": 1,
-              "line": 11,
+              "line": 15,
               "type": 3
             }
           }
@@ -952,7 +960,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "83b43d6a-2d8f-40e2-ab90-43e3de620e60",
+    "id": "e54c29bc-b8b6-4759-9311-b471c548a8f5",
     "location": null
   },
   "variables": {
@@ -1072,13 +1080,13 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
     },
     "system.github.job": {
-      "value": "build"
+      "value": "consumer"
     },
     "system.github.launch_endpoint": {
       "value": "https://launch.actions.githubusercontent.com"
@@ -1091,16 +1099,16 @@
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
-      "value": "83b43d6a-2d8f-40e2-ab90-43e3de620e60.build._2"
+      "value": "e54c29bc-b8b6-4759-9311-b471c548a8f5.consumer.__default"
     },
     "system.phaseDisplayName": {
-      "value": "build (2)"
+      "value": "consumer"
     },
     "system.runner.lowdiskspacethreshold": {
       "value": "100"
```

**Status codes:** official: [200, 200, 200] | aksh: [200]

**Timing (ms):** p50: official 555.0 / aksh 359.7 | p95: official 637.4 / aksh 359.7

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,59 +2,51 @@
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
   "conclusion": "succeeded",
-  "jobId": "d09e731b-f2db-5de0-886c-c08c022279ea",
+  "jobId": "fbe08957-6e89-5d99-8f92-91af91a30464",
   "outputs": {},
-  "planId": "83b43d6a-2d8f-40e2-ab90-43e3de620e60",
+  "planId": "e54c29bc-b8b6-4759-9311-b471c548a8f5",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-06-30T15:40:39.621233Z",
+      "completed_at": "2026-07-08T20:12:40.709Z",
       "conclusion": "succeeded",
-      "external_id": "7b9f8413-4e15-47f8-aa80-a2acd8b66b0e",
+      "external_id": "470a6429-6636-4734-8c4b-65dde0cdf809",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T15:40:39.583381Z",
+      "started_at": "2026-07-08T20:12:40.709Z",
       "status": "completed",
       "type": "runner"
     },
     {
       "action_name": "sh",
       "annotations": [],
-      "completed_at": "2026-06-30T15:40:59.685412Z",
+      "completed_at": "2026-07-08T20:12:40.709Z",
       "conclusion": "succeeded",
-      "external_id": "44a4d1de-865b-40e1-81ce-0995bce6ef1c",
-      "name": "Run if [ \"2\" = \"1\" ]; then exit 1; fi",
+      "external_id": "212c6014-9b60-45b1-b338-180dc4e57f7f",
+      "name": "Run ${{ format('echo \"got {0}\"', needs.producer.outputs.value) }}",
       "number": 2,
-      "started_at": "2026-06-30T15:40:39.62591Z",
+      "started_at": "2026-07-08T20:12:40.709Z",
       "status": "completed",
       "type": "run"
     },
     {
       "action_name": "complete_job",
       "annotations": [],
-      "completed_at": "2026-06-30T15:40:59.729881Z",
+      "completed_at": "2026-07-08T20:12:40.709Z",
       "conclusion": "succeeded",
-      "external_id": "ec8647e1-8cb6-43b6-9bcc-21e9614c6118",
+      "external_id": "275e9709-cb6d-4889-a409-ba01584ce090",
       "name": "Complete job",
       "number": 3,
-      "started_at": "2026-06-30T15:40:59.69795Z",
+      "started_at": "2026-07-08T20:12:40.709Z",
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
+      "message": "{\"ClassType\":\"StepsRunner\",\"FinishResult\":\"succeeded\"}",
+      "type": "task"
     }
   ]
 }
```

**Status codes:** official: [204, 204] | aksh: [204]

**Timing (ms):** p50: official 33.4 / aksh 49.7 | p95: official 33.4 / aksh 49.7

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "d09e731b-f2db-5de0-886c-c08c022279ea",
-  "planId": "83b43d6a-2d8f-40e2-ab90-43e3de620e60"
+  "jobId": "fbe08957-6e89-5d99-8f92-91af91a30464",
+  "planId": "e54c29bc-b8b6-4759-9311-b471c548a8f5"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-06-30T15:50:39.365092627Z"
+  "lockedUntil": "2026-07-08T20:22:39.269247974Z"
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200]

**Timing (ms):** p50: official 32.1 / aksh 54.2 | p95: official 342.9 / aksh 54.2
