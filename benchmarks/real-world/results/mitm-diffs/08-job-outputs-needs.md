# MITM comparison: 08-job-outputs-needs

**official**: ok — 59 flows
**aksh**: N/A — 39 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/_apis/distributedtask/pools/{n}/agents/{n}` | 1 | 0 | 122.4 | - | 204 |  |
| DELETE | `/session` | 1 | 1 | 29.6 | 37.9 | 204 | 204 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1` | 0 | 1 | - | 41.9 |  | 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 9 | 5 | 20.4 | 20.3 | 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-08-job-outputs-needs-1783541538&includeCapabilities=False` | 0 | 1 | - | 25.4 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official&includeCapabilities=False` | 2 | 0 | 35.8 | - | 200, 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 70.5 | 100.1 | 200 | 200 |
| GET | `/_ws/ingest.sock` | 0 | 1 | - | 46.9 |  | 401 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 | - | 2766.0 |  | 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 | - | 3367.5 |  | 200 |
| GET | `/health` | 4 | 2 | 52.9 | 33.1 | 200, 200, 200, 200 | 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 0 |  | None |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 2 | 0 | 0 | - | None, None |  |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 7192.9 |  | 200 |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 5 | 0 | 10737.9 | - | 200, 200, 202, 404, None |  |
| GET | `/ready` | 2 | 1 | 18.7 | 44.4 | 204, 204 | 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 148.8 | 74.5 | 200 | 200 |
| POST | `/_apis/oauth2/token` | 0 | 2 | - | 118.4 |  | 200, 200 |
| POST | `/_apis/oauth2/token/{guid}` | 10 | 0 | 30.5 | - | 200, 200, 200, 400, 400, 400, 400, 400, 400, 400 |  |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 0 | 1 | - | 63.8 |  | 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 2 | 0 | 53.2 | - | 200, 200 |  |
| POST | `/actions/runner-registration` | 2 | 1 | 450.7 | 314.6 | 200, 200 | 200 |
| POST | `/session` | 1 | 1 | 48.8 | 31.0 | 201 | 201 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 2 | 1 | 62.1 | 46.7 | 200, 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 0 | 1 | - | 312.1 |  | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 0 | 3 | - | 61.4 |  | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 2 | 1 | 34.3 | 45.2 | 200, 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 6 | 3 | 57.7 | 162.7 | 200, 200, 200, 200, 200, 200 | 200, 200, 200 |
| POST | `/{n}/acquirejob` | 2 | 1 | 497.9 | 435.5 | 200, 200 | 200 |
| POST | `/{n}/completejob` | 2 | 1 | 82.5 | 41.0 | 204, 204 | 204 |
| POST | `/{n}/renewjob` | 2 | 1 | 42.0 | 50.0 | 200, 200 | 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A12%3A37Z&sig=As%2F%***REDACTED***%3D&ske=2026-07-09T00%3A10%3A21Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A21Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A32Z&sv=2025-11-05` | 0 | 1 | - | 134.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A36Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A40Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A40Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A31Z&sv=2025-11-05` | 0 | 1 | - | 78.6 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A36Z&sig=EqzGm7rkCVxE1dcvJSLlRgRmIMHBR%2BqR4mjwlpZ8dpk%3D&ske=2026-07-09T00%3A10%3A55Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A55Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A31Z&sv=2025-11-05` | 0 | 1 | - | 37.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A37Z&sig=UOQZJVULTir3B4GqF6VNFR%2BAspv8xu2yt7QLxNHUIkk%3D&ske=2026-07-09T00%3A09%3A43Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A32Z&sv=2025-11-05` | 0 | 1 | - | 75.6 |  | 201 |

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
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-08-job-outputs-needs-1783541538&includeCapabilities=False`
- `GET /_ws/ingest.sock`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`
- `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`
- `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A12%3A37Z&sig=As%2F%***REDACTED***%3D&ske=2026-07-09T00%3A10%3A21Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A21Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A32Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A36Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A40Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A40Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A31Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A36Z&sig=EqzGm7rkCVxE1dcvJSLlRgRmIMHBR%2BqR4mjwlpZ8dpk%3D&ske=2026-07-09T00%3A10%3A55Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A55Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A31Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A12%3A37Z&sig=UOQZJVULTir3B4GqF6VNFR%2BAspv8xu2yt7QLxNHUIkk%3D&ske=2026-07-09T00%3A09%3A43Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A32Z&sv=2025-11-05`

## Per-endpoint comparison

### `DELETE /session`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 29.6 / aksh 37.9 | p95: official 29.6 / aksh 37.9

### `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`

**Header key differences:**

- official only: `{'authorization', 'x-tfs-fedauthredirect', 'accept-encoding', 'accept-language'}`

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

**Timing (ms):** p50: official 20.8 / aksh 19.8 | p95: official 21.9 / aksh 22.1

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{'x-tfs-fedauthredirect', 'accept-encoding', 'accept-language'}`

**Response body diff:**

```diff
--- official
+++ aksh
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

**Timing (ms):** p50: official 70.5 / aksh 100.1 | p95: official 70.5 / aksh 100.1

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200, 200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 38.7 / aksh 43.7 | p95: official 124.7 / aksh 43.7

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204, 204] | aksh: [204]

**Timing (ms):** p50: official 19.0 / aksh 44.4 | p95: official 19.0 / aksh 44.4

### `POST /_apis/distributedtask/pools/{n}/agents`

**Header key differences:**

- official only: `{'x-tfs-fedauthredirect', 'accept-encoding', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,12 +2,12 @@
   "authorization": {
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "q42qkE4RPggTens1/QZ6iz9O+TpY0e1XuuuM+S5CHAhaOd8RF0ekd+g0epjcb2YtkBkItwzJIJN7pJ/pyzWJ6PYp6H4BjmrxlB6n7SNZljZ/***REDACTED***+***REDACTED***/***REDACTED***/***REDACTED***/X307CY//aDuwueF7gA/m5JecnQw=="
+      "modulus": "***REDACTED***/1oRNMFN3FvsCm7/***REDACTED***/***REDACTED***+***REDACTED***=="
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
+  "name": "aksh-capture-08-job-outputs-needs-1783541538",
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
-    "clientId": "1fe1a153-c7b8-479a-8347-c6f5444e5cd3",
+    "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/_apis/oauth2/token",
+    "clientId": "93464d7a-13a8-461e-9994-7b623ef3770a",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "q42qkE4RPggTens1/QZ6iz9O+TpY0e1XuuuM+S5CHAhaOd8RF0ekd+g0epjcb2YtkBkItwzJIJN7pJ/pyzWJ6PYp6H4BjmrxlB6n7SNZljZ/***REDACTED***+***REDACTED***/***REDACTED***/***REDACTED***/X307CY//aDuwueF7gA/m5JecnQw=="
+      "modulus": "***REDACTED***/1oRNMFN3FvsCm7/***REDACTED***/***REDACTED***+***REDACTED***=="
     }
   },
-  "createdOn": "2026-06-30T15:38:22.05Z",
+  "createdOn": "2026-07-08T20:12:20.263Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
-  "ephemeral": false,
-  "id": 6,
+  "ephemeral": true,
+  "id": 682,
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
+  "name": "aksh-capture-08-job-outputs-needs-1783541538",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -60,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-6",
+  "queueName": "taskagent-682",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 148.8 / aksh 74.5 | p95: official 148.8 / aksh 74.5

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

**Timing (ms):** p50: official 688.0 / aksh 314.6 | p95: official 688.0 / aksh 314.6

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
-    "id": 6,
-    "name": "mitm-official",
-    "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+    "id": 682,
+    "name": "aksh-capture-08-job-outputs-needs-1783541538",
+    "osDescription": "linux aarch64",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "Nuraydias-Mac-Studio (PID: 23818)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 702)",
+  "sessionId": "56d7b4fd-578a-480b-b4b8-fcf57b15f3f0",
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
-  "ownerName": "Nuraydias-Mac-Studio (PID: 23818)",
-  "sessionId": "0d6fa535-05ef-4713-8f4f-a3d825116d1f"
+  "ownerName": "container (PID: 702)",
+  "sessionId": "473b0d36-5ff2-4d8d-b734-0a4b7bed11fc"
 }
```

**Status codes:** official: [201] | aksh: [201]

**Timing (ms):** p50: official 48.8 / aksh 31.0 | p95: official 48.8 / aksh 31.0

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,33 +2,33 @@
   "change_order": 1,
   "steps": [
     {
-      "completed_at": "2026-06-30T15:38:27.092Z",
+      "completed_at": "2026-07-08T20:12:36.131Z",
       "conclusion": 2,
-      "external_id": "dfd629d7-e4c2-4dd7-94cd-0581d93b66dd",
+      "external_id": "2be674b7-49a3-44e6-84df-3697b2b8bb5f",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T15:38:27.058Z",
+      "started_at": "2026-07-08T20:12:36.131Z",
       "status": 6
     },
     {
-      "completed_at": "2026-06-30T15:38:27.142Z",
+      "completed_at": "2026-07-08T20:12:36.564Z",
       "conclusion": 2,
-      "external_id": "315f5d94-5181-442a-94f0-5f2b2a05b79d",
+      "external_id": "05f0bd00-dea9-4387-b25c-36a36af134fc",
       "name": "Run echo \"value=42\" >> \"$GITHUB_OUTPUT\"",
       "number": 2,
-      "started_at": "2026-06-30T15:38:27.096Z",
+      "started_at": "2026-07-08T20:12:36.563Z",
       "status": 6
     },
     {
-      "completed_at": "2026-06-30T15:38:27.266Z",
+      "completed_at": "2026-07-08T20:12:37.114Z",
       "conclusion": 2,
-      "external_id": "16bd6216-c7c3-4189-935a-3c112b8a0f9d",
+      "external_id": "bb291f65-5b56-4d7b-b327-a65eb5b9f30b",
       "name": "Complete job",
       "number": 3,
-      "started_at": "2026-06-30T15:38:27.146Z",
+      "started_at": "2026-07-08T20:12:37.114Z",
       "status": 6
     }
   ],
-  "workflow_job_run_backend_id": "b95e454f-3498-5814-9355-8d4f55dbe4ad",
-  "workflow_run_backend_id": "59c06d0e-48c4-4eff-b29f-a2ca28da96dc"
+  "workflow_job_run_backend_id": "ccf9c861-5292-555b-9bd4-cc28daa8af04",
+  "workflow_run_backend_id": "e54c29bc-b8b6-4759-9311-b471c548a8f5"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200] | aksh: [200]

**Timing (ms):** p50: official 85.5 / aksh 46.7 | p95: official 85.5 / aksh 46.7

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "b95e454f-3498-5814-9355-8d4f55dbe4ad",
-  "workflow_run_backend_id": "59c06d0e-48c4-4eff-b29f-a2ca28da96dc"
+  "workflow_job_run_backend_id": "ccf9c861-5292-555b-9bd4-cc28daa8af04",
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
-  "logs_url": "https://productionresultssa1.blob.core.windows.net/actions-results/59c06d0e-48c4-4eff-b29f-a2ca28da96dc/workflow-job-run-b95e454f-3498-5814-9355-8d4f55dbe4ad/logs/job/job-logs.txt?se=2026-06-30T16%3A38%3A48Z&sig=***REDACTED***%3D&ske=2026-06-30T19%3A10%3A45Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A10%3A45Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A38%3A43Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa12.blob.core.windows.net/actions-results/e54c29bc-b8b6-4759-9311-b471c548a8f5/workflow-job-run-ccf9c861-5292-555b-9bd4-cc28daa8af04/logs/job/job-logs.txt?se=2026-07-08T21%3A12%3A37Z&sig=As%2F%***REDACTED***%3D&ske=2026-07-09T00%3A10%3A21Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A10%3A21Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A32Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200, 200] | aksh: [200]

**Timing (ms):** p50: official 34.8 / aksh 45.2 | p95: official 34.8 / aksh 45.2

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "dfd629d7-e4c2-4dd7-94cd-0581d93b66dd",
-  "workflow_job_run_backend_id": "b95e454f-3498-5814-9355-8d4f55dbe4ad",
-  "workflow_run_backend_id": "59c06d0e-48c4-4eff-b29f-a2ca28da96dc"
+  "step_backend_id": "2be674b7-49a3-44e6-84df-3697b2b8bb5f",
+  "workflow_job_run_backend_id": "ccf9c861-5292-555b-9bd4-cc28daa8af04",
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
-  "logs_url": "https://productionresultssa1.blob.core.windows.net/actions-results/59c06d0e-48c4-4eff-b29f-a2ca28da96dc/workflow-job-run-b95e454f-3498-5814-9355-8d4f55dbe4ad/logs/steps/step-logs-dfd629d7-e4c2-4dd7-94cd-0581d93b66dd.txt?se=2026-06-30T16%3A38%3A28Z&sig=FL%***REDACTED***%3D&ske=2026-06-30T19%3A10%3A44Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A10%3A44Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A38%3A23Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa12.blob.core.windows.net/actions-results/e54c29bc-b8b6-4759-9311-b471c548a8f5/workflow-job-run-ccf9c861-5292-555b-9bd4-cc28daa8af04/logs/steps/step-logs-2be674b7-49a3-44e6-84df-3697b2b8bb5f.txt?se=2026-07-08T21%3A12%3A36Z&sig=EqzGm7rkCVxE1dcvJSLlRgRmIMHBR%2BqR4mjwlpZ8dpk%3D&ske=2026-07-09T00%3A10%3A55Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A10%3A55Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A12%3A31Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 35.7 / aksh 41.6 | p95: official 124.1 / aksh 408.4

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
-  "jobMessageId": "b95e454f-3498-5814-9355-8d4f55dbe4ad",
-  "runnerOS": "macOS"
+  "jobMessageId": "ccf9c861-5292-555b-9bd4-cc28daa8af04",
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
-          "v": "28456675895"
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
-                      "v": "2026-06-30T15:15:40Z"
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
-          "v": 84333269802
+          "v": 85971410446
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
   "jobDisplayName": "producer",
-  "jobId": "b95e454f-3498-5814-9355-8d4f55dbe4ad",
+  "jobId": "ccf9c861-5292-555b-9bd4-cc28daa8af04",
   "jobName": "__default",
   "jobOutputs": {
     "col": 7,
@@ -874,34 +874,30 @@
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
-      "value": "gDNShl5hWjolhCP70--tpA0wzvwhwJScAxwJAFFuRkuPg"
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
+      "value": "***REDACTED***\\.vrTO_lNJIU6-AEG"
+    },
+    {
+      "type": "regex",
+      "value": "Neh1dy53U-***REDACTED***"
     }
   ],
   "messageType": "RunnerJobRequest",
   "plan": {
     "artifactLocation": "",
     "artifactUri": "",
-    "planId": "59c06d0e-48c4-4eff-b29f-a2ca28da96dc",
+    "planId": "e54c29bc-b8b6-4759-9311-b471c548a8f5",
     "planType": "actions",
     "version": 0
   },
@@ -911,7 +907,7 @@
       {
         "authorization": {
           "parameters": {
-            "AccessToken": "***REDACTED***"
+            "AccessToken": "***REDACTED***"
           },
           "scheme": "OAuth"
         },
@@ -928,7 +924,7 @@
         "isReady": true,
         "isShared": false,
         "name": "SystemVssConnection",
-        "url": "https://run-actions-3-azure-eastus.actions.githubusercontent.com/1/"
+        "url": "https://run-actions-1-azure-eastus.actions.githubusercontent.com/29/"
       }
     ]
   },
@@ -938,7 +934,7 @@
       "condition": "success()",
       "contextName": "gen",
       "continueOnError": null,
-      "id": "315f5d94-5181-442a-94f0-5f2b2a05b79d",
+      "id": "05f0bd00-dea9-4387-b25c-36a36af134fc",
       "inputs": {
         "map": [
           {
@@ -967,7 +963,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "59c06d0e-48c4-4eff-b29f-a2ca28da96dc",
+    "id": "e54c29bc-b8b6-4759-9311-b471c548a8f5",
     "location": null
   },
   "variables": {
@@ -1087,7 +1083,7 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
@@ -1106,13 +1102,13 @@
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
-      "value": "59c06d0e-48c4-4eff-b29f-a2ca28da96dc.producer.__default"
+      "value": "e54c29bc-b8b6-4759-9311-b471c548a8f5.producer.__default"
     },
     "system.phaseDisplayName": {
       "value": "producer"
```

**Status codes:** official: [200, 200] | aksh: [200]

**Timing (ms):** p50: official 517.4 / aksh 435.5 | p95: official 517.4 / aksh 435.5

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,63 +2,51 @@
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
   "conclusion": "succeeded",
-  "jobId": "b95e454f-3498-5814-9355-8d4f55dbe4ad",
-  "outputs": {
-    "value": {
-      "value": "42"
-    }
-  },
-  "planId": "59c06d0e-48c4-4eff-b29f-a2ca28da96dc",
+  "jobId": "ccf9c861-5292-555b-9bd4-cc28daa8af04",
+  "outputs": {},
+  "planId": "e54c29bc-b8b6-4759-9311-b471c548a8f5",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-06-30T15:38:27.092036Z",
+      "completed_at": "2026-07-08T20:12:37.838Z",
       "conclusion": "succeeded",
-      "external_id": "dfd629d7-e4c2-4dd7-94cd-0581d93b66dd",
+      "external_id": "2be674b7-49a3-44e6-84df-3697b2b8bb5f",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T15:38:27.058564Z",
+      "started_at": "2026-07-08T20:12:37.838Z",
       "status": "completed",
       "type": "runner"
     },
     {
       "action_name": "sh",
       "annotations": [],
-      "completed_at": "2026-06-30T15:38:27.1428Z",
+      "completed_at": "2026-07-08T20:12:37.838Z",
       "conclusion": "succeeded",
-      "external_id": "315f5d94-5181-442a-94f0-5f2b2a05b79d",
+      "external_id": "05f0bd00-dea9-4387-b25c-36a36af134fc",
       "name": "Run echo \"value=42\" >> \"$GITHUB_OUTPUT\"",
       "number": 2,
-      "started_at": "2026-06-30T15:38:27.096958Z",
+      "started_at": "2026-07-08T20:12:37.838Z",
       "status": "completed",
       "type": "run"
     },
     {
       "action_name": "complete_job",
       "annotations": [],
-      "completed_at": "2026-06-30T15:38:27.266359Z",
+      "completed_at": "2026-07-08T20:12:37.838Z",
       "conclusion": "succeeded",
-      "external_id": "16bd6216-c7c3-4189-935a-3c112b8a0f9d",
+      "external_id": "bb291f65-5b56-4d7b-b327-a65eb5b9f30b",
       "name": "Complete job",
       "number": 3,
-      "started_at": "2026-06-30T15:38:27.146739Z",
+      "started_at": "2026-07-08T20:12:37.838Z",
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

**Timing (ms):** p50: official 129.3 / aksh 41.0 | p95: official 129.3 / aksh 41.0

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "b95e454f-3498-5814-9355-8d4f55dbe4ad",
-  "planId": "59c06d0e-48c4-4eff-b29f-a2ca28da96dc"
+  "jobId": "ccf9c861-5292-555b-9bd4-cc28daa8af04",
+  "planId": "e54c29bc-b8b6-4759-9311-b471c548a8f5"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-06-30T15:48:26.85091374Z"
+  "lockedUntil": "2026-07-08T20:22:36.148224944Z"
 }
```

**Status codes:** official: [200, 200] | aksh: [200]

**Timing (ms):** p50: official 46.2 / aksh 50.0 | p95: official 46.2 / aksh 50.0
