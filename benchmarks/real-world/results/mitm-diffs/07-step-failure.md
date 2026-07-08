# MITM comparison: 07-step-failure

**official**: ok — 50 flows
**aksh**: N/A — 43 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/_apis/distributedtask/pools/{n}/agents/{n}` | 1 | 0 | 121.7 | - | 204 |  |
| DELETE | `/session` | 1 | 1 | 26.8 | 117.5 | 204 | 204 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1` | 0 | 1 | - | 33.9 |  | 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 9 | 5 | 55.8 | 22.0 | 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-07-step-failure-1783540818&includeCapabilities=False` | 0 | 1 | - | 24.1 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official&includeCapabilities=False` | 2 | 0 | 26.9 | - | 200, 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 23.5 | 52.3 | 200 | 200 |
| GET | `/_ws/ingest.sock` | 0 | 1 | - | 122.8 |  | 401 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 | - | 2874.1 |  | 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 | - | 2742.1 |  | 200 |
| GET | `/health` | 2 | 2 | 55.5 | 39.2 | 200, 200 | 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 0 |  | None |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 1 | 0 | 0 | - | None |  |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 6738.9 |  | 200 |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 4 | 0 | 13369.0 | - | 200, 202, 404, None |  |
| GET | `/ready` | 1 | 1 | 54.4 | 19.9 | 204 | 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 169.4 | 73.0 | 200 | 200 |
| POST | `/_apis/oauth2/token` | 0 | 2 | - | 83.2 |  | 200, 200 |
| POST | `/_apis/oauth2/token/{guid}` | 14 | 0 | 44.7 | - | 200, 200, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400 |  |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 0 | 1 | - | 49.8 |  | 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 1 | 0 | 41.2 | - | 200 |  |
| POST | `/actions/runner-registration` | 2 | 1 | 222.3 | 160.0 | 200, 200 | 200 |
| POST | `/session` | 1 | 1 | 31.6 | 37.9 | 201 | 201 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 2 | 35.0 | 47.6 | 200 | 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 0 | 1 | - | 44.0 |  | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 0 | 4 | - | 115.7 |  | 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 | 117.3 | 94.7 | 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 4 | 4 | 36.0 | 52.8 | 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/{n}/acquirejob` | 1 | 1 | 500.0 | 437.8 | 200 | 200 |
| POST | `/{n}/completejob` | 1 | 1 | 52.9 | 43.3 | 204 | 204 |
| POST | `/{n}/renewjob` | 1 | 1 | 36.9 | 84.2 | 200 | 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A00%3A36Z&sig=***REDACTED***%3D&ske=2026-07-08T21%3A01%3A55Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A01%3A55Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A31Z&sv=2025-11-05` | 0 | 1 | - | 76.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A00%3A34Z&sig=tj3fvDMMIBwXQg%2BBBGc8jcwFGPwYW5c%2Fbnj7R8KTn4w%3D&ske=2026-07-08T23%3A59%3A37Z&skoid={guid}&sks=b&skt=2026-07-08T19%3A59%3A37Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A29Z&sv=2025-11-05` | 0 | 1 | - | 25.6 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A00%3A35Z&sig=6lp2wqBDAZS%***REDACTED***%3D&ske=2026-07-09T00%3A00%3A23Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A00%3A23Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A30Z&sv=2025-11-05` | 0 | 1 | - | 77.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A00%3A35Z&sig=7mRO4p0MCCCJ%2Bz2pep22ngN09mP7uzpEfb3gMLFM%2F%2BU%3D&ske=2026-07-08T23%3A59%3A38Z&skoid={guid}&sks=b&skt=2026-07-08T19%3A59%3A38Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A30Z&sv=2025-11-05` | 0 | 1 | - | 84.6 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A00%3A35Z&sig=***REDACTED***%2FLRTg%3D&ske=2026-07-09T00%3A00%3A00Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A00%3A00Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A30Z&sv=2025-11-05` | 0 | 1 | - | 72.7 |  | 201 |

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
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-07-step-failure-1783540818&includeCapabilities=False`
- `GET /_ws/ingest.sock`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`
- `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`
- `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A00%3A36Z&sig=***REDACTED***%3D&ske=2026-07-08T21%3A01%3A55Z&skoid={guid}&sks=b&skt=2026-07-08T17%3A01%3A55Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A31Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A00%3A34Z&sig=tj3fvDMMIBwXQg%2BBBGc8jcwFGPwYW5c%2Fbnj7R8KTn4w%3D&ske=2026-07-08T23%3A59%3A37Z&skoid={guid}&sks=b&skt=2026-07-08T19%3A59%3A37Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A29Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A00%3A35Z&sig=6lp2wqBDAZS%***REDACTED***%3D&ske=2026-07-09T00%3A00%3A23Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A00%3A23Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A30Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A00%3A35Z&sig=7mRO4p0MCCCJ%2Bz2pep22ngN09mP7uzpEfb3gMLFM%2F%2BU%3D&ske=2026-07-08T23%3A59%3A38Z&skoid={guid}&sks=b&skt=2026-07-08T19%3A59%3A38Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A30Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A00%3A35Z&sig=***REDACTED***%2FLRTg%3D&ske=2026-07-09T00%3A00%3A00Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A00%3A00Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A30Z&sv=2025-11-05`

## Per-endpoint comparison

### `DELETE /session`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 26.8 / aksh 117.5 | p95: official 26.8 / aksh 117.5

### `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`

**Header key differences:**

- official only: `{'x-tfs-fedauthredirect', 'accept-encoding', 'accept-language', 'authorization'}`

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

**Timing (ms):** p50: official 21.9 / aksh 21.8 | p95: official 122.6 / aksh 23.5

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{'x-tfs-fedauthredirect', 'accept-language', 'accept-encoding'}`

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

**Timing (ms):** p50: official 23.5 / aksh 52.3 | p95: official 23.5 / aksh 52.3

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 55.5 / aksh 59.2 | p95: official 55.5 / aksh 59.2

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 54.4 / aksh 19.9 | p95: official 54.4 / aksh 19.9

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
-      "modulus": "0w2CrSXf4BCyR8SQFygvrZR+***REDACTED***+I5N+zWYprgMlnReIxQdzyvB/+3YIQS+Wt/***REDACTED***/mu2DvbIlb/***REDACTED***/***REDACTED***/***REDACTED***/Bl2SMNDw=="
+      "modulus": "1bBoVsDi05RZhAUimkQNbFheVs8/mUI0MX2wWl2igtb8w6NQ7F+***REDACTED***+P3oqdzMjcwsLlQrrD9EQ1hpmH+d+***REDACTED***/xVkj+7Ef7QZxZD51dX0LDgq9uaNSq+***REDACTED***=="
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
+  "name": "aksh-capture-07-step-failure-1783540818",
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
-    "clientId": "045b7820-fd57-4627-9201-e2fb54e94e41",
+    "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/_apis/oauth2/token",
+    "clientId": "c652078c-93d8-42cc-bbb2-414898953267",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "0w2CrSXf4BCyR8SQFygvrZR+***REDACTED***+I5N+zWYprgMlnReIxQdzyvB/+3YIQS+Wt/***REDACTED***/mu2DvbIlb/***REDACTED***/***REDACTED***/***REDACTED***/Bl2SMNDw=="
+      "modulus": "1bBoVsDi05RZhAUimkQNbFheVs8/mUI0MX2wWl2igtb8w6NQ7F+***REDACTED***+P3oqdzMjcwsLlQrrD9EQ1hpmH+d+***REDACTED***/xVkj+7Ef7QZxZD51dX0LDgq9uaNSq+***REDACTED***=="
     }
   },
-  "createdOn": "2026-06-30T15:37:05.613Z",
+  "createdOn": "2026-07-08T20:00:19.907Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
-  "ephemeral": false,
-  "id": 5,
+  "ephemeral": true,
+  "id": 679,
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
+  "name": "aksh-capture-07-step-failure-1783540818",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -60,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-5",
+  "queueName": "taskagent-679",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 169.4 / aksh 73.0 | p95: official 169.4 / aksh 73.0

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

**Timing (ms):** p50: official 228.8 / aksh 160.0 | p95: official 228.8 / aksh 160.0

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
-    "id": 5,
-    "name": "mitm-official",
-    "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+    "id": 679,
+    "name": "aksh-capture-07-step-failure-1783540818",
+    "osDescription": "linux aarch64",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "Nuraydias-Mac-Studio (PID: 22897)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 267)",
+  "sessionId": "446ed598-9627-44fa-85f0-79eb6129f4ef",
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
-  "ownerName": "Nuraydias-Mac-Studio (PID: 22897)",
-  "sessionId": "154c4821-91cf-42b4-a6e9-a05b06039fa4"
+  "ownerName": "container (PID: 267)",
+  "sessionId": "cf13ba56-16e0-408a-a8ca-0a352700e10c"
 }
```

**Status codes:** official: [201] | aksh: [201]

**Timing (ms):** p50: official 31.6 / aksh 37.9 | p95: official 31.6 / aksh 37.9

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,51 +2,24 @@
   "change_order": 1,
   "steps": [
     {
-      "completed_at": "2026-06-30T15:37:11.136Z",
+      "completed_at": "2026-07-08T20:00:34.761Z",
       "conclusion": 2,
-      "external_id": "099d204f-1968-49fd-a01a-f52195597216",
+      "external_id": "462f5227-e30e-4abf-b13a-aa7cebc0a3a8",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T15:37:11.103Z",
+      "started_at": "2026-07-08T20:00:34.761Z",
       "status": 6
     },
     {
-      "completed_at": "2026-06-30T15:37:11.181Z",
+      "completed_at": "2026-07-08T20:00:35.278Z",
       "conclusion": 3,
-      "external_id": "13316043-a488-4401-890e-47013148073b",
+      "external_id": "d175dc5d-88a4-4334-8364-f57a239be0b6",
       "name": "Run exit 1",
       "number": 2,
-      "started_at": "2026-06-30T15:37:11.140Z",
-      "status": 6
-    },
-    {
-      "completed_at": "2026-06-30T15:37:11.204Z",
-      "conclusion": 2,
-      "external_id": "d749e538-bba7-4199-a4f1-db5f3d99fb6a",
-      "name": "Run echo ran-on-failure",
-      "number": 3,
-      "started_at": "2026-06-30T15:37:11.182Z",
-      "status": 6
-    },
-    {
-      "completed_at": "2026-06-30T15:37:11.205Z",
-      "conclusion": 7,
-      "external_id": "1ff72b49-63dc-438d-8b37-37ef76c34a11",
-      "name": "Run echo never",
-      "number": 4,
-      "started_at": "2026-06-30T15:37:11.205Z",
-      "status": 6
-    },
-    {
-      "completed_at": "2026-06-30T15:37:11.305Z",
-      "conclusion": 2,
-      "external_id": "e21617fd-3e12-41e3-889c-8f340192fdc0",
-      "name": "Complete job",
-      "number": 5,
-      "started_at": "2026-06-30T15:37:11.209Z",
+      "started_at": "2026-07-08T20:00:35.277Z",
       "status": 6
     }
   ],
-  "workflow_job_run_backend_id": "1d45b4e8-7863-5259-8570-91d239392100",
-  "workflow_run_backend_id": "57e76f84-9517-4235-b3b2-f863618d6e9d"
+  "workflow_job_run_backend_id": "ba441e7f-1724-57ea-a2cb-368e996373c3",
+  "workflow_run_backend_id": "3dc8805c-9a17-4da2-bea4-c365f8b6785f"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200] | aksh: [200, 200]

**Timing (ms):** p50: official 35.0 / aksh 47.7 | p95: official 35.0 / aksh 47.7

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "1d45b4e8-7863-5259-8570-91d239392100",
-  "workflow_run_backend_id": "57e76f84-9517-4235-b3b2-f863618d6e9d"
+  "workflow_job_run_backend_id": "ba441e7f-1724-57ea-a2cb-368e996373c3",
+  "workflow_run_backend_id": "3dc8805c-9a17-4da2-bea4-c365f8b6785f"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa4.blob.core.windows.net/actions-results/57e76f84-9517-4235-b3b2-f863618d6e9d/workflow-job-run-1d45b4e8-7863-5259-8570-91d239392100/logs/job/job-logs.txt?se=2026-06-30T16%3A37%3A36Z&sig=HHSeearnE20CyYkG%2Bnvz6e3lBzYUrw3LJvECyHnV11s%3D&ske=2026-06-30T19%3A10%3A39Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A10%3A39Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A37%3A31Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa3.blob.core.windows.net/actions-results/3dc8805c-9a17-4da2-bea4-c365f8b6785f/workflow-job-run-ba441e7f-1724-57ea-a2cb-368e996373c3/logs/job/job-logs.txt?se=2026-07-08T21%3A00%3A36Z&sig=***REDACTED***%3D&ske=2026-07-08T21%3A01%3A55Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T17%3A01%3A55Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A31Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 117.3 / aksh 94.7 | p95: official 117.3 / aksh 94.7

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "099d204f-1968-49fd-a01a-f52195597216",
-  "workflow_job_run_backend_id": "1d45b4e8-7863-5259-8570-91d239392100",
-  "workflow_run_backend_id": "57e76f84-9517-4235-b3b2-f863618d6e9d"
+  "step_backend_id": "462f5227-e30e-4abf-b13a-aa7cebc0a3a8",
+  "workflow_job_run_backend_id": "ba441e7f-1724-57ea-a2cb-368e996373c3",
+  "workflow_run_backend_id": "3dc8805c-9a17-4da2-bea4-c365f8b6785f"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa4.blob.core.windows.net/actions-results/57e76f84-9517-4235-b3b2-f863618d6e9d/workflow-job-run-1d45b4e8-7863-5259-8570-91d239392100/logs/steps/step-logs-099d204f-1968-49fd-a01a-f52195597216.txt?se=2026-06-30T16%3A37%3A12Z&sig=***REDACTED***%3D&ske=2026-06-30T19%3A10%3A09Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A10%3A09Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A37%3A07Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa3.blob.core.windows.net/actions-results/3dc8805c-9a17-4da2-bea4-c365f8b6785f/workflow-job-run-ba441e7f-1724-57ea-a2cb-368e996373c3/logs/steps/step-logs-462f5227-e30e-4abf-b13a-aa7cebc0a3a8.txt?se=2026-07-08T21%3A00%3A34Z&sig=tj3fvDMMIBwXQg%2BBBGc8jcwFGPwYW5c%2Fbnj7R8KTn4w%3D&ske=2026-07-08T23%3A59%3A37Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T19%3A59%3A37Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A00%3A29Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 38.6 / aksh 44.9 | p95: official 52.4 / aksh 90.8

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
-  "jobMessageId": "1d45b4e8-7863-5259-8570-91d239392100",
-  "runnerOS": "macOS"
+  "jobMessageId": "ba441e7f-1724-57ea-a2cb-368e996373c3",
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
-          "v": "28456595959"
+          "v": "28971761086"
         },
         {
           "k": "run_number",
-          "v": "1"
+          "v": "21"
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
-          "v": 84332985165
+          "v": 85968956299
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
-  "jobId": "1d45b4e8-7863-5259-8570-91d239392100",
+  "jobId": "ba441e7f-1724-57ea-a2cb-368e996373c3",
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
-      "value": "cC-***REDACTED***"
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
+      "value": "***REDACTED***\\.XsGhqV_A8EI-uvZ"
+    },
+    {
+      "type": "regex",
+      "value": "K9hb9g31OjZE-***REDACTED***--CwcpA"
     }
   ],
   "messageType": "RunnerJobRequest",
   "plan": {
     "artifactLocation": "",
     "artifactUri": "",
-    "planId": "57e76f84-9517-4235-b3b2-f863618d6e9d",
+    "planId": "3dc8805c-9a17-4da2-bea4-c365f8b6785f",
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
-        "url": "https://run-actions-3-azure-eastus.actions.githubusercontent.com/115/"
+        "url": "https://run-actions-1-azure-eastus.actions.githubusercontent.com/234/"
       }
     ]
   },
@@ -915,7 +911,7 @@
       "condition": "success()",
       "contextName": "__run",
       "continueOnError": null,
-      "id": "13316043-a488-4401-890e-47013148073b",
+      "id": "d175dc5d-88a4-4334-8364-f57a239be0b6",
       "inputs": {
         "map": [
           {
@@ -945,7 +941,7 @@
       "condition": "failure()",
       "contextName": "__run_2",
       "continueOnError": null,
-      "id": "d749e538-bba7-4199-a4f1-db5f3d99fb6a",
+      "id": "d67322fe-42c2-40ab-a12f-fb551f65c824",
       "inputs": {
         "map": [
           {
@@ -975,7 +971,7 @@
       "condition": "success()",
       "contextName": "__run_3",
       "continueOnError": null,
-      "id": "1ff72b49-63dc-438d-8b37-37ef76c34a11",
+      "id": "10930ea5-5c95-4f2c-991c-ddd5817d03f6",
       "inputs": {
         "map": [
           {
@@ -1004,7 +1000,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "57e76f84-9517-4235-b3b2-f863618d6e9d",
+    "id": "3dc8805c-9a17-4da2-bea4-c365f8b6785f",
     "location": null
   },
   "variables": {
@@ -1124,7 +1120,7 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
@@ -1143,13 +1139,13 @@
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
-      "value": "57e76f84-9517-4235-b3b2-f863618d6e9d.build.__default"
+      "value": "3dc8805c-9a17-4da2-bea4-c365f8b6785f.build.__default"
     },
     "system.phaseDisplayName": {
       "value": "build"
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 500.0 / aksh 437.8 | p95: official 500.0 / aksh 437.8

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,19 +2,19 @@
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
   "conclusion": "failed",
-  "jobId": "1d45b4e8-7863-5259-8570-91d239392100",
+  "jobId": "ba441e7f-1724-57ea-a2cb-368e996373c3",
   "outputs": {},
-  "planId": "57e76f84-9517-4235-b3b2-f863618d6e9d",
+  "planId": "3dc8805c-9a17-4da2-bea4-c365f8b6785f",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-06-30T15:37:11.136142Z",
+      "completed_at": "2026-07-08T20:00:36.352Z",
       "conclusion": "succeeded",
-      "external_id": "099d204f-1968-49fd-a01a-f52195597216",
+      "external_id": "462f5227-e30e-4abf-b13a-aa7cebc0a3a8",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T15:37:11.10304Z",
+      "started_at": "2026-07-08T20:00:36.352Z",
       "status": "completed",
       "type": "runner"
     },
@@ -22,69 +22,70 @@
       "action_name": "sh",
       "annotations": [
         {
-          "endLine": 5,
+          "endLine": 1,
           "level": "failure",
           "message": "Process completed with exit code 1.",
-          "startLine": 5,
+          "startLine": 1,
+          "stepNumber": 2
+        },
+        {
+          "endLine": 1,
+          "level": "failure",
+          "message": "process exit code 1",
+          "startLine": 1,
           "stepNumber": 2
         }
       ],
-      "completed_at": "2026-06-30T15:37:11.181036Z",
+      "completed_at": "2026-07-08T20:00:36.352Z",
       "conclusion": "failed",
-      "external_id": "13316043-a488-4401-890e-47013148073b",
+      "external_id": "d175dc5d-88a4-4334-8364-f57a239be0b6",
       "name": "Run exit 1",
       "number": 2,
-      "started_at": "2026-06-30T15:37:11.140538Z",
+      "started_at": "2026-07-08T20:00:36.352Z",
       "status": "completed",
       "type": "run"
     },
     {
       "action_name": "sh",
       "annotations": [],
-      "completed_at": "2026-06-30T15:37:11.204893Z",
+      "completed_at": "2026-07-08T20:00:36.352Z",
       "conclusion": "succeeded",
-      "external_id": "d749e538-bba7-4199-a4f1-db5f3d99fb6a",
+      "external_id": "d67322fe-42c2-40ab-a12f-fb551f65c824",
       "name": "Run echo ran-on-failure",
       "number": 3,
-      "started_at": "2026-06-30T15:37:11.18207Z",
+      "started_at": "2026-07-08T20:00:36.352Z",
       "status": "completed",
       "type": "run"
     },
     {
+      "action_name": "sh",
       "annotations": [],
-      "completed_at": "2026-06-30T15:37:11.205738Z",
+      "completed_at": "2026-07-08T20:00:36.352Z",
       "conclusion": "skipped",
-      "external_id": "1ff72b49-63dc-438d-8b37-37ef76c34a11",
+      "external_id": "10930ea5-5c95-4f2c-991c-ddd5817d03f6",
       "name": "Run echo never",
       "number": 4,
-      "started_at": "2026-06-30T15:37:11.205462Z",
-      "status": "completed"
+      "started_at": "2026-07-08T20:00:36.352Z",
+      "status": "completed",
+      "type": "run"
     },
     {
       "action_name": "complete_job",
       "annotations": [],
-      "completed_at": "2026-06-30T15:37:11.305652Z",
+      "completed_at": "2026-07-08T20:00:36.352Z",
       "conclusion": "succeeded",
-      "external_id": "e21617fd-3e12-41e3-889c-8f340192fdc0",
+      "external_id": "630ff530-6dc9-4ba5-9c9d-83330704d75e",
       "name": "Complete job",
       "number": 5,
-      "started_at": "2026-06-30T15:37:11.209356Z",
+      "started_at": "2026-07-08T20:00:36.352Z",
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

**Timing (ms):** p50: official 52.9 / aksh 43.3 | p95: official 52.9 / aksh 43.3

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "1d45b4e8-7863-5259-8570-91d239392100",
-  "planId": "57e76f84-9517-4235-b3b2-f863618d6e9d"
+  "jobId": "ba441e7f-1724-57ea-a2cb-368e996373c3",
+  "planId": "3dc8805c-9a17-4da2-bea4-c365f8b6785f"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-06-30T15:47:10.902789426Z"
+  "lockedUntil": "2026-07-08T20:10:34.819598601Z"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 36.9 / aksh 84.2 | p95: official 36.9 / aksh 84.2
