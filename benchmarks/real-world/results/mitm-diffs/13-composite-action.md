# MITM comparison: 13-composite-action

**official**: ok — 36 flows
**aksh**: N/A — 42 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/session` | 0 | 1 | - | 37.9 |  | 204 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1` | 0 | 1 | - | 24.2 |  | 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 6 | 5 | 25.7 | 26.7 | 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-13-composite-action-1783541710&includeCapabilities=False` | 0 | 1 | - | 52.8 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official-13-composite-action-2026-06-30T16-00-10Z&includeCapabilities=False` | 1 | 0 | 26.4 | - | 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 105.6 | 61.1 | 200 | 200 |
| GET | `/_ws/ingest.sock` | 0 | 1 | - | 25.7 |  | 401 |
| GET | `/actions/checkout/tar.gz/***REDACTED***` | 1 | 1 | 152.4 | 130.1 | 200 | 200 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 | - | 2835.4 |  | 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 | - | 3820.6 |  | 200 |
| GET | `/health` | 2 | 2 | 27.9 | 23.8 | 200, 200 | 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 0 |  | None |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 2 | 0 | 25045.2 | - | 202, None |  |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 3215.1 |  | 200 |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 2 | 0 | 3106.2 | - | 200, None |  |
| GET | `/ready` | 1 | 1 | 17.4 | 22.1 | 204 | 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 151.1 | 178.9 | 200 | 200 |
| POST | `/_apis/oauth2/token` | 0 | 2 | - | 86.2 |  | 200, 200 |
| POST | `/_apis/oauth2/token/{guid}` | 2 | 0 | 28.2 | - | 200, 200 |  |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 0 | 1 | - | 46.3 |  | 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 1 | 0 | 53.6 | - | 200 |  |
| POST | `/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 1 | 1 | 97.7 | 103.6 | 200 | 200 |
| POST | `/actions/runner-registration` | 1 | 1 | 268.3 | 212.1 | 200 | 200 |
| POST | `/session` | 1 | 1 | 34.9 | 37.3 | 201 | 201 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 5 | 2 | 72.3 | 48.4 | 200, 200, 200, 200, 200 | 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 0 | 1 | - | 38.5 |  | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 0 | 3 | - | 81.1 |  | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 | 54.3 | 33.2 | 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 4 | 3 | 61.4 | 39.6 | 200, 200, 200, 200 | 200, 200, 200 |
| POST | `/{n}/acquirejob` | 1 | 1 | 431.9 | 374.1 | 200 | 200 |
| POST | `/{n}/completejob` | 1 | 1 | 43.8 | 40.2 | 204 | 204 |
| POST | `/{n}/renewjob` | 1 | 1 | 46.2 | 129.3 | 200 | 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A16%3A05Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A08Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A08Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A16%3A00Z&sv=2025-11-05` | 0 | 1 | - | 23.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A26Z&sig=0%***REDACTED***%3D&ske=2026-07-09T00%3A00%3A17Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A00%3A17Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A21Z&sv=2025-11-05` | 0 | 1 | - | 31.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A16%3A04Z&sig=8VODER0bFYJ1K5RHlzvsfOKi2tm%2B0hj0C%2FUjNz5iTpk%3D&ske=2026-07-09T00%3A09%3A56Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A59Z&sv=2025-11-05` | 0 | 1 | - | 24.8 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A16%3A04Z&sig=***REDACTED***%2BwXb0%3D&ske=2026-07-09T00%3A09%3A55Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A55Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A59Z&sv=2025-11-05` | 0 | 1 | - | 159.9 |  | 201 |

## Missing endpoints

### official only

- `GET /_apis/distributedtask/pools/{n}/agents?agentName=mitm-official-13-composite-action-2026-06-30T16-00-10Z&includeCapabilities=False`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token/{guid}`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64`

### aksh only

- `DELETE /session`
- `GET /_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-13-composite-action-1783541710&includeCapabilities=False`
- `GET /_ws/ingest.sock`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`
- `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`
- `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A16%3A05Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A08Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A08Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A16%3A00Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A26Z&sig=0%***REDACTED***%3D&ske=2026-07-09T00%3A00%3A17Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A00%3A17Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A21Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A16%3A04Z&sig=8VODER0bFYJ1K5RHlzvsfOKi2tm%2B0hj0C%2FUjNz5iTpk%3D&ske=2026-07-09T00%3A09%3A56Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A59Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A16%3A04Z&sig=***REDACTED***%2BwXb0%3D&ske=2026-07-09T00%3A09%3A55Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A55Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A59Z&sv=2025-11-05`

## Per-endpoint comparison

### `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`

**Header key differences:**

- official only: `{'accept-encoding', 'authorization', 'accept-language', 'x-tfs-fedauthredirect'}`

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

**Timing (ms):** p50: official 22.6 / aksh 23.6 | p95: official 35.1 / aksh 38.6

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{'accept-encoding', 'accept-language', 'x-tfs-fedauthredirect'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -10,7 +10,7 @@
       "isInternal": true,
       "name": "Default",
       "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 3,
+      "size": 2,
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

**Timing (ms):** p50: official 105.6 / aksh 61.1 | p95: official 105.6 / aksh 61.1

### `GET /actions/checkout/tar.gz/***REDACTED***`

**Header key differences:**

- official only: `{'authorization'}`
- aksh only: `{'accept'}`

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 152.4 / aksh 130.1 | p95: official 152.4 / aksh 130.1

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 32.5 / aksh 24.0 | p95: official 32.5 / aksh 24.0

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 17.4 / aksh 22.1 | p95: official 17.4 / aksh 22.1

### `POST /_apis/distributedtask/pools/{n}/agents`

**Header key differences:**

- official only: `{'accept-encoding', 'accept-language', 'x-tfs-fedauthredirect'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,12 +2,12 @@
   "authorization": {
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "***REDACTED***+***REDACTED***+***REDACTED***/***REDACTED***+***REDACTED***+RU0lwrMzLkOcX6iz34b/***REDACTED***/***REDACTED***=="
+      "modulus": "***REDACTED***+t4RjM7vBD8g+bZAsrjuWE4caRFuXrXh+dNlrkA+***REDACTED***/C3kBf6Xx3KzzDdBVHRD+UXAL3bDA9/***REDACTED***/***REDACTED***=="
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
-  "name": "mitm-official-13-composite-action-2026-06-30T16-00-10Z",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-13-composite-action-1783541710",
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
-    "clientId": "3a319728-f233-4e5f-9ced-1a57cb616ece",
+    "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/_apis/oauth2/token",
+    "clientId": "3f9b4baa-d744-4549-aca5-99480864cec7",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "***REDACTED***+***REDACTED***+***REDACTED***/***REDACTED***+***REDACTED***+RU0lwrMzLkOcX6iz34b/***REDACTED***/***REDACTED***=="
+      "modulus": "***REDACTED***+t4RjM7vBD8g+bZAsrjuWE4caRFuXrXh+dNlrkA+***REDACTED***/C3kBf6Xx3KzzDdBVHRD+UXAL3bDA9/***REDACTED***/***REDACTED***=="
     }
   },
-  "createdOn": "2026-06-30T16:00:17.507Z",
+  "createdOn": "2026-07-08T20:15:12.597Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
-  "ephemeral": false,
-  "id": 10,
+  "ephemeral": true,
+  "id": 687,
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
-  "name": "mitm-official-13-composite-action-2026-06-30T16-00-10Z",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-13-composite-action-1783541710",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -60,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-10",
+  "queueName": "taskagent-687",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 151.1 / aksh 178.9 | p95: official 151.1 / aksh 178.9

### `POST /actions/build/{guid}/jobs/{guid}/runnerresolve/actions`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

_identical_

**Response body diff:**

_identical_

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 97.7 / aksh 103.6 | p95: official 97.7 / aksh 103.6

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

**Timing (ms):** p50: official 268.3 / aksh 212.1 | p95: official 268.3 / aksh 212.1

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
-    "id": 10,
-    "name": "mitm-official-13-composite-action-2026-06-30T16-00-10Z",
-    "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+    "id": 687,
+    "name": "aksh-capture-13-composite-action-1783541710",
+    "osDescription": "linux aarch64",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "Nuraydias-Mac-Studio (PID: 46501)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 3544)",
+  "sessionId": "10f3b9f0-8ca4-4514-9814-fc8686bca42b",
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
-  "ownerName": "Nuraydias-Mac-Studio (PID: 46501)",
-  "sessionId": "ea5e0c93-bd65-4262-9ab2-da0dc4520dc8"
+  "ownerName": "container (PID: 3544)",
+  "sessionId": "18609602-bc4c-4b78-9486-0c027c61752b"
 }
```

**Status codes:** official: [201] | aksh: [201]

**Timing (ms):** p50: official 34.9 / aksh 37.3 | p95: official 34.9 / aksh 37.3

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

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
-      "external_id": "0af600a2-e822-414d-a507-37fbdca1e19d",
+      "completed_at": "2026-07-08T20:15:25.812Z",
+      "conclusion": 2,
+      "external_id": "4989cdf4-7cb3-46c7-986e-30323e544a2d",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T16:00:25.837Z",
-      "status": 3
+      "started_at": "2026-07-08T20:15:25.812Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-08T20:16:04.711Z",
+      "conclusion": 3,
+      "external_id": "58118346-2b21-4e03-a739-55af34c23f3f",
+      "name": "actions/checkout@v4",
+      "number": 2,
+      "started_at": "2026-07-08T20:15:26.319Z",
+      "status": 6
     }
   ],
-  "workflow_job_run_backend_id": "469f97e4-8e43-5ab3-a54e-1c12d6a1fdf9",
-  "workflow_run_backend_id": "f43472b4-7ab0-44c8-ab53-505f7cb6a903"
+  "workflow_job_run_backend_id": "4b30cdae-3fdd-580a-8a9a-b09a23545a69",
+  "workflow_run_backend_id": "3079ab10-dff0-45ad-ab43-dfb8d675b814"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 53.2 / aksh 52.6 | p95: official 136.5 / aksh 52.6

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "469f97e4-8e43-5ab3-a54e-1c12d6a1fdf9",
-  "workflow_run_backend_id": "f43472b4-7ab0-44c8-ab53-505f7cb6a903"
+  "workflow_job_run_backend_id": "4b30cdae-3fdd-580a-8a9a-b09a23545a69",
+  "workflow_run_backend_id": "3079ab10-dff0-45ad-ab43-dfb8d675b814"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa8.blob.core.windows.net/actions-results/f43472b4-7ab0-44c8-ab53-505f7cb6a903/workflow-job-run-469f97e4-8e43-5ab3-a54e-1c12d6a1fdf9/logs/job/job-logs.txt?se=2026-06-30T17%3A01%3A17Z&sig=inMH7SmzJsSW%2Fkz%2Bvmzy29fMYTtwcRWO53gY4QTRXAY%3D&ske=2026-06-30T19%3A51%3A02Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A51%3A02Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T16%3A01%3A12Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa12.blob.core.windows.net/actions-results/3079ab10-dff0-45ad-ab43-dfb8d675b814/workflow-job-run-4b30cdae-3fdd-580a-8a9a-b09a23545a69/logs/job/job-logs.txt?se=2026-07-08T21%3A16%3A05Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A08Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A10%3A08Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A16%3A00Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 54.3 / aksh 33.2 | p95: official 54.3 / aksh 33.2

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "0af600a2-e822-414d-a507-37fbdca1e19d",
-  "workflow_job_run_backend_id": "469f97e4-8e43-5ab3-a54e-1c12d6a1fdf9",
-  "workflow_run_backend_id": "f43472b4-7ab0-44c8-ab53-505f7cb6a903"
+  "step_backend_id": "4989cdf4-7cb3-46c7-986e-30323e544a2d",
+  "workflow_job_run_backend_id": "4b30cdae-3fdd-580a-8a9a-b09a23545a69",
+  "workflow_run_backend_id": "3079ab10-dff0-45ad-ab43-dfb8d675b814"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa8.blob.core.windows.net/actions-results/f43472b4-7ab0-44c8-ab53-505f7cb6a903/workflow-job-run-469f97e4-8e43-5ab3-a54e-1c12d6a1fdf9/logs/steps/step-logs-0af600a2-e822-414d-a507-37fbdca1e19d.txt?se=2026-06-30T17%3A00%3A26Z&sig=tqeJBb9%2F96n06HfbdWJgsq%2Fy6g3AAG7OxPXG4uF7cWk%3D&ske=2026-06-30T19%3A51%3A52Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A51%3A52Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T16%3A00%3A21Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa12.blob.core.windows.net/actions-results/3079ab10-dff0-45ad-ab43-dfb8d675b814/workflow-job-run-4b30cdae-3fdd-580a-8a9a-b09a23545a69/logs/steps/step-logs-4989cdf4-7cb3-46c7-986e-30323e544a2d.txt?se=2026-07-08T21%3A15%3A26Z&sig=0%***REDACTED***%3D&ske=2026-07-09T00%3A00%3A17Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A00%3A17Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A21Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 72.1 / aksh 41.6 | p95: official 99.4 / aksh 43.1

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
-  "jobMessageId": "469f97e4-8e43-5ab3-a54e-1c12d6a1fdf9",
-  "runnerOS": "macOS"
+  "jobMessageId": "4b30cdae-3fdd-580a-8a9a-b09a23545a69",
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
-          "v": "28458099588"
+          "v": "28972659317"
         },
         {
           "k": "run_number",
-          "v": "1"
+          "v": "17"
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
-          "v": 84338342323
+          "v": 85971977581
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
-  "jobId": "469f97e4-8e43-5ab3-a54e-1c12d6a1fdf9",
+  "jobId": "4b30cdae-3fdd-580a-8a9a-b09a23545a69",
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
-      "value": "***REDACTED***\\.5oo-Wab7h6FsTKtziTolKJiy-mdPn-ZJHXy2F1w5q"
-    },
-    {
-      "type": "regex",
-      "value": "-nCHfyH3OTjkgBL-DmxphGni4sFxW8l-_AHHXla36FYBA"
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
+      "value": "***REDACTED***\\.F3YPWWbrEytZvZq"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***-NoEwBvmew2MkAhbN0mFI_A"
     }
   ],
   "messageType": "RunnerJobRequest",
   "plan": {
     "artifactLocation": "",
     "artifactUri": "",
-    "planId": "f43472b4-7ab0-44c8-ab53-505f7cb6a903",
+    "planId": "3079ab10-dff0-45ad-ab43-dfb8d675b814",
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
-        "url": "https://run-actions-3-azure-eastus.actions.githubusercontent.com/56/"
+        "url": "https://run-actions-1-azure-eastus.actions.githubusercontent.com/6/"
       }
     ]
   },
@@ -915,7 +911,7 @@
       "condition": "success()",
       "contextName": "__actions_checkout",
       "continueOnError": null,
-      "id": "46c295dc-c71f-425f-a585-43043f4699f2",
+      "id": "58118346-2b21-4e03-a739-55af34c23f3f",
       "name": "__actions_checkout",
       "reference": {
         "name": "actions/checkout",
@@ -930,7 +926,7 @@
       "condition": "success()",
       "contextName": "__self",
       "continueOnError": null,
-      "id": "b5f60cc4-5d96-4f00-937a-b9e4c9db098a",
+      "id": "1d16c75c-1316-4d6d-bf76-d7a6ba34dc74",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -967,7 +963,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "f43472b4-7ab0-44c8-ab53-505f7cb6a903",
+    "id": "3079ab10-dff0-45ad-ab43-dfb8d675b814",
     "location": null
   },
   "variables": {
@@ -1087,7 +1083,7 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "***REDACTED******REDACTED***"
+      "value": "***REDACTED******REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
@@ -1106,13 +1102,13 @@
     },
     "system.github.token": {
       "isSecret": true,
-      "value": "***REDACTED******REDACTED***"
+      "value": "***REDACTED******REDACTED***"
     },
     "system.github.token.permissions": {
       "value": "{\"Contents\":\"read\",\"Metadata\":\"read\",\"Packages\":\"read\"}"
     },
     "system.orchestrationId": {
-      "value": "f43472b4-7ab0-44c8-ab53-505f7cb6a903.build.__default"
+      "value": "3079ab10-dff0-45ad-ab43-dfb8d675b814.build.__default"
     },
     "system.phaseDisplayName": {
       "value": "build"
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 431.9 / aksh 374.1 | p95: official 431.9 / aksh 374.1

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,124 +2,83 @@
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
   "conclusion": "failed",
-  "jobId": "469f97e4-8e43-5ab3-a54e-1c12d6a1fdf9",
+  "jobId": "4b30cdae-3fdd-580a-8a9a-b09a23545a69",
   "outputs": {},
-  "planId": "f43472b4-7ab0-44c8-ab53-505f7cb6a903",
+  "planId": "3079ab10-dff0-45ad-ab43-dfb8d675b814",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-06-30T16:00:26.381722Z",
+      "completed_at": "2026-07-08T20:16:05.312Z",
       "conclusion": "succeeded",
-      "external_id": "0af600a2-e822-414d-a507-37fbdca1e19d",
+      "external_id": "4989cdf4-7cb3-46c7-986e-30323e544a2d",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T16:00:25.837182Z",
+      "started_at": "2026-07-08T20:16:05.312Z",
       "status": "completed",
       "type": "runner"
     },
     {
-      "action_name": "actions/checkout",
+      "action_name": "actions/checkout@v4",
       "annotations": [
         {
-          "endLine": 48,
+          "endLine": 1,
           "level": "failure",
-          "message": "unable to access 'https://github.com/preloopdev/aksh-conformance-sample/': SSL certificate problem: unable to get local issuer certificate",
-          "startLine": 48,
-          "stepNumber": 2
-        },
-        {
-          "endLine": 52,
-          "level": "failure",
-          "message": "unable to access 'https://github.com/preloopdev/aksh-conformance-sample/': SSL certificate problem: unable to get local issuer certificate",
-          "startLine": 52,
-          "stepNumber": 2
-        },
-        {
-          "endLine": 56,
-          "level": "failure",
-          "message": "unable to access 'https://github.com/preloopdev/aksh-conformance-sample/': SSL certificate problem: unable to get local issuer certificate",
-          "startLine": 56,
-          "stepNumber": 2
-        },
-        {
-          "endLine": 57,
-          "level": "failure",
-          "message": "The process '/opt/homebrew/bin/git' failed with exit code 128",
-          "startLine": 57,
+          "message": "node action exited with code 1",
+          "startLine": 1,
           "stepNumber": 2
         }
       ],
-      "completed_at": "2026-06-30T16:00:58.098768Z",
+      "completed_at": "2026-07-08T20:16:05.312Z",
       "conclusion": "failed",
-      "external_id": "46c295dc-c71f-425f-a585-43043f4699f2",
-      "name": "Run actions/checkout@v4",
+      "external_id": "58118346-2b21-4e03-a739-55af34c23f3f",
+      "name": "actions/checkout@v4",
       "number": 2,
-      "ref": "v4",
-      "started_at": "2026-06-30T16:00:26.386641Z",
+      "started_at": "2026-07-08T20:16:05.312Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
+      "action_name": "./.github/actions/greet",
       "annotations": [],
-      "completed_at": "2026-06-30T16:00:58.101014Z",
+      "completed_at": "2026-07-08T20:16:05.312Z",
       "conclusion": "skipped",
-      "external_id": "b5f60cc4-5d96-4f00-937a-b9e4c9db098a",
-      "name": "Run ./.github/actions/greet",
+      "external_id": "1d16c75c-1316-4d6d-bf76-d7a6ba34dc74",
+      "name": "./.github/actions/greet",
       "number": 3,
-      "started_at": "2026-06-30T16:00:58.100632Z",
-      "status": "completed"
+      "started_at": "2026-07-08T20:16:05.312Z",
+      "status": "completed",
+      "type": "action"
     },
     {
-      "action_name": "actions/checkout",
+      "action_name": "actions/checkout@v4",
       "annotations": [],
-      "completed_at": "2026-06-30T16:00:58.341642Z",
+      "completed_at": "2026-07-08T20:16:05.312Z",
       "conclusion": "succeeded",
-      "external_id": "37ec34e1-87f9-4310-92d5-1d1b6ee802cb",
-      "name": "Post Run actions/checkout@v4",
-      "number": 6,
-      "ref": "v4",
-      "started_at": "2026-06-30T16:00:58.102542Z",
+      "external_id": "__post_58118346-2b21-4e03-a739-55af34c23f3f",
+      "name": "Post actions/checkout@v4",
+      "number": 4,
+      "started_at": "2026-07-08T20:16:05.312Z",
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
-      "completed_at": "2026-06-30T16:00:58.365484Z",
+      "annotations": [],
+      "completed_at": "2026-07-08T20:16:05.312Z",
       "conclusion": "succeeded",
-      "external_id": "5a971640-e1ef-497f-8926-3d4a82d6433e",
+      "external_id": "bc1fc114-ae70-45b0-8e86-510154d78d60",
       "name": "Complete job",
-      "number": 7,
-      "started_at": "2026-06-30T16:00:58.344188Z",
+      "number": 5,
+      "started_at": "2026-07-08T20:16:05.312Z",
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

**Timing (ms):** p50: official 43.8 / aksh 40.2 | p95: official 43.8 / aksh 40.2

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'x-actions-session', 'accept-language'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "469f97e4-8e43-5ab3-a54e-1c12d6a1fdf9",
-  "planId": "f43472b4-7ab0-44c8-ab53-505f7cb6a903"
+  "jobId": "4b30cdae-3fdd-580a-8a9a-b09a23545a69",
+  "planId": "3079ab10-dff0-45ad-ab43-dfb8d675b814"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-06-30T16:10:25.599798882Z"
+  "lockedUntil": "2026-07-08T20:25:25.836485768Z"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 46.2 / aksh 129.3 | p95: official 46.2 / aksh 129.3
