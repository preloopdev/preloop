# MITM comparison: 06-multi-step

**official**: ok — 47 flows
**aksh**: captured — 42 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/_apis/distributedtask/pools/{n}/agents/{n}` | 1 | 1 | 125.1 | 0.2 | 204 | 204 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 9 | 9 | 33.2 | 0.3 | 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official&includeCapabilities=False` | 2 | 2 | 28.9 | 0.3 | 200, 200 | 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/messages?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false&waitSeconds={n}` | 0 | 1 | - | 0.3 |  | 200 |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 23.2 | 0.2 | 200 | 200 |
| GET | `/health` | 2 | 0 | 26.0 | - | 200, 200 |  |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 1 | 0 | 0.0 | - | None |  |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 2 | 0 | 1906.9 | - | 200, None |  |
| GET | `/ready` | 1 | 0 | 20.2 | - | 204 |  |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 154.7 | 0.3 | 200 | 200 |
| POST | `/_apis/distributedtask/pools/{n}/sessions` | 0 | 1 | - | 0.3 |  | 201 |
| POST | `/_apis/oauth2/token/{guid}` | 13 | 0 | 22.7 | - | 200, 200, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400 |  |
| POST | `/_apis/v1/AgentRequest/{n}/{n}?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 0 | 1 | - | 0.2 |  | 200 |
| POST | `/_apis/v1/oauth2/token` | 0 | 13 | - | 0.2 |  | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 1 | 0 | 132.7 | - | 200 |  |
| POST | `/actions/runner-registration` | 2 | 0 | 191.7 | - | 200, 200 |  |
| POST | `/api/v3/actions/runner-registration` | 0 | 2 | - | 0.4 |  | 200, 200 |
| POST | `/broker/{n}/acquirejob` | 0 | 1 | - | 0.3 |  | 200 |
| POST | `/broker/{n}/completejob` | 0 | 1 | - | 0.2 |  | 204 |
| POST | `/broker/{n}/renewjob` | 0 | 1 | - | 0.2 |  | 200 |
| POST | `/session` | 1 | 0 | 49.2 | - | 201 |  |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 1 | 32.2 | 0.2 | 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 | 32.5 | 0.2 | 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 5 | 5 | 47.2 | 0.2 | 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| POST | `/{n}/acquirejob` | 1 | 0 | 460.8 | - | 200 |  |
| POST | `/{n}/completejob` | 1 | 0 | 30.1 | - | 204 |  |
| POST | `/{n}/renewjob` | 1 | 0 | 44.1 | - | 200 |  |

## Missing endpoints

### official only

- `GET /health`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `GET /ready`
- `POST /_apis/oauth2/token/{guid}`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64`
- `POST /actions/runner-registration`
- `POST /session`
- `POST /{n}/acquirejob`
- `POST /{n}/completejob`
- `POST /{n}/renewjob`

### aksh only

- `GET /_apis/distributedtask/pools/{n}/messages?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false&waitSeconds={n}`
- `POST /_apis/distributedtask/pools/{n}/sessions`
- `POST /_apis/v1/AgentRequest/{n}/{n}?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64`
- `POST /_apis/v1/oauth2/token`
- `POST /api/v3/actions/runner-registration`
- `POST /broker/{n}/acquirejob`
- `POST /broker/{n}/completejob`
- `POST /broker/{n}/renewjob`

## Per-endpoint comparison

### `DELETE /_apis/distributedtask/pools/{n}/agents/{n}`

**Header key differences:**

- official only: `{activityid, cache-control, pragma, strict-transport-security, x-frame-options, x-tfs-processid, x-vss-senderdeploymentid}`
- aksh only: `{content-type}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1 +1 @@
-null
\ No newline at end of file
+{}
\ No newline at end of file
```

**Response body schema diff:**

```diff
--- official
+++ aksh
@@ -1 +1 @@
-"null"
\ No newline at end of file
+{}
\ No newline at end of file
```

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 125.1 / aksh 0.2 | p95: official 125.1 / aksh 0.2

### `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`

**Header key differences:**

- official only: `{activityid, cache-control, pragma, strict-transport-security, x-tfs-processid, x-vss-senderdeploymentid}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,12 +1,11 @@
 {
-  "deploymentId": "2c974d96-2c30-cef5-eff2-3e0511a903a5",
+  "deploymentId": "00000000-0000-0000-0000-000000000000",
   "deploymentType": "hosted",
-  "instanceId": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
+  "instanceId": "2a0247bc-2cb1-4201-ad41-3ea29786bf35",
   "locationServiceData": {
     "clientCacheFresh": true,
     "defaultAccessMappingMoniker": "ScaleUnitMapping",
-    "lastChangeId": 13922305,
-    "lastChangeId64": 13922305,
-    "serviceOwner": "0000005a-0000-8888-8000-000000000000"
+    "lastChangeId": 1,
+    "lastChangeId64": 1
   }
 }
\ No newline at end of file
```

**Response body schema diff:**

```diff
--- official
+++ aksh
@@ -6,7 +6,6 @@
     "clientCacheFresh": "boolean",
     "defaultAccessMappingMoniker": "string",
     "lastChangeId": "number",
-    "lastChangeId64": "number",
-    "serviceOwner": "string"
+    "lastChangeId64": "number"
   }
 }
\ No newline at end of file
```

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 20.9 / aksh 0.2 | p95: official 116.6 / aksh 0.4

### `GET /_apis/distributedtask/pools/{n}/agents?agentName=mitm-official&includeCapabilities=False`

**Header key differences:**

- official only: `{activityid, cache-control, pragma, strict-transport-security, transfer-encoding, x-frame-options, x-tfs-processid, x-vss-senderdeploymentid}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,55 +1,4 @@
 {
-  "count": 1,
-  "value": [
-    {
-      "authorization": {
-        "clientId": "f9d250f4-e9c4-452f-ad52-207d863c5867",
-        "publicKey": {
-          "exponent": "AQAB",
-          "modulus": "3bKIVir/***REDACTED***/***REDACTED***/***REDACTED***/***REDACTED***+***REDACTED***/***REDACTED***+SSwBOHm5NkTrQ=="
-        }
-      },
-      "createdOn": "2026-06-30T15:31:11.42Z",
-      "currentParallelism": 0,
-      "disableUpdate": false,
-      "enabled": true,
-      "ephemeral": false,
-      "id": 3,
-      "isElastic": false,
-      "isVirtual": false,
-      "labels": [
-        {
-          "id": 1,
-          "name": "self-hosted",
-          "type": "system"
-        },
-        {
-          "id": 2,
-          "name": "macOS",
-          "type": "system"
-        },
-        {
-          "id": 3,
-          "name": "ARM64",
-          "type": "system"
-        },
-        {
-          "id": 4,
-          "name": "mitm",
-          "type": "user"
-        }
-      ],
-      "lastConnectedOn": "2026-06-30T15:33:35",
-      "maxParallelism": 1,
-      "name": "mitm-official",
-      "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
-      "owningTenant": null,
-      "provisioningState": "Provisioned",
-      "queueName": "taskagent-3",
-      "runnerGroupId": 1,
-      "runnerGroupName": null,
-      "status": "offline",
-      "version": "2.335.1"
-    }
-  ]
+  "count": 0,
+  "value": []
 }
\ No newline at end of file
```

**Response body schema diff:**

```diff
--- official
+++ aksh
@@ -1,40 +1,4 @@
 {
   "count": "number",
-  "value": [
-    {
-      "authorization": {
-        "clientId": "string",
-        "publicKey": {
-          "exponent": "string",
-          "modulus": "string"
-        }
-      },
-      "createdOn": "string",
-      "currentParallelism": "number",
-      "disableUpdate": "boolean",
-      "enabled": "boolean",
-      "ephemeral": "boolean",
-      "id": "number",
-      "isElastic": "boolean",
-      "isVirtual": "boolean",
-      "labels": [
-        {
-          "id": "number",
-          "name": "string",
-          "type": "string"
-        }
-      ],
-      "lastConnectedOn": "string",
-      "maxParallelism": "number",
-      "name": "string",
-      "osDescription": "string",
-      "owningTenant": "null",
-      "provisioningState": "string",
-      "queueName": "string",
-      "runnerGroupId": "number",
-      "runnerGroupName": "null",
-      "status": "string",
-      "version": "string"
-    }
-  ]
+  "value": []
 }
\ No newline at end of file
```

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 37.1 / aksh 0.3 | p95: official 37.1 / aksh 0.3

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{activityid, cache-control, pragma, strict-transport-security, transfer-encoding, x-frame-options, x-tfs-processid, x-vss-senderdeploymentid}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,29 +1,11 @@
 {
-  "count": 2,
+  "count": 1,
   "value": [
     {
-      "agentCloudId": null,
-      "autoSize": true,
-      "createdOn": "2026-06-30T15:20:20.43Z",
       "id": 1,
       "isHosted": false,
-      "isInternal": true,
       "name": "Default",
-      "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 0,
-      "targetSize": null
-    },
-    {
-      "agentCloudId": 1,
-      "autoSize": true,
-      "createdOn": "2026-06-30T15:20:20.777Z",
-      "id": 2,
-      "isHosted": true,
-      "isInternal": false,
-      "name": "GitHub Actions",
-      "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 1,
-      "targetSize": 1
+      "poolType": 1
     }
   ]
 }
\ No newline at end of file
```

**Response body schema diff:**

```diff
--- official
+++ aksh
@@ -2,28 +2,10 @@
   "count": "number",
   "value": [
     {
-      "agentCloudId": "null",
-      "autoSize": "boolean",
-      "createdOn": "string",
       "id": "number",
       "isHosted": "boolean",
-      "isInternal": "boolean",
       "name": "string",
-      "scope": "string",
-      "size": "number",
-      "targetSize": "null"
-    },
-    {
-      "agentCloudId": "number",
-      "autoSize": "boolean",
-      "createdOn": "string",
-      "id": "number",
-      "isHosted": "boolean",
-      "isInternal": "boolean",
-      "name": "string",
-      "scope": "string",
-      "size": "number",
-      "targetSize": "number"
+      "poolType": "number"
     }
   ]
 }
\ No newline at end of file
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 23.2 / aksh 0.2 | p95: official 23.2 / aksh 0.2

### `POST /_apis/distributedtask/pools/{n}/agents`

**Header key differences:**

- official only: `{activityid, cache-control, pragma, strict-transport-security, x-frame-options, x-tfs-processid, x-vss-senderdeploymentid}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,38 +1,33 @@
 {
   "authorization": {
-    "authorizationUrl": "https://tokenghub.actions.githubusercontent.com/_apis/oauth2/token/5e4d430c-d710-4b62-aed8-555ffd0f7592",
-    "clientId": "0dbfb03e-551b-4123-8bbf-58caee302de6",
+    "authorizationUrl": "http://127.0.0.1:9090/runner/server/_apis/v1/oauth2/token",
+    "clientId": "204be7de-9ab6-4c97-957d-ddd6b9a691c9",
     "publicKey": {
       "exponent": "AQAB",
       "modulus": "8Tlt/***REDACTED***/csCN/***REDACTED***+***REDACTED***/+vsojH/***REDACTED***+myMwcBJJ+***REDACTED***/nmzr987ZiNEMC1TCsdYXoo/***REDACTED***=="
     }
   },
-  "createdOn": "2026-06-30T15:35:02.96Z",
   "currentParallelism": 0,
   "disableUpdate": false,
   "enabled": true,
   "ephemeral": false,
-  "id": 4,
+  "id": 1,
   "isElastic": false,
   "isVirtual": false,
   "labels": [
     {
-      "id": 1,
       "name": "self-hosted",
-      "type": "system"
+      "type": "user"
     },
     {
-      "id": 2,
       "name": "macOS",
-      "type": "system"
+      "type": "user"
     },
     {
-      "id": 3,
       "name": "ARM64",
-      "type": "system"
+      "type": "user"
     },
     {
-      "id": 4,
       "name": "mitm",
       "type": "user"
     }
@@ -40,7 +35,6 @@
   "maxParallelism": 1,
   "name": "mitm-official",
   "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
-  "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
       "$type": "System.Boolean",
@@ -48,11 +42,11 @@
     },
     "ServerUrl": {
       "$type": "System.String",
-      "$value": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/"
+      "$value": "http://127.0.0.1:9090/runner/server"
     },
     "ServerUrlV2": {
       "$type": "System.String",
-      "$value": "https://broker.actions.githubusercontent.com/"
+      "$value": "http://127.0.0.1:9090/runner/server"
     },
     "UseV2Flow": {
       "$type": "System.Boolean",
@@ -60,7 +54,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-4",
+  "queueName": "taskagent-1",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Response body schema diff:**

```diff
--- official
+++ aksh
@@ -7,7 +7,6 @@
       "modulus": "string"
     }
   },
-  "createdOn": "string",
   "currentParallelism": "number",
   "disableUpdate": "boolean",
   "enabled": "boolean",
@@ -17,7 +16,6 @@
   "isVirtual": "boolean",
   "labels": [
     {
-      "id": "number",
       "name": "string",
       "type": "string"
     }
@@ -25,7 +23,6 @@
   "maxParallelism": "number",
   "name": "string",
   "osDescription": "string",
-  "owningTenant": "null",
   "properties": {
     "RequireFipsCryptography": {
       "$type": "string",
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 154.7 / aksh 0.3 | p95: official 154.7 / aksh 0.3

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{x-github-backend, x-github-request-id}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 32.2 / aksh 0.2 | p95: official 32.2 / aksh 0.2

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{x-github-backend, x-github-request-id}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa2.blob.core.windows.net/actions-results/70d3e060-f9c5-4bf4-a5c7-46e890539934/workflow-job-run-282ec8be-25da-59b5-8c3d-967df82fa336/logs/job/job-logs.txt?se=2026-06-30T16%3A35%3A41Z&sig=8WP%2FH6GMb%***REDACTED***%3D&ske=2026-06-30T18%3A09%3A26Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T14%3A09%3A26Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A35%3A36Z&sv=2025-11-05"
+  "logs_url": "http://127.0.0.1:9090/replay/results/70d3e060-f9c5-4bf4-a5c7-46e890539934/282ec8be-25da-59b5-8c3d-967df82fa336/job-logs.txt"
 }
\ No newline at end of file
```

**Response body schema diff:**

_identical_

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 32.5 / aksh 0.2 | p95: official 32.5 / aksh 0.2

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{x-github-backend, x-github-request-id}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa2.blob.core.windows.net/actions-results/70d3e060-f9c5-4bf4-a5c7-46e890539934/workflow-job-run-282ec8be-25da-59b5-8c3d-967df82fa336/logs/steps/step-logs-0d20c64f-d203-4ecd-88fc-95bcad1ac884.txt?se=2026-06-30T16%3A35%3A10Z&sig=a1Efv0Zb%2BmVNW%***REDACTED***%3D&ske=2026-06-30T19%3A10%3A45Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A10%3A45Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A35%3A05Z&sv=2025-11-05",
+  "logs_url": "http://127.0.0.1:9090/replay/results/70d3e060-f9c5-4bf4-a5c7-46e890539934/282ec8be-25da-59b5-8c3d-967df82fa336/step-0d20c64f-d203-4ecd-88fc-95bcad1ac884.txt",
   "soft_size_limit": "1048576"
 }
\ No newline at end of file
```

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200]

**Timing (ms):** p50: official 35.3 / aksh 0.2 | p95: official 88.3 / aksh 0.2
