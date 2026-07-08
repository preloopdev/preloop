# MITM comparison: 11-cache-roundtrip

**official**: ok — 40 flows
**aksh**: N/A — 52 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/session` | 0 | 1 | - | 51.6 |  | 204 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1` | 0 | 1 | - | 18.3 |  | 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 6 | 5 | 51.5 | 34.4 | 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-11-cache-roundtrip-1783541710&includeCapabilities=False` | 0 | 1 | - | 30.2 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official-11-cache-roundtrip-2026-06-30T15-56-46Z&includeCapabilities=False` | 1 | 0 | 24.0 | - | 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 417.6 | 26.8 | 200 | 200 |
| GET | `/_ws/ingest.sock` | 0 | 1 | - | 41.9 |  | 401 |
| GET | `/actions/cache/tar.gz/***REDACTED***` | 1 | 1 | 200.2 | 256.6 | 200 | 200 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 | - | 2746.4 |  | 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 | - | 3136.8 |  | 200 |
| GET | `/health` | 2 | 2 | 36.6 | 34.3 | 200, 200 | 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 2 | - | 0 |  | None, None |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 1 | 0 | 0 | - | None |  |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 6515.0 |  | 200 |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 2 | 0 | 1647.6 | - | 200, None |  |
| GET | `/ready` | 1 | 1 | 16.3 | 44.8 | 204 | 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 160.2 | 78.3 | 200 | 200 |
| POST | `/_apis/oauth2/token` | 0 | 2 | - | 83.0 |  | 200, 200 |
| POST | `/_apis/oauth2/token/{guid}` | 2 | 0 | 70.9 | - | 200, 200 |  |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 0 | 1 | - | 62.1 |  | 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 1 | 0 | 63.7 | - | 200 |  |
| POST | `/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 1 | 1 | 114.5 | 105.2 | 200 | 200 |
| POST | `/actions/runner-registration` | 1 | 1 | 209.6 | 195.8 | 200 | 200 |
| POST | `/session` | 1 | 1 | 124.8 | 60.0 | 201 | 201 |
| POST | `/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry` | 1 | 0 | 59.4 | - | 200 |  |
| POST | `/twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload` | 1 | 0 | 83.9 | - | 200 |  |
| POST | `/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL` | 1 | 0 | 42.3 | - | 200 |  |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 4 | 1 | 146.0 | 48.8 | 200, 200, 200, 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 0 | 1 | - | 63.0 |  | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 0 | 6 | - | 121.0 |  | 200, 200, 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 | 31.1 | 43.7 | 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 6 | 6 | 34.8 | 44.2 | 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200 |
| POST | `/{n}/acquirejob` | 1 | 1 | 456.9 | 343.7 | 200 | 200 |
| POST | `/{n}/completejob` | 1 | 1 | 139.4 | 38.4 | 204 | 204 |
| POST | `/{n}/renewjob` | 1 | 2 | 36.1 | 165.8 | 200 | 200, 200 |
| PUT | `/actions-cache/d7d-5385177621?se=2026-06-30T16%3A56%3A56Z&sig=***REDACTED***%3D&ske=2026-06-30T19%3A40%3A39Z&skoid={guid}&sks=b&skt=2026-06-30T15%3A40%3A39Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A56%3A51Z&sv=2025-11-05` | 1 | 0 | 23.9 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A16%3A34Z&sig=yQ%***REDACTED***%2FnU%3D&ske=2026-07-09T00%3A09%3A40Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A40Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A16%3A29Z&sv=2025-11-05` | 0 | 1 | - | 24.8 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-__post_{guid}.txt?se=2026-07-08T21%3A16%3A33Z&sig=qvvwrP%2FcoJn2u7%2FXxI65uzWZXRt%2B2691nglOqEgtKH0%3D&ske=2026-07-09T00%3A10%3A56Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A16%3A28Z&sv=2025-11-05` | 0 | 1 | - | 33.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A27Z&sig=74W%2FQJOlQVWM3WHA3Pj%2Fhj6hRGnZgnHeqPCCiAeCg88%3D&ske=2026-07-09T00%3A09%3A40Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A40Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A22Z&sv=2025-11-05` | 0 | 1 | - | 83.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A27Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A32Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A32Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A22Z&sv=2025-11-05` | 0 | 1 | - | 107.5 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A16%3A00Z&sig=HwV6Oo8MelkkcjEFSxXl%2B4cfTsLwcPvL0%2F%2BOVCqqKbU%3D&ske=2026-07-09T00%3A10%3A56Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A55Z&sv=2025-11-05` | 0 | 1 | - | 28.5 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A16%3A00Z&sig=fUe%***REDACTED***%3D&ske=2026-07-09T00%3A09%3A58Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A58Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A55Z&sv=2025-11-05` | 0 | 1 | - | 91.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A16%3A33Z&sig=17KZeJGN9JnvJMYnFTGeu01D5w6P%2FHIpUTjdQoAwPf4%3D&ske=2026-07-09T00%3A09%3A40Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A40Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A16%3A28Z&sv=2025-11-05` | 0 | 1 | - | 75.7 |  | 201 |

## Missing endpoints

### official only

- `GET /_apis/distributedtask/pools/{n}/agents?agentName=mitm-official-11-cache-roundtrip-2026-06-30T15-56-46Z&includeCapabilities=False`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token/{guid}`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64`
- `POST /twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry`
- `POST /twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload`
- `POST /twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL`
- `PUT /actions-cache/d7d-5385177621?se=2026-06-30T16%3A56%3A56Z&sig=***REDACTED***%3D&ske=2026-06-30T19%3A40%3A39Z&skoid={guid}&sks=b&skt=2026-06-30T15%3A40%3A39Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A56%3A51Z&sv=2025-11-05`

### aksh only

- `DELETE /session`
- `GET /_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-11-cache-roundtrip-1783541710&includeCapabilities=False`
- `GET /_ws/ingest.sock`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`
- `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`
- `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A16%3A34Z&sig=yQ%***REDACTED***%2FnU%3D&ske=2026-07-09T00%3A09%3A40Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A40Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A16%3A29Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-__post_{guid}.txt?se=2026-07-08T21%3A16%3A33Z&sig=qvvwrP%2FcoJn2u7%2FXxI65uzWZXRt%2B2691nglOqEgtKH0%3D&ske=2026-07-09T00%3A10%3A56Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A16%3A28Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A27Z&sig=74W%2FQJOlQVWM3WHA3Pj%2Fhj6hRGnZgnHeqPCCiAeCg88%3D&ske=2026-07-09T00%3A09%3A40Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A40Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A22Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A27Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A32Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A32Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A22Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A16%3A00Z&sig=HwV6Oo8MelkkcjEFSxXl%2B4cfTsLwcPvL0%2F%2BOVCqqKbU%3D&ske=2026-07-09T00%3A10%3A56Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A55Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A16%3A00Z&sig=fUe%***REDACTED***%3D&ske=2026-07-09T00%3A09%3A58Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A58Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A55Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A16%3A33Z&sig=17KZeJGN9JnvJMYnFTGeu01D5w6P%2FHIpUTjdQoAwPf4%3D&ske=2026-07-09T00%3A09%3A40Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A40Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A16%3A28Z&sv=2025-11-05`

## Per-endpoint comparison

### `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`

**Header key differences:**

- official only: `{'authorization', 'accept-language', 'accept-encoding', 'x-tfs-fedauthredirect'}`

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

**Timing (ms):** p50: official 33.7 / aksh 19.3 | p95: official 109.3 / aksh 94.6

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
-      "size": 1,
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

**Timing (ms):** p50: official 417.6 / aksh 26.8 | p95: official 417.6 / aksh 26.8

### `GET /actions/cache/tar.gz/***REDACTED***`

**Header key differences:**

- official only: `{'authorization'}`
- aksh only: `{'accept'}`

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 200.2 / aksh 256.6 | p95: official 200.2 / aksh 256.6

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 40.1 / aksh 42.1 | p95: official 40.1 / aksh 42.1

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 16.3 / aksh 44.8 | p95: official 16.3 / aksh 44.8

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
-      "modulus": "qXX6qQyvlK1eCVx71XHiM+ido6iDDV81Al8OUWULEZ6mZKO7KXx+***REDACTED***/4a2Vc6FSGNs1B5+dSy1B9ZwTQPgUxaKRHrkG/***REDACTED***/GAUHEvKUDq/***REDACTED***+Fjra/bfQWLqtmE+***REDACTED***/viOx1BXjJwwgLqQAAi9xoCvAw=="
+      "modulus": "qmxNgBfQp+y6qQuIehy+8aGMvriz+LuAiIz++***REDACTED***/***REDACTED***/***REDACTED***+ru3yGFvy0D0zJgOWk+6hsJl7m0ffX4+cTEN7I+***REDACTED***+zYpvuv9WKwsqYi+***REDACTED***/+BPoFeuZ+0URbQSNk/ekjgpYmVDkxZEAFQ1dpii3YUToWDw=="
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
-  "name": "mitm-official-11-cache-roundtrip-2026-06-30T15-56-46Z",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-11-cache-roundtrip-1783541710",
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
-    "clientId": "4276799d-a97f-4533-be6e-b3116cb1d62f",
+    "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/_apis/oauth2/token",
+    "clientId": "279f1d9a-06f7-462c-b01b-1c8a69c12932",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "qXX6qQyvlK1eCVx71XHiM+ido6iDDV81Al8OUWULEZ6mZKO7KXx+***REDACTED***/4a2Vc6FSGNs1B5+dSy1B9ZwTQPgUxaKRHrkG/***REDACTED***/GAUHEvKUDq/***REDACTED***+Fjra/bfQWLqtmE+***REDACTED***/viOx1BXjJwwgLqQAAi9xoCvAw=="
+      "modulus": "qmxNgBfQp+y6qQuIehy+8aGMvriz+LuAiIz++***REDACTED***/***REDACTED***/***REDACTED***+ru3yGFvy0D0zJgOWk+6hsJl7m0ffX4+cTEN7I+***REDACTED***+zYpvuv9WKwsqYi+***REDACTED***/+BPoFeuZ+0URbQSNk/ekjgpYmVDkxZEAFQ1dpii3YUToWDw=="
     }
   },
-  "createdOn": "2026-06-30T15:56:48.903Z",
+  "createdOn": "2026-07-08T20:15:11.513Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
-  "ephemeral": false,
-  "id": 8,
+  "ephemeral": true,
+  "id": 685,
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
-  "name": "mitm-official-11-cache-roundtrip-2026-06-30T15-56-46Z",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-11-cache-roundtrip-1783541710",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -60,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-8",
+  "queueName": "taskagent-685",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 160.2 / aksh 78.3 | p95: official 160.2 / aksh 78.3

### `POST /actions/build/{guid}/jobs/{guid}/runnerresolve/actions`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

_identical_

**Response body diff:**

_identical_

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 114.5 / aksh 105.2 | p95: official 114.5 / aksh 105.2

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

**Timing (ms):** p50: official 209.6 / aksh 195.8 | p95: official 209.6 / aksh 195.8

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
-    "id": 8,
-    "name": "mitm-official-11-cache-roundtrip-2026-06-30T15-56-46Z",
-    "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+    "id": 685,
+    "name": "aksh-capture-11-cache-roundtrip-1783541710",
+    "osDescription": "linux aarch64",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "Nuraydias-Mac-Studio (PID: 43362)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 751)",
+  "sessionId": "0cdc53a8-ec4f-4f55-ab53-c0258d3307d8",
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
-  "ownerName": "Nuraydias-Mac-Studio (PID: 43362)",
-  "sessionId": "3e20321c-6a6d-438e-aa4f-e3f9d8a61233"
+  "ownerName": "container (PID: 751)",
+  "sessionId": "96e9b091-495c-4486-8106-02bab10c5db6"
 }
```

**Status codes:** official: [201] | aksh: [201]

**Timing (ms):** p50: official 124.8 / aksh 60.0 | p95: official 124.8 / aksh 60.0

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,15 +2,60 @@
   "change_order": 1,
   "steps": [
     {
-      "completed_at": null,
-      "conclusion": 0,
-      "external_id": "51200acc-bdba-460d-8f4a-85b884afe685",
+      "completed_at": "2026-07-08T20:15:27.021Z",
+      "conclusion": 2,
+      "external_id": "e693eee4-bcac-48b7-957d-ad5feb6d1bb4",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T15:56:54.349Z",
-      "status": 3
+      "started_at": "2026-07-08T20:15:27.021Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-08T20:15:27.458Z",
+      "conclusion": 2,
+      "external_id": "d1d7592b-09be-44ad-a254-6053d70a2222",
+      "name": "Run mkdir -p .cache-dir && date > .cache-dir/stamp",
+      "number": 2,
+      "started_at": "2026-07-08T20:15:27.454Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-08T20:16:00.555Z",
+      "conclusion": 2,
+      "external_id": "e9cfe8b1-2caf-477d-93e8-d56a9241be33",
+      "name": "actions/cache@v4",
+      "number": 3,
+      "started_at": "2026-07-08T20:15:27.671Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-08T20:16:00.937Z",
+      "conclusion": 2,
+      "external_id": "f63fcd50-b775-4305-bd13-5c0af7f48ad7",
+      "name": "Run cat .cache-dir/stamp",
+      "number": 4,
+      "started_at": "2026-07-08T20:16:00.929Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-08T20:16:33.563Z",
+      "conclusion": 2,
+      "external_id": "__post_e9cfe8b1-2caf-477d-93e8-d56a9241be33",
+      "name": "Post actions/cache@v4",
+      "number": 5,
+      "started_at": "2026-07-08T20:16:01.142Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-08T20:16:33.723Z",
+      "conclusion": 2,
+      "external_id": "65df042f-a301-42fe-bad1-440a343b1b79",
+      "name": "Complete job",
+      "number": 6,
+      "started_at": "2026-07-08T20:16:33.723Z",
+      "status": 6
     }
   ],
-  "workflow_job_run_backend_id": "a6d4fc3d-278e-53c5-961b-2aa0d956a8d9",
-  "workflow_run_backend_id": "2399dfb1-ff49-4d32-ba36-b782a6433856"
+  "workflow_job_run_backend_id": "82f3c139-d117-5712-bfeb-3d31c02601b8",
+  "workflow_run_backend_id": "b20b2b4b-ca9f-43aa-93ce-8cdb7ab09b28"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200] | aksh: [200]

**Timing (ms):** p50: official 131.4 / aksh 48.8 | p95: official 362.2 / aksh 48.8

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "a6d4fc3d-278e-53c5-961b-2aa0d956a8d9",
-  "workflow_run_backend_id": "2399dfb1-ff49-4d32-ba36-b782a6433856"
+  "workflow_job_run_backend_id": "82f3c139-d117-5712-bfeb-3d31c02601b8",
+  "workflow_run_backend_id": "b20b2b4b-ca9f-43aa-93ce-8cdb7ab09b28"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa19.blob.core.windows.net/actions-results/2399dfb1-ff49-4d32-ba36-b782a6433856/workflow-job-run-a6d4fc3d-278e-53c5-961b-2aa0d956a8d9/logs/job/job-logs.txt?se=2026-06-30T16%3A57%3A33Z&sig=***REDACTED***%2FVXTlMAOh8nx8%3D&ske=2026-06-30T19%3A40%3A55Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A40%3A55Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A57%3A28Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa4.blob.core.windows.net/actions-results/b20b2b4b-ca9f-43aa-93ce-8cdb7ab09b28/workflow-job-run-82f3c139-d117-5712-bfeb-3d31c02601b8/logs/job/job-logs.txt?se=2026-07-08T21%3A16%3A34Z&sig=yQ%***REDACTED***%2FnU%3D&ske=2026-07-09T00%3A09%3A40Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A09%3A40Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A16%3A29Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 31.1 / aksh 43.7 | p95: official 31.1 / aksh 43.7

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "51200acc-bdba-460d-8f4a-85b884afe685",
-  "workflow_job_run_backend_id": "a6d4fc3d-278e-53c5-961b-2aa0d956a8d9",
-  "workflow_run_backend_id": "2399dfb1-ff49-4d32-ba36-b782a6433856"
+  "step_backend_id": "e693eee4-bcac-48b7-957d-ad5feb6d1bb4",
+  "workflow_job_run_backend_id": "82f3c139-d117-5712-bfeb-3d31c02601b8",
+  "workflow_run_backend_id": "b20b2b4b-ca9f-43aa-93ce-8cdb7ab09b28"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa19.blob.core.windows.net/actions-results/2399dfb1-ff49-4d32-ba36-b782a6433856/workflow-job-run-a6d4fc3d-278e-53c5-961b-2aa0d956a8d9/logs/steps/step-logs-51200acc-bdba-460d-8f4a-85b884afe685.txt?se=2026-06-30T16%3A56%3A55Z&sig=***REDACTED***%2BU9PDv7E%3D&ske=2026-06-30T19%3A39%3A59Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A39%3A59Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A56%3A50Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa4.blob.core.windows.net/actions-results/b20b2b4b-ca9f-43aa-93ce-8cdb7ab09b28/workflow-job-run-82f3c139-d117-5712-bfeb-3d31c02601b8/logs/steps/step-logs-e693eee4-bcac-48b7-957d-ad5feb6d1bb4.txt?se=2026-07-08T21%3A15%3A27Z&sig=***REDACTED***%3D&ske=2026-07-09T00%3A10%3A32Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A10%3A32Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A22Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 27.4 / aksh 49.2 | p95: official 75.8 / aksh 52.4

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
-  "jobMessageId": "a6d4fc3d-278e-53c5-961b-2aa0d956a8d9",
-  "runnerOS": "macOS"
+  "jobMessageId": "82f3c139-d117-5712-bfeb-3d31c02601b8",
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
-          "v": "28457865400"
+          "v": "28972661091"
         },
         {
           "k": "run_number",
-          "v": "2"
+          "v": "13"
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
-          "v": 84337516346
+          "v": 85971982972
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
-  "jobId": "a6d4fc3d-278e-53c5-961b-2aa0d956a8d9",
+  "jobId": "82f3c139-d117-5712-bfeb-3d31c02601b8",
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
-      "value": "***REDACTED***\\.VdwlwrSpA7Ex-P_FRdAWydPKatkUhbnZvQZhRqYcT"
-    },
-    {
-      "type": "regex",
-      "value": "***REDACTED***"
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
+      "value": "***REDACTED***\\.auIkq81y2c9QIWu"
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
-    "planId": "2399dfb1-ff49-4d32-ba36-b782a6433856",
+    "planId": "b20b2b4b-ca9f-43aa-93ce-8cdb7ab09b28",
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
-        "url": "https://run-actions-2-azure-eastus.actions.githubusercontent.com/112/"
+        "url": "https://run-actions-3-azure-eastus.actions.githubusercontent.com/91/"
       }
     ]
   },
@@ -915,7 +911,7 @@
       "condition": "success()",
       "contextName": "__run",
       "continueOnError": null,
-      "id": "4c1cf56b-d370-4673-8c0f-6d69b1be1191",
+      "id": "d1d7592b-09be-44ad-a254-6053d70a2222",
       "inputs": {
         "map": [
           {
@@ -945,7 +941,7 @@
       "condition": "success()",
       "contextName": "__actions_cache",
       "continueOnError": null,
-      "id": "ac28222d-bc9a-43c0-b339-d23f8da11684",
+      "id": "e9cfe8b1-2caf-477d-93e8-d56a9241be33",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1000,7 +996,7 @@
       "condition": "success()",
       "contextName": "__run_2",
       "continueOnError": null,
-      "id": "7176254a-7a30-4117-ac35-51c41cdd9813",
+      "id": "f63fcd50-b775-4305-bd13-5c0af7f48ad7",
       "inputs": {
         "map": [
           {
@@ -1029,7 +1025,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "2399dfb1-ff49-4d32-ba36-b782a6433856",
+    "id": "b20b2b4b-ca9f-43aa-93ce-8cdb7ab09b28",
     "location": null
   },
   "variables": {
@@ -1149,7 +1145,7 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
@@ -1168,13 +1164,13 @@
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
-      "value": "2399dfb1-ff49-4d32-ba36-b782a6433856.build.__default"
+      "value": "b20b2b4b-ca9f-43aa-93ce-8cdb7ab09b28.build.__default"
     },
     "system.phaseDisplayName": {
       "value": "build"
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 456.9 / aksh 343.7 | p95: official 456.9 / aksh 343.7

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,109 +2,103 @@
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
   "conclusion": "succeeded",
-  "jobId": "a6d4fc3d-278e-53c5-961b-2aa0d956a8d9",
+  "jobId": "82f3c139-d117-5712-bfeb-3d31c02601b8",
   "outputs": {},
-  "planId": "2399dfb1-ff49-4d32-ba36-b782a6433856",
+  "planId": "b20b2b4b-ca9f-43aa-93ce-8cdb7ab09b28",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-06-30T15:56:55.060474Z",
+      "completed_at": "2026-07-08T20:16:34.197Z",
       "conclusion": "succeeded",
-      "external_id": "51200acc-bdba-460d-8f4a-85b884afe685",
+      "external_id": "e693eee4-bcac-48b7-957d-ad5feb6d1bb4",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T15:56:54.349511Z",
+      "started_at": "2026-07-08T20:16:34.197Z",
       "status": "completed",
       "type": "runner"
     },
     {
       "action_name": "sh",
       "annotations": [],
-      "completed_at": "2026-06-30T15:56:55.098842Z",
+      "completed_at": "2026-07-08T20:16:34.197Z",
       "conclusion": "succeeded",
-      "external_id": "4c1cf56b-d370-4673-8c0f-6d69b1be1191",
+      "external_id": "d1d7592b-09be-44ad-a254-6053d70a2222",
       "name": "Run mkdir -p .cache-dir && date > .cache-dir/stamp",
       "number": 2,
-      "started_at": "2026-06-30T15:56:55.065332Z",
+      "started_at": "2026-07-08T20:16:34.197Z",
       "status": "completed",
       "type": "run"
     },
     {
-      "action_name": "actions/cache",
-      "annotations": [],
-      "completed_at": "2026-06-30T15:56:56.393998Z",
+      "action_name": "actions/cache@v4",
+      "annotations": [
+        {
+          "endLine": 1,
+          "level": "warning",
+          "message": "Input 'save-always' has been deprecated with message: save-always does not work as intended and will be removed in a future release.\nA separate `actions/cache/restore` step should be used instead.\nSee https://github.com/actions/cache/tree/main/save#always-save-cache for more details.",
+          "startLine": 1,
+          "stepNumber": 3
+        }
+      ],
+      "completed_at": "2026-07-08T20:16:34.197Z",
       "conclusion": "succeeded",
-      "external_id": "ac28222d-bc9a-43c0-b339-d23f8da11684",
-      "name": "Run actions/cache@v4",
+      "external_id": "e9cfe8b1-2caf-477d-93e8-d56a9241be33",
+      "name": "actions/cache@v4",
       "number": 3,
-      "ref": "v4",
-      "started_at": "2026-06-30T15:56:55.099464Z",
+      "started_at": "2026-07-08T20:16:34.197Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
       "action_name": "sh",
       "annotations": [],
-      "completed_at": "2026-06-30T15:56:56.425334Z",
+      "completed_at": "2026-07-08T20:16:34.197Z",
       "conclusion": "succeeded",
-      "external_id": "7176254a-7a30-4117-ac35-51c41cdd9813",
+      "external_id": "f63fcd50-b775-4305-bd13-5c0af7f48ad7",
       "name": "Run cat .cache-dir/stamp",
       "number": 4,
-      "started_at": "2026-06-30T15:56:56.395994Z",
+      "started_at": "2026-07-08T20:16:34.197Z",
       "status": "completed",
       "type": "run"
     },
     {
-      "action_name": "actions/cache",
-      "annotations": [],
-      "completed_at": "2026-06-30T15:56:57.364559Z",
+      "action_name": "actions/cache@v4",
+      "annotations": [
+        {
+          "endLine": 1,
+          "level": "warning",
+          "message": "Input 'save-always' has been deprecated with message: save-always does not work as intended and will be removed in a future release.\nA separate `actions/cache/restore` step should be used instead.\nSee https://github.com/actions/cache/tree/main/save#always-save-cache for more details.",
+          "startLine": 1,
+          "stepNumber": 5
+        }
+      ],
+      "completed_at": "2026-07-08T20:16:34.197Z",
       "conclusion": "succeeded",
-      "external_id": "331120e6-b2b1-435d-8125-8d2a4d23eda2",
-      "name": "Post Run actions/cache@v4",
-      "number": 8,
-      "ref": "v4",
-      "started_at": "2026-06-30T15:56:56.427994Z",
+      "external_id": "__post_e9cfe8b1-2caf-477d-93e8-d56a9241be33",
+      "name": "Post actions/cache@v4",
+      "number": 5,
+      "started_at": "2026-07-08T20:16:34.197Z",
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
-          "message": "Node.js 20 is deprecated. The following actions target Node.js 20 but are being forced to run on Node.js 24: actions/cache@v4. For more information see: https://github.blog/changelog/2025-09-19-deprecation-of-node-20-on-github-actions-runners/",
-          "startLine": 2,
-          "stepNumber": 9
-        }
-      ],
-      "completed_at": "2026-06-30T15:56:57.380572Z",
+      "annotations": [],
+      "completed_at": "2026-07-08T20:16:34.197Z",
       "conclusion": "succeeded",
-      "external_id": "3bba0178-4b9c-41d6-8c8c-b24e05c120d7",
+      "external_id": "65df042f-a301-42fe-bad1-440a343b1b79",
       "name": "Complete job",
-      "number": 9,
-      "started_at": "2026-06-30T15:56:57.370544Z",
+      "number": 6,
+      "started_at": "2026-07-08T20:16:34.197Z",
       "status": "completed",
       "type": "runner"
     }
   ],
   "telemetry": [
     {
-      "message": "Action archive cache usage: actions/cache@***REDACTED*** use cache False has cache False",
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
+      "message": "{\"ClassType\":\"StepsRunner\",\"FinishResult\":\"succeeded\"}",
+      "type": "task"
     }
   ]
 }
```

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 139.4 / aksh 38.4 | p95: official 139.4 / aksh 38.4

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "a6d4fc3d-278e-53c5-961b-2aa0d956a8d9",
-  "planId": "2399dfb1-ff49-4d32-ba36-b782a6433856"
+  "jobId": "82f3c139-d117-5712-bfeb-3d31c02601b8",
+  "planId": "b20b2b4b-ca9f-43aa-93ce-8cdb7ab09b28"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-06-30T16:06:54.12846133Z"
+  "lockedUntil": "2026-07-08T20:25:27.032558544Z"
 }
```

**Status codes:** official: [200] | aksh: [200, 200]

**Timing (ms):** p50: official 36.1 / aksh 275.6 | p95: official 36.1 / aksh 275.6
