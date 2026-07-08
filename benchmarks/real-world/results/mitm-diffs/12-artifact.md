# MITM comparison: 12-artifact

**official**: ok — 45 flows
**aksh**: N/A — 46 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/session` | 0 | 1 | - | 49.4 |  | 204 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1` | 0 | 1 | - | 19.2 |  | 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 6 | 5 | 22.5 | 45.4 | 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-12-artifact-1783541710&includeCapabilities=False` | 0 | 1 | - | 23.4 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official-12-artifact-2026-06-30T15-58-25Z&includeCapabilities=False` | 1 | 0 | 27.0 | - | 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 120.6 | 23.7 | 200 | 200 |
| GET | `/_ws/ingest.sock` | 0 | 1 | - | 50.4 |  | 401 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22out.zip%22&rsct=application%2Fzip&se=2026-06-30T16%3A08%3A40Z&sig=***REDACTED***%3D&ske=2026-06-30T19%3A51%3A19Z&skoid={guid}&sks=b&skt=2026-06-30T15%3A51%3A19Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-06-30T15%3A58%3A35Z&sv=2025-11-05` | 1 | 0 | 19.8 | - | 200 |  |
| GET | `/actions/download-artifact/tar.gz/***REDACTED***` | 1 | 1 | 212.3 | 1260.6 | 200 | 200 |
| GET | `/actions/upload-artifact/tar.gz/***REDACTED***` | 1 | 1 | 202.9 | 263.0 | 200 | 200 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 | - | 2812.7 |  | 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 | - | 3965.4 |  | 200 |
| GET | `/health` | 2 | 2 | 24.9 | 25.3 | 200, 200 | 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 0 |  | None |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 1 | 0 | 0 | - | None |  |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 0 | 1 | - | 3432.4 |  | 200 |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false` | 2 | 0 | 1543.4 | - | 200, None |  |
| GET | `/ready` | 1 | 1 | 15.7 | 21.7 | 204 | 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 152.1 | 78.2 | 200 | 200 |
| POST | `/_apis/oauth2/token` | 0 | 2 | - | 104.1 |  | 200, 200 |
| POST | `/_apis/oauth2/token/{guid}` | 2 | 0 | 25.9 | - | 200, 200 |  |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 0 | 1 | - | 135.4 |  | 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 1 | 0 | 49.5 | - | 200 |  |
| POST | `/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 1 | 1 | 97.1 | 104.0 | 200 | 200 |
| POST | `/actions/runner-registration` | 1 | 1 | 174.1 | 228.1 | 200 | 200 |
| POST | `/session` | 1 | 1 | 57.9 | 38.5 | 201 | 201 |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact` | 1 | 0 | 154.3 | - | 200 |  |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact` | 1 | 0 | 229.6 | - | 200 |  |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL` | 1 | 0 | 115.3 | - | 200 |  |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts` | 2 | 0 | 71.0 | - | 200, 200 |  |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 4 | 2 | 57.5 | 119.1 | 200, 200, 200, 200 | 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 0 | 1 | - | 70.6 |  | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 0 | 4 | - | 208.9 |  | 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 | 30.8 | 64.3 | 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 6 | 4 | 43.3 | 44.4 | 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/{n}/acquirejob` | 1 | 1 | 466.8 | 399.5 | 200 | 200 |
| POST | `/{n}/completejob` | 1 | 1 | 40.9 | 54.7 | 204 | 204 |
| POST | `/{n}/renewjob` | 1 | 1 | 100.6 | 42.3 | 200 | 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-06-30T16%3A58%3A39Z&sig=8bbCbAWNjTMi6C%2BpgRonKTVxvnhFlwgoqyCyzgdCV%2FA%3D&ske=2026-06-30T19%3A51%3A53Z&skoid={guid}&sks=b&skt=2026-06-30T15%3A51%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A58%3A34Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 1 | 0 | 22.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-06-30T16%3A58%3A39Z&sig=8bbCbAWNjTMi6C%2BpgRonKTVxvnhFlwgoqyCyzgdCV%2FA%3D&ske=2026-06-30T19%3A51%3A53Z&skoid={guid}&sks=b&skt=2026-06-30T15%3A51%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A58%3A34Z&sv=2025-11-05&comp=blocklist` | 1 | 0 | 23.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A15%3A55Z&sig=v5G%***REDACTED***%2BCiw%3D&ske=2026-07-09T00%3A09%3A57Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A57Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A50Z&sv=2025-11-05` | 0 | 1 | - | 27.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A27Z&sig=OV1UZr0qx1LssmH1I6PLH8WE3l0%2BaaqeiNKjNDRTx%2FY%3D&ske=2026-07-09T00%3A09%3A37Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A37Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A22Z&sv=2025-11-05` | 0 | 1 | - | 83.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A27Z&sig=***REDACTED***%2Fk7UE%3D&ske=2026-07-09T00%3A09%3A53Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A22Z&sv=2025-11-05` | 0 | 1 | - | 42.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A54Z&sig=giLAoRx1IOnPJleoDn%2FGoEJsDUXyIa6dRWW0Sqrx3JI%3D&ske=2026-07-09T00%3A11%3A04Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A11%3A04Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A49Z&sv=2025-11-05` | 0 | 1 | - | 49.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A54Z&sig=sfT%2B29UUyI%2F0ebjaz4%2BshUz7dbNxPkddd%2FRGXt7waHM%3D&ske=2026-07-09T00%3A10%3A12Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A12Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A49Z&sv=2025-11-05` | 0 | 1 | - | 94.8 |  | 201 |

## Missing endpoints

### official only

- `GET /_apis/distributedtask/pools/{n}/agents?agentName=mitm-official-12-artifact-2026-06-30T15-58-25Z&includeCapabilities=False`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22out.zip%22&rsct=application%2Fzip&se=2026-06-30T16%3A08%3A40Z&sig=***REDACTED***%3D&ske=2026-06-30T19%3A51%3A19Z&skoid={guid}&sks=b&skt=2026-06-30T15%3A51%3A19Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-06-30T15%3A58%3A35Z&sv=2025-11-05`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token/{guid}`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64`
- `POST /twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact`
- `POST /twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact`
- `POST /twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL`
- `POST /twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-06-30T16%3A58%3A39Z&sig=8bbCbAWNjTMi6C%2BpgRonKTVxvnhFlwgoqyCyzgdCV%2FA%3D&ske=2026-06-30T19%3A51%3A53Z&skoid={guid}&sks=b&skt=2026-06-30T15%3A51%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A58%3A34Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-06-30T16%3A58%3A39Z&sig=8bbCbAWNjTMi6C%2BpgRonKTVxvnhFlwgoqyCyzgdCV%2FA%3D&ske=2026-06-30T19%3A51%3A53Z&skoid={guid}&sks=b&skt=2026-06-30T15%3A51%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A58%3A34Z&sv=2025-11-05&comp=blocklist`

### aksh only

- `DELETE /session`
- `GET /_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=aksh-capture-12-artifact-1783541710&includeCapabilities=False`
- `GET /_ws/ingest.sock`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`
- `POST /_apis/oauth2/token`
- `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`
- `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`
- `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-08T21%3A15%3A55Z&sig=v5G%***REDACTED***%2BCiw%3D&ske=2026-07-09T00%3A09%3A57Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A57Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A50Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A27Z&sig=OV1UZr0qx1LssmH1I6PLH8WE3l0%2BaaqeiNKjNDRTx%2FY%3D&ske=2026-07-09T00%3A09%3A37Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A37Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A22Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A27Z&sig=***REDACTED***%2Fk7UE%3D&ske=2026-07-09T00%3A09%3A53Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A09%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A22Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A54Z&sig=giLAoRx1IOnPJleoDn%2FGoEJsDUXyIa6dRWW0Sqrx3JI%3D&ske=2026-07-09T00%3A11%3A04Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A11%3A04Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A49Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-08T21%3A15%3A54Z&sig=sfT%2B29UUyI%2F0ebjaz4%2BshUz7dbNxPkddd%2FRGXt7waHM%3D&ske=2026-07-09T00%3A10%3A12Z&skoid={guid}&sks=b&skt=2026-07-08T20%3A10%3A12Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A49Z&sv=2025-11-05`

## Per-endpoint comparison

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

**Status codes:** official: [200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200]

**Timing (ms):** p50: official 23.1 / aksh 35.4 | p95: official 25.9 / aksh 97.0

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
-      "size": 2,
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

**Timing (ms):** p50: official 120.6 / aksh 23.7 | p95: official 120.6 / aksh 23.7

### `GET /actions/download-artifact/tar.gz/***REDACTED***`

**Header key differences:**

- official only: `{'authorization'}`
- aksh only: `{'accept'}`

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 212.3 / aksh 1260.6 | p95: official 212.3 / aksh 1260.6

### `GET /actions/upload-artifact/tar.gz/***REDACTED***`

**Header key differences:**

- official only: `{'authorization'}`
- aksh only: `{'accept'}`

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 202.9 / aksh 263.0 | p95: official 202.9 / aksh 263.0

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 26.0 / aksh 27.3 | p95: official 26.0 / aksh 27.3

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204] | aksh: [204]

**Timing (ms):** p50: official 15.7 / aksh 21.7 | p95: official 15.7 / aksh 21.7

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
-      "modulus": "vc3BmfHvBFN2lF1pusvXL+A20QAEp1kdq2gMixvSRS9/yAzSvbA1Mot+bNhjwzDnPa+FG9o7P4GMQye//***REDACTED***+***REDACTED***/FeEAF/8t71HfZIAWcI8bW9fzqFit/azoY8zT9ZLxB1319c/L8aJCGgUo3JNivZhOWbvr9N/0jgjmUjhKE7osNQ5Q=="
+      "modulus": "rtpXpZQg1ih1/ucPfwF51lwGhrOjygvT9/RBkjvFtBlZTvzLEDc0UdLnPEa9/***REDACTED***+***REDACTED***+***REDACTED***+LYfovW079gJJEc/***REDACTED***=="
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
-  "name": "mitm-official-12-artifact-2026-06-30T15-58-25Z",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-12-artifact-1783541710",
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
-    "clientId": "fca29863-cd6b-408a-9d14-7dbd76cef1ab",
+    "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/_apis/oauth2/token",
+    "clientId": "74ea450e-fe06-4487-bb8b-079a3d7ade4e",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "vc3BmfHvBFN2lF1pusvXL+A20QAEp1kdq2gMixvSRS9/yAzSvbA1Mot+bNhjwzDnPa+FG9o7P4GMQye//***REDACTED***+***REDACTED***/FeEAF/8t71HfZIAWcI8bW9fzqFit/azoY8zT9ZLxB1319c/L8aJCGgUo3JNivZhOWbvr9N/0jgjmUjhKE7osNQ5Q=="
+      "modulus": "rtpXpZQg1ih1/ucPfwF51lwGhrOjygvT9/RBkjvFtBlZTvzLEDc0UdLnPEa9/***REDACTED***+***REDACTED***+***REDACTED***+LYfovW079gJJEc/***REDACTED***=="
     }
   },
-  "createdOn": "2026-06-30T15:58:32.63Z",
+  "createdOn": "2026-07-08T20:15:11.843Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
-  "ephemeral": false,
-  "id": 9,
+  "ephemeral": true,
+  "id": 686,
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
-  "name": "mitm-official-12-artifact-2026-06-30T15-58-25Z",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "name": "aksh-capture-12-artifact-1783541710",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -60,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-9",
+  "queueName": "taskagent-686",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 152.1 / aksh 78.2 | p95: official 152.1 / aksh 78.2

### `POST /actions/build/{guid}/jobs/{guid}/runnerresolve/actions`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

_identical_

**Response body diff:**

_identical_

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 97.1 / aksh 104.0 | p95: official 97.1 / aksh 104.0

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

**Timing (ms):** p50: official 174.1 / aksh 228.1 | p95: official 174.1 / aksh 228.1

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
-    "id": 9,
-    "name": "mitm-official-12-artifact-2026-06-30T15-58-25Z",
-    "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+    "id": 686,
+    "name": "aksh-capture-12-artifact-1783541710",
+    "osDescription": "linux aarch64",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "Nuraydias-Mac-Studio (PID: 45128)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 3550)",
+  "sessionId": "3ffd87ba-b809-4329-a16d-0e7c6e1926f9",
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
-  "ownerName": "Nuraydias-Mac-Studio (PID: 45128)",
-  "sessionId": "61de09dc-0cad-4852-8d81-d85e74fa5a3f"
+  "ownerName": "container (PID: 3550)",
+  "sessionId": "8291aa2b-a939-4422-88b0-69269ea69529"
 }
```

**Status codes:** official: [201] | aksh: [201]

**Timing (ms):** p50: official 57.9 / aksh 38.5 | p95: official 57.9 / aksh 38.5

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,15 +2,33 @@
   "change_order": 1,
   "steps": [
     {
-      "completed_at": null,
-      "conclusion": 0,
-      "external_id": "bcd55429-82b8-43d2-9faf-cbde541431e6",
+      "completed_at": "2026-07-08T20:15:26.933Z",
+      "conclusion": 2,
+      "external_id": "1dbd18d8-76ac-428c-924a-dd87405e69ee",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T15:58:38.034Z",
-      "status": 3
+      "started_at": "2026-07-08T20:15:26.933Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-08T20:15:27.372Z",
+      "conclusion": 2,
+      "external_id": "647f760c-e22c-4a70-9fde-88801aced526",
+      "name": "Run echo hi > out.txt",
+      "number": 2,
+      "started_at": "2026-07-08T20:15:27.369Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-08T20:15:54.186Z",
+      "conclusion": 3,
+      "external_id": "c52832ab-0b94-4846-8107-e1b6bc52f0fe",
+      "name": "actions/upload-artifact@v4",
+      "number": 3,
+      "started_at": "2026-07-08T20:15:27.676Z",
+      "status": 6
     }
   ],
-  "workflow_job_run_backend_id": "fead789f-6099-5bf6-a92a-eef960b13d9d",
-  "workflow_run_backend_id": "bfa562dc-b5aa-48b0-b348-767321cbc264"
+  "workflow_job_run_backend_id": "d132f8e7-829e-56e2-ade5-6bc500a59862",
+  "workflow_run_backend_id": "4a044606-e840-4445-b736-70ef38238694"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 59.4 / aksh 196.8 | p95: official 65.7 / aksh 196.8

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "fead789f-6099-5bf6-a92a-eef960b13d9d",
-  "workflow_run_backend_id": "bfa562dc-b5aa-48b0-b348-767321cbc264"
+  "workflow_job_run_backend_id": "d132f8e7-829e-56e2-ade5-6bc500a59862",
+  "workflow_run_backend_id": "4a044606-e840-4445-b736-70ef38238694"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa11.blob.core.windows.net/actions-results/bfa562dc-b5aa-48b0-b348-767321cbc264/workflow-job-run-fead789f-6099-5bf6-a92a-eef960b13d9d/logs/job/job-logs.txt?se=2026-06-30T16%3A59%3A17Z&sig=6%***REDACTED***%3D&ske=2026-06-30T19%3A50%3A46Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A50%3A46Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A59%3A12Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa9.blob.core.windows.net/actions-results/4a044606-e840-4445-b736-70ef38238694/workflow-job-run-d132f8e7-829e-56e2-ade5-6bc500a59862/logs/job/job-logs.txt?se=2026-07-08T21%3A15%3A55Z&sig=v5G%***REDACTED***%2BCiw%3D&ske=2026-07-09T00%3A09%3A57Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A09%3A57Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A50Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 30.8 / aksh 64.3 | p95: official 30.8 / aksh 64.3

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "bcd55429-82b8-43d2-9faf-cbde541431e6",
-  "workflow_job_run_backend_id": "fead789f-6099-5bf6-a92a-eef960b13d9d",
-  "workflow_run_backend_id": "bfa562dc-b5aa-48b0-b348-767321cbc264"
+  "step_backend_id": "1dbd18d8-76ac-428c-924a-dd87405e69ee",
+  "workflow_job_run_backend_id": "d132f8e7-829e-56e2-ade5-6bc500a59862",
+  "workflow_run_backend_id": "4a044606-e840-4445-b736-70ef38238694"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa11.blob.core.windows.net/actions-results/bfa562dc-b5aa-48b0-b348-767321cbc264/workflow-job-run-fead789f-6099-5bf6-a92a-eef960b13d9d/logs/steps/step-logs-bcd55429-82b8-43d2-9faf-cbde541431e6.txt?se=2026-06-30T16%3A58%3A40Z&sig=v4P07XFCcGgFznZTy2Fq%2BXOK7o%2BXDp%2FxQkM3vOEUUGE%3D&ske=2026-06-30T19%3A51%3A01Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-30T15%3A51%3A01Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-30T15%3A58%3A35Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa9.blob.core.windows.net/actions-results/4a044606-e840-4445-b736-70ef38238694/workflow-job-run-d132f8e7-829e-56e2-ade5-6bc500a59862/logs/steps/step-logs-1dbd18d8-76ac-428c-924a-dd87405e69ee.txt?se=2026-07-08T21%3A15%3A27Z&sig=***REDACTED***%2Fk7UE%3D&ske=2026-07-09T00%3A09%3A53Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-08T20%3A09%3A53Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-08T20%3A15%3A22Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 31.7 / aksh 42.1 | p95: official 105.4 / aksh 58.5

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
-  "jobMessageId": "fead789f-6099-5bf6-a92a-eef960b13d9d",
-  "runnerOS": "macOS"
+  "jobMessageId": "d132f8e7-829e-56e2-ade5-6bc500a59862",
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
-          "v": "28457979320"
+          "v": "28972658912"
         },
         {
           "k": "run_number",
-          "v": "2"
+          "v": "11"
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
-          "v": 84337915482
+          "v": 85971976675
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
-  "jobId": "fead789f-6099-5bf6-a92a-eef960b13d9d",
+  "jobId": "d132f8e7-829e-56e2-ade5-6bc500a59862",
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
+      "value": "***REDACTED***\\.y5E-psnopPNZFBw"
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
-    "planId": "bfa562dc-b5aa-48b0-b348-767321cbc264",
+    "planId": "4a044606-e840-4445-b736-70ef38238694",
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
-        "url": "https://run-actions-2-azure-eastus.actions.githubusercontent.com/205/"
+        "url": "https://run-actions-1-azure-eastus.actions.githubusercontent.com/61/"
       }
     ]
   },
@@ -915,7 +911,7 @@
       "condition": "success()",
       "contextName": "__run",
       "continueOnError": null,
-      "id": "a7d94f00-d460-4e41-9f74-de8f20ec7144",
+      "id": "647f760c-e22c-4a70-9fde-88801aced526",
       "inputs": {
         "map": [
           {
@@ -945,7 +941,7 @@
       "condition": "success()",
       "contextName": "__actions_upload-artifact",
       "continueOnError": null,
-      "id": "699b3407-025d-4c38-bec0-4000acb0cf50",
+      "id": "c52832ab-0b94-4846-8107-e1b6bc52f0fe",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1000,7 +996,7 @@
       "condition": "success()",
       "contextName": "__actions_download-artifact",
       "continueOnError": null,
-      "id": "a3292587-d908-42e0-9706-aafc9c301d60",
+      "id": "b8d34e56-6ed1-48ef-9e60-e3513a4ebd83",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1055,7 +1051,7 @@
       "condition": "success()",
       "contextName": "__run_2",
       "continueOnError": null,
-      "id": "56899bb4-5820-460d-b1e1-c5be6dc77d74",
+      "id": "665b3922-9576-47b1-a73d-21df62e4aeb8",
       "inputs": {
         "map": [
           {
@@ -1084,7 +1080,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "bfa562dc-b5aa-48b0-b348-767321cbc264",
+    "id": "4a044606-e840-4445-b736-70ef38238694",
     "location": null
   },
   "variables": {
@@ -1204,7 +1200,7 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
@@ -1223,13 +1219,13 @@
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
-      "value": "bfa562dc-b5aa-48b0-b348-767321cbc264.build.__default"
+      "value": "4a044606-e840-4445-b736-70ef38238694.build.__default"
     },
     "system.phaseDisplayName": {
       "value": "build"
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 466.8 / aksh 399.5 | p95: official 466.8 / aksh 399.5

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,114 +1,96 @@
 {
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
-  "conclusion": "succeeded",
-  "jobId": "fead789f-6099-5bf6-a92a-eef960b13d9d",
+  "conclusion": "failed",
+  "jobId": "d132f8e7-829e-56e2-ade5-6bc500a59862",
   "outputs": {},
-  "planId": "bfa562dc-b5aa-48b0-b348-767321cbc264",
+  "planId": "4a044606-e840-4445-b736-70ef38238694",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-06-30T15:58:39.068454Z",
+      "completed_at": "2026-07-08T20:15:55.331Z",
       "conclusion": "succeeded",
-      "external_id": "bcd55429-82b8-43d2-9faf-cbde541431e6",
+      "external_id": "1dbd18d8-76ac-428c-924a-dd87405e69ee",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-06-30T15:58:38.034913Z",
+      "started_at": "2026-07-08T20:15:55.331Z",
       "status": "completed",
       "type": "runner"
     },
     {
       "action_name": "sh",
       "annotations": [],
-      "completed_at": "2026-06-30T15:58:39.10275Z",
+      "completed_at": "2026-07-08T20:15:55.331Z",
       "conclusion": "succeeded",
-      "external_id": "a7d94f00-d460-4e41-9f74-de8f20ec7144",
+      "external_id": "647f760c-e22c-4a70-9fde-88801aced526",
       "name": "Run echo hi > out.txt",
       "number": 2,
-      "started_at": "2026-06-30T15:58:39.073557Z",
+      "started_at": "2026-07-08T20:15:55.331Z",
       "status": "completed",
       "type": "run"
     },
     {
-      "action_name": "actions/upload-artifact",
-      "annotations": [],
-      "completed_at": "2026-06-30T15:58:39.922439Z",
-      "conclusion": "succeeded",
-      "external_id": "699b3407-025d-4c38-bec0-4000acb0cf50",
-      "name": "Run actions/upload-artifact@v4",
+      "action_name": "actions/upload-artifact@v4",
+      "annotations": [
+        {
+          "endLine": 1,
+          "level": "failure",
+          "message": "node action exited with code 1",
+          "startLine": 1,
+          "stepNumber": 3
+        }
+      ],
+      "completed_at": "2026-07-08T20:15:55.331Z",
+      "conclusion": "failed",
+      "external_id": "c52832ab-0b94-4846-8107-e1b6bc52f0fe",
+      "name": "actions/upload-artifact@v4",
       "number": 3,
-      "ref": "v4",
-      "started_at": "2026-06-30T15:58:39.103458Z",
+      "started_at": "2026-07-08T20:15:55.331Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
-      "action_name": "actions/download-artifact",
+      "action_name": "actions/download-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-06-30T15:58:40.531599Z",
-      "conclusion": "succeeded",
-      "external_id": "a3292587-d908-42e0-9706-aafc9c301d60",
-      "name": "Run actions/download-artifact@v4",
+      "completed_at": "2026-07-08T20:15:55.331Z",
+      "conclusion": "skipped",
+      "external_id": "b8d34e56-6ed1-48ef-9e60-e3513a4ebd83",
+      "name": "actions/download-artifact@v4",
       "number": 4,
-      "ref": "v4",
-      "started_at": "2026-06-30T15:58:39.923264Z",
+      "started_at": "2026-07-08T20:15:55.331Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
       "action_name": "sh",
       "annotations": [],
-      "completed_at": "2026-06-30T15:58:40.549872Z",
-      "conclusion": "succeeded",
-      "external_id": "56899bb4-5820-460d-b1e1-c5be6dc77d74",
+      "completed_at": "2026-07-08T20:15:55.331Z",
+      "conclusion": "skipped",
+      "external_id": "665b3922-9576-47b1-a73d-21df62e4aeb8",
       "name": "Run cat dl/out.txt",
       "number": 5,
-      "started_at": "2026-06-30T15:58:40.532069Z",
+      "started_at": "2026-07-08T20:15:55.331Z",
       "status": "completed",
       "type": "run"
     },
     {
       "action_name": "complete_job",
-      "annotations": [
-        {
-          "endLine": 2,
-          "level": "warning",
-          "message": "Node.js 20 is deprecated. The following actions target Node.js 20 but are being forced to run on Node.js 24: actions/download-artifact@v4, actions/upload-artifact@v4. For more information see: https://github.blog/changelog/2025-09-19-deprecation-of-node-20-on-github-actions-runners/",
-          "startLine": 2,
-          "stepNumber": 6
-        }
-      ],
-      "completed_at": "2026-06-30T15:58:40.561327Z",
+      "annotations": [],
+      "completed_at": "2026-07-08T20:15:55.331Z",
       "conclusion": "succeeded",
-      "external_id": "5d0df9b1-5e9f-4eef-a070-52022847f27f",
+      "external_id": "9ee9ec94-3e6c-4941-b73a-45ca1a615e3d",
       "name": "Complete job",
       "number": 6,
-      "started_at": "2026-06-30T15:58:40.55393Z",
+      "started_at": "2026-07-08T20:15:55.331Z",
       "status": "completed",
       "type": "runner"
     }
   ],
   "telemetry": [
     {
-      "message": "Action archive cache usage: actions/upload-artifact@***REDACTED*** use cache False has cache False",
-      "type": "General"
-    },
-    {
-      "message": "Action archive cache usage: actions/download-artifact@***REDACTED*** use cache False has cache False",
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

**Timing (ms):** p50: official 40.9 / aksh 54.7 | p95: official 40.9 / aksh 54.7

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'accept-language', 'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "fead789f-6099-5bf6-a92a-eef960b13d9d",
-  "planId": "bfa562dc-b5aa-48b0-b348-767321cbc264"
+  "jobId": "d132f8e7-829e-56e2-ade5-6bc500a59862",
+  "planId": "4a044606-e840-4445-b736-70ef38238694"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-06-30T16:08:37.727634369Z"
+  "lockedUntil": "2026-07-08T20:25:26.9576588Z"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 100.6 / aksh 42.3 | p95: official 100.6 / aksh 42.3
