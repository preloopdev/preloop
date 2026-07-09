# Runner flow diff: 21-job-timeout

- official capture: `benchmarks/real-world/results/runner-flow/21-job-timeout/official/latest`
- aksh capture: `benchmarks/real-world/results/runner-flow/21-job-timeout/aksh/latest`
- official summary: status=ok flows=223
- aksh summary: status=ok flows=44

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/health` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 183 | 2 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 0 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 6 | 6 |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 1 | 1 |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 1 | 1 |
| `GET run.actions.githubusercontent.com/health` | 1 | 1 |
| `GET token.actions.githubusercontent.com/ready` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/session` | 1 | 0 ⚠ |
| `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents` | 1 | 1 |
| `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/oauth2/token` | 2 | 1 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 2 | 2 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 4 | 6 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 4 | 6 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 2 | 1 ⚠ |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 4 | 6 ⚠ |

## Endpoint sequence diff

```diff
--- official
+++ aksh
@@ -8,18 +8,22 @@
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/oauth2/token",
-  "POST broker.actions.githubusercontent.com/session",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
   "POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64",
   "POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/oauth2/token",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob",
   "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "GET broker.actions.githubusercontent.com/health",
   "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
+  "GET token.actions.githubusercontent.com/ready",
   "GET run.actions.githubusercontent.com/health",
-  "GET token.actions.githubusercontent.com/ready",
-  "GET broker.actions.githubusercontent.com/health",
+  "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
+  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
   "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
@@ -27,199 +31,16 @@
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpd
... truncated ...
```

## Per-flow contract differences

### `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}`

#### occurrence 1
- status: official=202 aksh=None

#### occurrence 2
- response redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "body": "{\"jobId\":\"{guid}\",\"timeout\":\"0.00:05:00.0000\"}",
-  "messageId": 1726084727432460049,
+  "messageId": 7266667065315197237,
   "messageType": "JobCancellation"
 }
```

### `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}`

#### occurrence 1
- response schema differs
```diff
--- official
+++ aksh
@@ -3,7 +3,6 @@
   "deploymentType": "string",
   "instanceId": "string",
   "locationServiceData": {
-    "clientCacheFresh": "bool",
     "defaultAccessMappingMoniker": "string",
     "lastChangeId": "number",
     "lastChangeId64": "number",
```

### `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock`

#### occurrence 1
- status: official=101 aksh=401

### `GET run.actions.githubusercontent.com/health`

#### occurrence 1
- response binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "bytes": 51,
-  "sha256": "f851eb22d02ce1b60a781efe7d1d4a5679bd339bff77c5bd10e54adea88612a9"
+  "sha256": "7dc00268876482414f5923baca12dedd6ad5476d1d6a6b37162270738f19c2f0"
 }
```

### `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -2,11 +2,11 @@
   "authorization": {
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "7bem1YlAhu+qEw/jKa5AXLyLTs+xJVQbOUj0mLS7BcBkv5h7vK/{token}+09KtBVKeAVa4qt3H+64TO6sQEvoTX5o+{token}/KQVCnQChwyYpT+L+ONn84V8Ggm/g4ywbLU2vOEN+{token}=="
+      "modulus": "{token}/1Ru3pEthHiRSx/{token}+{token}/vMgDyTwZnQt7NFel5mF+0K4QE7AEyc1n+/{token}/{token}/{token}/+Z5quN2I395+{token}+{token}=="
     }
   },
   "createdOn": "{time}",
-  "disableUpdate": false,
+  "disableUpdate": true,
   "ephemeral": true,
   "id": "{volatile}",
   "labels": [
@@ -48,7 +48,7 @@
   ],
   "maxParallelism": 1,
   "name": "{token}",
-  "osDescription": "Ubuntu 24.04.4 LTS",
+  "osDescription": "linux aarch64",
   "provisioningState": "Provisioned",
   "status": 0,
   "version": "2.335.1"
```
- response redacted value differs
```diff
--- official
+++ aksh
@@ -4,12 +4,12 @@
     "clientId": "{guid}",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "7bem1YlAhu+qEw/jKa5AXLyLTs+xJVQbOUj0mLS7BcBkv5h7vK/{token}+09KtBVKeAVa4qt3H+64TO6sQEvoTX5o+{token}/KQVCnQChwyYpT+L+ONn84V8Ggm/g4ywbLU2vOEN+{token}=="
+      "modulus": "{token}/1Ru3pEthHiRSx/{token}+{token}/vMgDyTwZnQt7NFel5mF+0K4QE7AEyc1n+/{token}/{token}/{token}/+Z5quN2I395+{token}+{token}=="
     }
   },
   "createdOn": "{time}",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
   "ephemeral": true,
   "id": "{volatile}",
@@ -44,7 +44,7 @@
   ],
   "maxParallelism": 1,
   "name": "{token}",
-  "osDescription": "Ubuntu 24.04.4 LTS",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -65,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-748",
+  "queueName": "taskagent-716",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

### `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/oauth2/token`

#### occurrence 1
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "bytes": 921,
-  "sha256": "c5a06eb4d445ec4875f5e599100c9c3539d31206757c18f8222b146f6c38a47b"
+  "sha256": "1c8140cdcb90f8abb7d9eedc3db314a6bcef647ba875a0b051ae4bec65c0b4f9"
 }
```

### `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

#### occurrence 1
- request schema differs
```diff
--- official
+++ aksh
@@ -9,24 +9,6 @@
       "number": "number",
       "started_at": "string",
       "status": "number"
-    },
-    {
-      "completed_at": "null",
-      "conclusion": "number",
-      "external_id": "string",
-      "name": "string",
-      "number": "number",
-      "started_at": "string",
-      "status": "number"
-    },
-    {
-      "completed_at": "null",
-      "conclusion": "number",
-      "external_id": "string",
-      "name": "string",
-      "number": "number",
-      "started_at": "null",
-      "status": "number"
     }
   ],
   "workflow_job_run_backend_id": "string",
```

#### occurrence 2
- request redacted value differs
```diff
--- official
+++ aksh
@@ -3,18 +3,9 @@
   "steps": [
     {
       "completed_at": "{volatile}",
-      "conclusion": 4,
-      "external_id": "{volatile}",
-      "name": "Run echo \"Sleeping for 120 seconds (should be cancelled by 1-min timeout)\"",
-      "number": 3,
-      "started_at": "{volatile}",
-      "status": 6
-    },
-    {
-      "completed_at": "{volatile}",
       "conclusion": 7,
       "external_id": "{volatile}",
-      "name": "Run echo \"This should also never print\"",
+      "name": "Run echo \"Step 3 - default condition, should be skipped\"",
       "number": 4,
       "started_at": "{volatile}",
       "status": 6
@@ -23,8 +14,35 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
+      "name": "Run echo \"Step 4 - always(), should run even after cancel\"",
+      "number": 5,
+      "started_at": "{volatile}",
+      "status": 6
+    },
+    {
+      "completed_at": "{volatile}",
+      "conclusion": 2,
+      "external_id": "{volatile}",
+      "name": "Run echo \"Step 5 - cancelled(), should run after cancel\"",
+      "number": 6,
+      "started_at": "{volatile}",
+      "status": 6
+    },
+    {
+      "completed_at": "{volatile}",
+      "conclusion": 7,
+      "external_id": "{volatile}",
+      "name": "Run echo \"Step 6 - failure(), should be skipped on cancel\"",
+      "number": 7,
+      "started_at": "{volatile}",
+      "status": 6
+    },
+    {
+      "completed_at": "{volatile}",
+      "conclusion": 2,
+      "external_id": "{volatile}",
       "name": "Complete job",
-      "number": 5,
+      "number": 8,
       "started_at": "{volatile}",
       "status": 6
     }
```

### `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 15,
+  "line_count": 8,
   "step_backend_id": "{volatile}",
   "uploaded_at": "{time}",
   "workflow_job_run_backend_id": "{volatile}",
```

#### occurrence 3
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 8,
+  "line_count": 6,
   "step_backend_id": "{volatile}",
   "uploaded_at": "{time}",
   "workflow_job_run_backend_id": "{volatile}",
```

#### occurrence 4
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 2,
+  "line_count": 5,
   "step_backend_id": "{volatile}",
   "uploaded_at": "{time}",
   "workflow_job_run_backend_id": "{volatile}",
```

### `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob`

#### occurrence 1
- response redacted value differs
```diff
--- official
+++ aksh
@@ -29,11 +29,11 @@
         },
         {
           "k": "run_id",
-          "v": "29035746879"
+          "v": "28996434656"
         },
         {
           "k": "run_number",
-          "v": "18"
+          "v": "17"
         },
         {
           "k": "retention_days",
@@ -61,7 +61,7 @@
         },
         {
           "k": "workflow",
-          "v": "mitm job timeout"
+          "v": "mitm cancel semantics"
         },
         {
           "k": "head_ref",
@@ -113,7 +113,7 @@
               },
               {
                 "k": "workflow",
-                "v": ".github/workflows/21-job-timeout.yml"
+                "v": ".github/workflows/22-cancel-semantics.yml"
               },
               {
                 "k": "inputs",
@@ -687,7 +687,7 @@
         },
         {
           "k": "workflow_ref",
-          "v": "preloopdev/aksh-conformance-sample/.github/workflows/21-job-timeout.yml@refs/heads/main"
+          "v": "preloopdev/aksh-conformance-sample/.github/workflows/22-cancel-semantics.yml@refs/heads/main"
         },
         {
           "k": "workflow_sha",
@@ -743,10 +743,10 @@
   "defaults": [],
   "environmentVariables": [],
   "fileTable": [
-    ".github/workflows/21-job-timeout.yml"
+    ".github/workflows/22-cancel-semantics.yml"
   ],
   "jobContainer": null,
-  "jobDisplayName": "timeout-test",
+  "jobDisplayName": "cancel-test",
   "jobId": "{guid}",
   "jobName": "__default",
   "jobOutputs": null,
@@ -839,7 +839,7 @@
     },
     {
       "type": "regex",
-      "value": "{token}\\.O6dBaQfwAVidiqQ"
+      "value": "{token}\\.ia7AK45GVSaoQdQ"
     },
     {
       "type": "regex",
@@ -893,7 +893,7 @@
               "col": 14,
               "file": 1,
               "line": 8,
-              "lit": "echo \"Starting long step...\"",
+              "lit": "echo \"Step 1 - will run\"",
               "type": 0
             }
           }
@@ -923,7 +923,7 @@
               "col": 14,
               "file":
... truncated ...
```

### `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob`

#### occurrence 1
- request schema differs
```diff
--- official
+++ aksh
@@ -17,36 +17,6 @@
       "started_at": "string",
       "status": "string",
       "type": "string"
-    },
-    {
-      "action_name": "string",
-      "annotations": [
-        {
-          "endLine": "number",
-          "level": "string",
-          "message": "string",
-          "startLine": "number",
-          "stepNumber": "number"
-        }
-      ],
-      "completed_at": "string",
-      "conclusion": "string",
-      "external_id": "string",
-      "name": "string",
-      "number": "number",
-      "started_at": "string",
-      "status": "string",
-      "type": "string"
-    },
-    {
-      "annotations": [],
-      "completed_at": "string",
-      "conclusion": "string",
-      "external_id": "string",
-      "name": "string",
-      "number": "number",
-      "started_at": "string",
-      "status": "string"
     }
   ],
   "telemetry": [
```

### `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt`

#### occurrence 1
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1985,
-  "sha256": "9f9437e31e9a7dc8ce603fbb154371029a1f17544d5d342ffe4a4e9912dfad98"
+  "bytes": 1857,
+  "sha256": "3faaa1501893fb13fcee5f8e6c0193a452b089202cd5b087c080574b9aa092dc"
 }
```

### `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt`

#### occurrence 1
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 960,
-  "sha256": "8b9fbf5731a2f9588371f11e15541cea29ac97ae8d2ae0abdcbc2dea683b39c1"
+  "bytes": 451,
+  "sha256": "1d04ceb0a53af1cd66b28f6711a08c14f293fcc0ef4c6edd7c7412083c265891"
 }
```

#### occurrence 2
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 293,
-  "sha256": "f6f4c347cb93316ef79ac7ef7ce180f726289adedabb47f90e2d98b1d3e88ad1"
+  "bytes": 269,
+  "sha256": "386e43ee86414465c5e57d52af54f71099b84a48b73384865974b9cf88932c7a"
 }
```

#### occurrence 3
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 606,
-  "sha256": "1a570de36f9d4836a25c33e30891ecd960c9dfc7ccfe9aa8d3ade23a05328d99"
+  "bytes": 370,
+  "sha256": "316ded93087ec9e2242e6a173234477feffefb4eeb3a170647db179e6a149acb"
 }
```

#### occurrence 4
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 135,
-  "sha256": "3bab16aea2f06ba35ca069562212749c477439a5200549e3301b40869f843041"
+  "bytes": 359,
+  "sha256": "95bbbd68bae12ba07c1babac122251c99c48320eab466a71976b165349f80e93"
 }
```

## Verdict

FAIL: 21 contract differences found.

- endpoint-sequence: 1
- request-binary: 6
- request-schema: 2
- request-value: 5
- response-binary: 1
- response-schema: 1
- response-value: 3
- status: 2
