# Runner flow diff: 22-cancel-semantics

- official capture: `benchmarks/real-world/results/runner-flow/22-cancel-semantics/official/latest`
- aksh capture: `benchmarks/real-world/results/runner-flow/22-cancel-semantics/aksh/latest`
- official summary: status=ok flows=229
- aksh summary: status=ok flows=38

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
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 6 | 4 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 6 | 4 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 2 | 1 ⚠ |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 6 | 4 ⚠ |

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
   "GET broker.actions.githubusercontent.com/health",
   "GET run.actions.githubusercontent.com/health",
+  "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
   "GET token.actions.githubusercontent.com/ready",
-  "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
+  "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
+  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
   "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
@@ -27,205 +31,10 @@
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
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
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
-  "GET broker.a
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
-  "messageId": 6543838626391933975,
+  "messageId": 8245387891817297669,
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
-  "sha256": "20b83ede8d5250a501a6c3ee438cd0054187d75c66bc824bed0abe67ac737373"
+  "sha256": "8c4a01a7b65cbad516823ea49283f5778bdf9f21c493627a19dd3b044ff46300"
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
-      "modulus": "{token}/sKmAbqr+/{token}/{token}+f0xWxd/{token}/{token}/zRHKflsoHrOhdDE4vVe/{token}=="
+      "modulus": "{token}+G1sKgIHE+{token}/{token}+/SwcBsrtUSG5jLZbV+{token}/{token}/zesc/NrgnVLZnn0gAInSTOw=="
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
-      "modulus": "{token}/sKmAbqr+/{token}/{token}+f0xWxd/{token}/{token}/zRHKflsoHrOhdDE4vVe/{token}=="
+      "modulus": "{token}+G1sKgIHE+{token}/{token}+/SwcBsrtUSG5jLZbV+{token}/{token}/zesc/NrgnVLZnn0gAInSTOw=="
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
-  "queueName": "taskagent-749",
+  "queueName": "taskagent-717",
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
-  "sha256": "995e068d434f04d67fec3bc32e4174b3de89e4673f96be2fab2cc3b027935096"
+  "sha256": "c6746f4e4185e792c49a6c1f1a0038809283e799296779cff1f35cc405cf270a"
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
-      "name": "Run echo \"Step 2 - long sleep (will be cancelled)\"",
-      "number": 3,
-      "started_at": "{volatile}",
-      "status": 6
-    },
-    {
-      "completed_at": "{volatile}",
       "conclusion": 7,
       "external_id": "{volatile}",
-      "name": "Run echo \"Step 3 - default condition, should be skipped\"",
+      "name": "Run echo \"This should also never print\"",
       "number": 4,
       "started_at": "{volatile}",
       "status": 6
@@ -23,35 +14,8 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Run echo \"Step 4 - always(), should run even after cancel\"",
+      "name": "Complete job",
       "number": 5,
-      "started_at": "{volatile}",
-      "status": 6
-    },
-    {
-      "completed_at": "{volatile}",
-      "conclusion": 2,
-      "external_id": "{volatile}",
-      "name": "Run echo \"Step 5 - cancelled(), should run after cancel\"",
-      "number": 6,
-      "started_at": "{volatile}",
-      "status": 6
-    },
-    {
-      "completed_at": "{volatile}",
-      "conclusion": 7,
-      "external_id": "{volatile}",
-      "name": "Run echo \"Step 6 - failure(), should be skipped on cancel\"",
-      "number": 7,
-      "started_at": "{volatile}",
-      "status": 6
-    },
-    {
-      "completed_at": "{volatile}",
-      "conclusion": 2,
-      "external_id": "{volatile}",
-      "name": "Complete job",
-      "number": 8,
       "started_at": "{volatile}",
       "status": 6
     }
```

### `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 39,
+  "line_count": 21,
   "uploaded_at": "{time}",
   "workflow_job_run_backend_id": "{volatile}",
   "workflow_run_backend_id": "{volatile}"
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

#### occurrence 4
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 5,
+  "line_count": 1,
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
-          "v": "29035871692"
+          "v": "28996429830"
         },
         {
           "k": "run_number",
-          "v": "19"
+          "v": "16"
         },
         {
           "k": "retention_days",
@@ -61,7 +61,7 @@
         },
         {
           "k": "workflow",
-          "v": "mitm cancel semantics"
+          "v": "mitm job timeout"
         },
         {
           "k": "head_ref",
@@ -113,7 +113,7 @@
               },
               {
                 "k": "workflow",
-                "v": ".github/workflows/22-cancel-semantics.yml"
+                "v": ".github/workflows/21-job-timeout.yml"
               },
               {
                 "k": "inputs",
@@ -687,7 +687,7 @@
         },
         {
           "k": "workflow_ref",
-          "v": "preloopdev/aksh-conformance-sample/.github/workflows/22-cancel-semantics.yml@refs/heads/main"
+          "v": "preloopdev/aksh-conformance-sample/.github/workflows/21-job-timeout.yml@refs/heads/main"
         },
         {
           "k": "workflow_sha",
@@ -743,10 +743,10 @@
   "defaults": [],
   "environmentVariables": [],
   "fileTable": [
-    ".github/workflows/22-cancel-semantics.yml"
+    ".github/workflows/21-job-timeout.yml"
   ],
   "jobContainer": null,
-  "jobDisplayName": "cancel-test",
+  "jobDisplayName": "timeout-test",
   "jobId": "{guid}",
   "jobName": "__default",
   "jobOutputs": null,
@@ -839,7 +839,7 @@
     },
     {
       "type": "regex",
-      "value": "{token}\\.6-PPymN9tTb6qbY"
+      "value": "{token}\\.ca65DzuJIegl4qv"
     },
     {
       "type": "regex",
@@ -893,7 +893,7 @@
               "col": 14,
               "file": 1,
               "line": 8,
-              "lit": "echo \"Step 1 - will run\"",
+              "lit": "echo \"Starting long step...\"",
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
-  "bytes": 2564,
-  "sha256": "8dc3e8383cd59d0aaffe38298f801782cd0b2e29097a4234c3cc116479576018"
+  "bytes": 1291,
+  "sha256": "f5eb68c9a112879ec6e6b326246622d3b0723379d8597ae59bd8c581d1be7ca6"
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
-  "bytes": 964,
-  "sha256": "f3a143f0aa43c5fd2c41ad13829bc341b7ce8ead5cce5a00d76459ace4dc227b"
+  "bytes": 457,
+  "sha256": "7b2f92bca512732f3277d68ec80c0dbbe84931ab9e8713f0bf7a632426c69f4c"
 }
```

#### occurrence 2
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "bytes": 281,
-  "sha256": "8943729f745b0cd440df7ecff2d67e2547f2e17194c53ff607facb2eaf472551"
+  "sha256": "ce8b173bd23ab6a09952ad69cba2ed3a3992f23f7c0f1a5e6e9621daef10838f"
 }
```

#### occurrence 3
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 463,
-  "sha256": "e10f3f07541b522cdff73343051d04bf9d23258225c7247dd77892f14d6ac662"
+  "bytes": 498,
+  "sha256": "232c2a944f723cb771076b02a08e1ee3bc3a68a4634818f9df298af3293ded5a"
 }
```

#### occurrence 4
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 371,
-  "sha256": "c7f9d39623e9f81d9da915606b82f4f24ac934dca6b4f2321e5072af081bb2b1"
+  "bytes": 53,
+  "sha256": "249ea149f7b546b62ae763cfba1693093dfc7fc046676752f705121455be006b"
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
