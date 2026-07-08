# Runner flow diff: 92-unicode-special-chars

- official capture: `benchmarks/real-world/results/runner-flow/92-unicode-special-chars/official/latest`
- aksh capture: `benchmarks/real-world/results/runner-flow/92-unicode-special-chars/aksh/latest`
- official summary: status=ok flows=51
- aksh summary: status=ok flows=53

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/health` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 1 |
| `GET nodejs.org/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 ⚠ |
| `GET nodejs.org/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 6 | 6 |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 1 | 1 |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 1 | 1 |
| `GET run.actions.githubusercontent.com/health` | 1 | 1 |
| `GET token.actions.githubusercontent.com/ready` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/session` | 1 | 1 |
| `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents` | 1 | 1 |
| `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/oauth2/token` | 2 | 2 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 8 | 8 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 8 | 8 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 8 | 8 |

## Endpoint sequence diff

```diff
--- official
+++ aksh
@@ -8,6 +8,8 @@
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET nodejs.org/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz",
+  "GET nodejs.org/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz",
   "POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/oauth2/token",
   "POST broker.actions.githubusercontent.com/session",
   "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
@@ -16,11 +18,11 @@
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob",
   "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
-  "GET broker.actions.githubusercontent.com/health",
   "GET token.actions.githubusercontent.com/ready",
   "GET run.actions.githubusercontent.com/health",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "GET broker.actions.githubusercontent.com/health",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
@@ -44,10 +46,10 @@
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
+  "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
-  "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob",
   "DELETE broker.actions.githubusercontent.com/session"
 ]
```

## Per-flow contract differences

### `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}`

#### occurrence 1
- response redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/221/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
-  "messageId": 6889348772513085760,
+  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/120/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
+  "messageId": 4051011470762753415,
   "messageType": "RunnerJobRequest"
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
-  "sha256": "88720fc5627dbc3e1d88e3a4dd0af496807277703e087ea87f64c3fb9612b21f"
+  "sha256": "35ef8f0c3f48f8ab4c08a7a6d626917b45a2d36f7e855c986f8bd0ee2968a1c1"
 }
```

### `POST broker.actions.githubusercontent.com/session`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,6 +1,6 @@
 {
   "agent": "{volatile}",
-  "ownerName": "container (PID: 91)",
+  "ownerName": "container (PID: 32)",
   "sessionId": "{guid}",
   "useFipsEncryption": false
 }
```
- response redacted value differs
```diff
--- official
+++ aksh
@@ -1,6 +1,6 @@
 {
   "assignmentQueued": false,
   "orchestrationId": "",
-  "ownerName": "container (PID: 91)",
+  "ownerName": "container (PID: 32)",
   "sessionId": "{guid}"
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
-      "modulus": "{token}+AQkA6e3K1sdYSsn9N4BI+{token}+{token}/uTBUU/e1l8FIZfzWQX1Hs4iXnU/{token}/{token}+7uISBKgBIo9Ob7mMeaCB+zZvzAc+6iVWuemMweAMHb/xoYZiQ=="
+      "modulus": "rHRh/46R+IEFfpF6RcL+{token}/LQ5+{token}/WE5CAeSJWqBnUX/{token}/WYXgkMdrEd2s9AV3IZWO4K+pC+{token}/{token}+{token}=="
     }
   },
   "createdOn": "{time}",
-  "disableUpdate": false,
+  "disableUpdate": true,
   "ephemeral": true,
   "id": "{volatile}",
   "labels": [
@@ -43,7 +43,7 @@
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
-      "modulus": "{token}+AQkA6e3K1sdYSsn9N4BI+{token}+{token}/uTBUU/e1l8FIZfzWQX1Hs4iXnU/{token}/{token}+7uISBKgBIo9Ob7mMeaCB+zZvzAc+6iVWuemMweAMHb/xoYZiQ=="
+      "modulus": "rHRh/46R+IEFfpF6RcL+{token}/LQ5+{token}/WE5CAeSJWqBnUX/{token}/WYXgkMdrEd2s9AV3IZWO4K+pC+{token}/{token}+{token}=="
     }
   },
   "createdOn": "{time}",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
   "ephemeral": true,
   "id": "{volatile}",
@@ -39,7 +39,7 @@
   ],
   "maxParallelism": 1,
   "name": "{token}",
-  "osDescription": "Ubuntu 24.04.4 LTS",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -60,7 +60,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-554",
+  "queueName": "taskagent-569",
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
-  "sha256": "9d5bd0f014eb6541d1c2ef5fec7414cff877120496b8e834bcb8edf2b6ca5e1b"
+  "sha256": "58edf7915b2af747a34077ea5096518afc243c74c940caca798f89875bb9e0f5"
 }
```

#### occurrence 2
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "bytes": 921,
-  "sha256": "ca64549d00e73f5a3f2f25c7574ac96e381c8c83a4e877f996704d01dc9ad5b6"
+  "sha256": "0b840fb195241b1e8d93c62676fdfd1e73493e383621c7f55674db067777e692"
 }
```

### `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -14,7 +14,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Set environment with unicode and special chars",
+      "name": "Run echo \"EMOJI_VAR=$EMOJI_VAR\"",
       "number": 2,
       "started_at": "{volatile}",
       "status": 6
@@ -23,7 +23,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Test unicode in output variables",
+      "name": "Run echo 'emoji_output<<EOF' >> $GITHUB_OUTPUT",
       "number": 3,
       "started_at": "{volatile}",
       "status": 6
@@ -32,7 +32,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Retrieve and verify unicode outputs",
+      "name": "Run EMOJI='🌟 Success! ✅ All tests passed 🎯'",
       "number": 4,
       "started_at": "{volatile}",
       "status": 6
@@ -41,7 +41,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Test file paths with spaces and unicode",
+      "name": "Run mkdir -p \"test dir with spaces\"",
       "number": 5,
       "started_at": "{volatile}",
       "status": 6
@@ -50,7 +50,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Test env var with newlines",
+      "name": "Run echo \"Multiline env var:\"",
       "number": 6,
       "started_at": "{volatile}",
       "status": 6
@@ -59,7 +59,7 @@
       "completed_at": "{volatile}",
       "conclusion": 3,
       "external_id": "{volatile}",
-      "name": "Test hex escape sequences",
+      "name": "Run echo 'hex_output<<EOF' >> $GITHUB_OUTPUT",
       "number": 7,
       "started_at": "{volatile}",
       "status": 6
@@ -68,14 +68,14 @@
       "completed_at": "{volatile}",
       "conclusion": 7,
       "external_id": "{volatile}",
-      "name": "Verify special character round-trip",
+      "
... truncated ...
```

### `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 126,
+  "line_count": 108,
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

#### occurrence 2
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 18,
+  "line_count": 12,
   "step_backend_id": "{volatile}",
   "uploaded_at": "{time}",
   "workflow_job_run_backend_id": "{volatile}",
```

#### occurrence 6
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 20,
+  "line_count": 15,
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
-          "v": "28908142378"
+          "v": "28912842887"
         },
         {
           "k": "run_number",
-          "v": "8"
+          "v": "13"
         },
         {
           "k": "retention_days",
@@ -839,7 +839,7 @@
     },
     {
       "type": "regex",
-      "value": "{token}\\.EyVeBVfM6UAt37B"
+      "value": "{token}\\.8U4MjZfuUd-3NBR"
     },
     {
       "type": "regex",
```

### `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob`

#### occurrence 1
- request schema differs
```diff
--- official
+++ aksh
@@ -37,16 +37,6 @@
       "started_at": "string",
       "status": "string",
       "type": "string"
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
-  "bytes": 8427,
-  "sha256": "dd9f3d8fc7fb09ff18c393a356a29421fbc19d5ed24815aa938bbe91a71bd7c8"
+  "bytes": 6576,
+  "sha256": "4bcdf8613df4ff4354cde344b93221c4e984793a98de61de76c7d00f30316467"
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
-  "bytes": 984,
-  "sha256": "796f5cdb9d7f731da907c78fcd7616e887dc147bde2c000d9a50a04a18c40c9d"
+  "bytes": 476,
+  "sha256": "5b56f1de0a8289440325694f4341bb040f119a31345a1f5be0ac616414435bab"
 }
```

#### occurrence 2
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1083,
-  "sha256": "88c9b1a6addd652626f20a9658e1fe6d6a3a8abe29057afb27237d0daa7387d6"
+  "bytes": 681,
+  "sha256": "43c2e48244222d81401d8e0061089fc30c4e4d5247dffae1a7e58b33382fd9d7"
 }
```

#### occurrence 3
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1084,
-  "sha256": "bed361fa19c1ef1eb70861918ab8a261a2b549c8d3bb8adb4902387553a83898"
+  "bytes": 926,
+  "sha256": "018e32a7fae6217b7192528da0fc94faff7a6fb96ca691af9940768728ec3f84"
 }
```

#### occurrence 4
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 2049,
-  "sha256": "3d06fd7963573149a7d861939563f98302a43d217a9ee7298c9c787e9d696da5"
+  "bytes": 1721,
+  "sha256": "b7cacf2f02e9c25212536434945c4dfd29460aba86b9554a75fedd66471d79b6"
 }
```

#### occurrence 5
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1365,
-  "sha256": "a9d57f86ef90ce13bf4ff423b9da7e7a25a8e8aea17447490e7472e713f609b2"
+  "bytes": 1216,
+  "sha256": "629a8a21f22c8dc8794d3240949d457cd35626a52256d9789a264235f2912dd7"
 }
```

#### occurrence 6
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1102,
-  "sha256": "aa8613cae4fe25173016282ba81d8980196c11279ad980f4691eea13be91004a"
+  "bytes": 823,
+  "sha256": "6239740186aabb1c0b368f3bc1a043decfd6fdc227b8cdc4134b1343f7d0fb38"
 }
```

#### occurrence 7
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 720,
-  "sha256": "0901b627b6af34076b18d7399f9d12405f6e49ebc97d415a1b9a08b6213c4462"
+  "bytes": 678,
+  "sha256": "81abf419893237d05867d866d3d2398d45acb18e57f7ee867daaf167d6fea81e"
 }
```

#### occurrence 8
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 61,
-  "sha256": "f3621ea463fac2b7c26b984b66e1fce4ea3cc0007001520e28a7663235d6f034"
+  "bytes": 53,
+  "sha256": "696d8ab0badaa5fcc795113faa71dae6a0cb8fb23567a23ac551f8fead59674f"
 }
```

## Verdict

FAIL: 27 contract differences found.

- endpoint-sequence: 1
- request-binary: 11
- request-schema: 1
- request-value: 7
- response-binary: 1
- response-schema: 1
- response-value: 4
- status: 1
