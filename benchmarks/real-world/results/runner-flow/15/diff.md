# Runner flow diff: 15-oidc-id-token

- official capture: `benchmarks/real-world/results/runner-flow/15-oidc-id-token/official/latest`
- aksh capture: `benchmarks/real-world/results/runner-flow/15-oidc-id-token/aksh/latest`
- official summary: status=ok flows=37
- aksh summary: status=ok flows=37

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/health` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 1 |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 3 | 6 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}` | 3 | 0 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 1 | 1 |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 1 | 1 |
| `GET run.actions.githubusercontent.com/health` | 1 | 1 |
| `GET token.actions.githubusercontent.com/ready` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/session` | 1 | 1 |
| `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents` | 1 | 1 |
| `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/oauth2/token` | 2 | 2 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 2 | 2 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 3 | 3 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 3 | 3 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 3 | 3 |

## Endpoint sequence diff

```diff
--- official
+++ aksh
@@ -1,7 +1,7 @@
 [
-  "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools?poolType=Automation",
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False",
   "POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents",
@@ -16,10 +16,13 @@
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob",
   "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "GET broker.actions.githubusercontent.com/health",
   "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
+  "GET run.actions.githubusercontent.com/health",
   "GET token.actions.githubusercontent.com/ready",
-  "GET broker.actions.githubusercontent.com/health",
-  "GET run.actions.githubusercontent.com/health",
+  "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
   "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
@@ -27,13 +30,10 @@
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
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
-  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/39/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
-  "messageId": 2610184153354346968,
+  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/111/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
+  "messageId": 3482807693499311504,
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

### `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools?poolType=Automation`

#### occurrence 1
- response redacted value differs
```diff
--- official
+++ aksh
@@ -10,7 +10,7 @@
       "isInternal": true,
       "name": "Default",
       "scope": "{guid}",
-      "size": 1,
+      "size": 3,
       "targetSize": null
     },
     {
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
   "bytes": 50,
-  "sha256": "ab32aa245faa743553a959c657ad4bcfa6a031325d56394bda6044f299d8cbb5"
+  "sha256": "2738d982aa6c75ec3bda6f640557087be0e876f668ec25d20217058ab338645d"
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
-  "ownerName": "container (PID: 2936)",
+  "ownerName": "container (PID: 96)",
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
-  "ownerName": "container (PID: 2936)",
+  "ownerName": "container (PID: 96)",
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
-      "modulus": "{token}+8RNqsuG6tVNAQSfKm206/{token}/{token}/{token}+w++{token}/JWUs95fsoOW5/{token}=="
+      "modulus": "{token}/{token}+{token}/du+{token}+wbfKyrtyvFV8/{token}+KMHSwF2kif3yhzmiQb1oKy6+ocLw=="
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
-      "modulus": "{token}+8RNqsuG6tVNAQSfKm206/{token}/{token}/{token}+w++{token}/JWUs95fsoOW5/{token}=="
+      "modulus": "{token}/{token}+{token}/du+{token}+wbfKyrtyvFV8/{token}+KMHSwF2kif3yhzmiQb1oKy6+ocLw=="
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
-  "queueName": "taskagent-746",
+  "queueName": "taskagent-755",
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
-  "sha256": "fc5d0076a931ab0edcb3b744c2403f5ec8b02acf14ad95638270e032f0ce7dd2"
+  "sha256": "d155ecc79e67a54f5e3a475e0907f7737cb1e13945f9226e78bf780e9217bc83"
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
-  "sha256": "49fdc13baf921c91364d7782e4c7a0f28711c47acf18744ee6ae2b2daf5b429b"
+  "sha256": "88b4d5fb639c60786f8a01402344231fcd7be2cc234003f74877e649534e4585"
 }
```

### `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

#### occurrence 1
- request schema differs
```diff
--- official
+++ aksh
@@ -9,15 +9,6 @@
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
@@ -1,15 +1,6 @@
 {
   "change_order": 2,
   "steps": [
-    {
-      "completed_at": "{volatile}",
-      "conclusion": 3,
-      "external_id": "{volatile}",
-      "name": "Run curl -sS -H \"Authorization: ***\" \\",
-      "number": 2,
-      "started_at": "{volatile}",
-      "status": 6
-    },
     {
       "completed_at": "{volatile}",
       "conclusion": 2,
```

### `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 22,
+  "line_count": 17,
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
-  "line_count": 14,
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
-  "line_count": 7,
+  "line_count": 8,
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
-          "v": "29035640286"
+          "v": "29036551282"
         },
         {
           "k": "run_number",
-          "v": "15"
+          "v": "17"
         },
         {
           "k": "retention_days",
@@ -839,7 +839,7 @@
     },
     {
       "type": "regex",
-      "value": "{token}\\.lMRUy9EllaTXWGR"
+      "value": "{token}\\.2dREeGM9-qrit9K"
     },
     {
       "type": "regex",
@@ -862,7 +862,7 @@
           "CacheServerUrl": "https://artifactcache.actions.githubusercontent.com/{token}/",
           "ConnectivityChecks": "[\"https://broker.actions.githubusercontent.com/health\",\"https://token.actions.githubusercontent.com/ready\",\"https://run.actions.githubusercontent.com/health\"]",
           "FeedStreamUrl": "wss://results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
-          "GenerateIdTokenUrl": "https://{token}.actions.githubusercontent.com/39//idtoken/{guid}/{guid}?api-version=2.0",
+          "GenerateIdTokenUrl": "https://{token}.actions.githubusercontent.com/111//idtoken/{guid}/{guid}?api-version=2.0",
           "PipelinesServiceUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/{token}/",
           "ResultsServiceUrl": "https://results-receiver.actions.githubusercontent.com/",
           "ServerId": "",
```

### `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -22,17 +22,24 @@
       "action_name": "sh",
       "annotations": [
         {
-          "endLine": 7,
+          "endLine": 1,
           "level": "failure",
           "message": "Process completed with exit code 127.",
-          "startLine": 7,
+          "startLine": 1,
+          "stepNumber": 2
+        },
+        {
+          "endLine": 1,
+          "level": "failure",
+          "message": "process exit code 127",
+          "startLine": 1,
           "stepNumber": 2
         }
       ],
       "completed_at": "{volatile}",
       "conclusion": "failed",
       "external_id": "{volatile}",
-      "name": "Run curl -sS -H \"Authorization: ***\" \\",
+      "name": "Run curl -sS -H \"Authorization: Bearer ${token}\" \\",
       "number": 2,
       "started_at": "{volatile}",
       "status": "completed",
@@ -53,16 +60,8 @@
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

### `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt`

#### occurrence 1
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1536,
-  "sha256": "a447369f0f9bd6349797d4214d626c4ac2d8794eec33d6f981b2ebb664a90691"
+  "bytes": 1171,
+  "sha256": "03214393ffc8b0cea6d92253761622a8c34fd6d41b26145c22f24a186d910b93"
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
-  "bytes": 911,
-  "sha256": "b5beadfa5bac3f52db449b1f843602025c71d309cf667623424fe1c92e4822f6"
+  "bytes": 447,
+  "sha256": "818c634343ff104ac87f70f1cd458bcd2703e502aaf1771a029ae9beb46742c1"
 }
```

#### occurrence 2
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 570,
-  "sha256": "2d72d203258ba059cd53b3ea716bae1f9794b7a9bce0f81493d896c67319fe5a"
+  "bytes": 669,
+  "sha256": "074ed2efe9e92a8009ae3533d92ae62ef1c4b6b7bdf8dff8fbbfc900fb859344"
 }
```

#### occurrence 3
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 61,
-  "sha256": "d154e38ce041f921a486b91503e2be1c87e86f57450dde75fceb2ad80f1cb4b2"
+  "bytes": 53,
+  "sha256": "d0b6207f0564ee3c229ee7f99062a134315e4f5d9f9321bc0fe21845108ee89c"
 }
```

## Verdict

FAIL: 23 contract differences found.

- endpoint-sequence: 1
- request-binary: 6
- request-schema: 1
- request-value: 7
- response-binary: 1
- response-schema: 1
- response-value: 5
- status: 1
