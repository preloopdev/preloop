# Runner flow diff: 93-empty-null-values

- official capture: `/Users/bnjoroge/workflow-triggers/benchmarks/real-world/results/runner-flow/93-empty-null-values/official/latest`
- aksh capture: `/Users/bnjoroge/workflow-triggers/benchmarks/real-world/results/runner-flow/93-empty-null-values/aksh/latest`
- official summary: status=ok flows=69
- aksh summary: status=ok flows=71

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
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 14 | 14 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 14 | 14 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 14 | 14 |

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
+  "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
+  "GET token.actions.githubusercontent.com/ready",
   "GET broker.actions.githubusercontent.com/health",
   "GET run.actions.githubusercontent.com/health",
-  "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
-  "GET token.actions.githubusercontent.com/ready",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
@@ -62,10 +64,10 @@
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
-  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/19/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
-  "messageId": 4664487091323980923,
+  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/114/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
+  "messageId": 4861983066216545093,
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
-  "bytes": 49,
-  "sha256": "513993176cc717800874185beb0639bcdfc9984f7287bd0712940d5f55211145"
+  "bytes": 50,
+  "sha256": "501783bd7581e5255bea337a6075b6547b10d6386f8cb6c7733a9811bb0990bd"
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
-      "modulus": "yyvHa9q8x8EIL6ca/{token}+{token}/{token}+dTKOUJLKy7+//W+2SNmYWGqBufVm1NR/{token}+duxBjQ1w/MrGuFGBh7VZzihliTyStAkF/WSi9UCdM/{token}++cH/hXTh3FQ8ap1NfgSslQ+w=="
+      "modulus": "2z9uLxhplzA6jvE5+799A2QJV+{token}++zsT3+{token}+{token}/{token}/2xsKBPI/XGt6sTs6+n67QF9JM+0ELeHjtcbL+tZVFMGHH/{token}+hEPe/+{token}=="
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
-      "modulus": "yyvHa9q8x8EIL6ca/{token}+{token}/{token}+dTKOUJLKy7+//W+2SNmYWGqBufVm1NR/{token}+duxBjQ1w/MrGuFGBh7VZzihliTyStAkF/WSi9UCdM/{token}++cH/hXTh3FQ8ap1NfgSslQ+w=="
+      "modulus": "2z9uLxhplzA6jvE5+799A2QJV+{token}++zsT3+{token}+{token}/{token}/2xsKBPI/XGt6sTs6+n67QF9JM+0ELeHjtcbL+tZVFMGHH/{token}+hEPe/+{token}=="
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
-  "queueName": "taskagent-549",
+  "queueName": "taskagent-567",
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
-  "sha256": "8665998fbc5b18ea26e4bc53dfdf3ac8887be1ff1c533adba4427bed8cd1339c"
+  "sha256": "44c76f45dde79375e996dfa1f3e1015c8b777a7382394baac38ea21a9a1b39fa"
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
-  "sha256": "563e9a2f5ad67ea398508a47e531581eb927c82cf41dbeaec276c0b7dd05538b"
+  "sha256": "a9d3eb365c27ec80a42d51fa241721fbddb67e1114bc8504f67ca44dbb2221a1"
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
-      "name": "Set empty string output",
+      "name": "Run echo 'empty_var=' >> $GITHUB_OUTPUT",
       "number": 2,
       "started_at": "{volatile}",
       "status": 6
@@ -23,7 +23,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Verify empty string output",
+      "name": "Run EMPTY=''",
       "number": 3,
       "started_at": "{volatile}",
       "status": 6
@@ -32,7 +32,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Test empty string comparison",
+      "name": "Run EMPTY=''",
       "number": 4,
       "started_at": "{volatile}",
       "status": 6
@@ -41,7 +41,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Test unset env var reference",
+      "name": "Run # Unset env var should be empty/null",
       "number": 5,
       "started_at": "{volatile}",
       "status": 6
@@ -50,7 +50,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Test step output that is never set",
+      "name": "Run # Intentionally don't set never_set_var",
       "number": 6,
       "started_at": "{volatile}",
       "status": 6
@@ -59,7 +59,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Access undefined step output",
+      "name": "Run UNDEFINED=''",
       "number": 7,
       "started_at": "{volatile}",
       "status": 6
@@ -68,7 +68,7 @@
       "completed_at": "{volatile}",
       "conclusion": 2,
       "external_id": "{volatile}",
-      "name": "Test empty string in matrix (simulated)",
+      "name": "Run # Simulate matrix with empty value",
       "number": 8,
       "started_at": "{volatile}",
  
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
-  "line_count": 176,
+  "line_count": 166,
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

#### occurrence 9
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 14,
+  "line_count": 11,
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
-          "v": "28907425125"
+          "v": "28912803463"
         },
         {
           "k": "run_number",
-          "v": "12"
+          "v": "20"
         },
         {
           "k": "retention_days",
@@ -839,7 +839,7 @@
     },
     {
       "type": "regex",
-      "value": "{token}\\.v1s3xM50o7yGVDb"
+      "value": "{token}\\.u0aJx6hLxFH4KYn"
     },
     {
       "type": "regex",
```

### `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -24,7 +24,7 @@
       "completed_at": "{volatile}",
       "conclusion": "succeeded",
       "external_id": "{volatile}",
-      "name": "Set empty string output",
+      "name": "Run echo 'empty_var=' >> $GITHUB_OUTPUT",
       "number": 2,
       "started_at": "{volatile}",
       "status": "completed",
@@ -36,7 +36,7 @@
       "completed_at": "{volatile}",
       "conclusion": "succeeded",
       "external_id": "{volatile}",
-      "name": "Verify empty string output",
+      "name": "Run ${{ format('EMPTY=''{0}''",
       "number": 3,
       "started_at": "{volatile}",
       "status": "completed",
@@ -48,7 +48,7 @@
       "completed_at": "{volatile}",
       "conclusion": "succeeded",
       "external_id": "{volatile}",
-      "name": "Test empty string comparison",
+      "name": "Run ${{ format('EMPTY=''{0}''",
       "number": 4,
       "started_at": "{volatile}",
       "status": "completed",
@@ -60,7 +60,7 @@
       "completed_at": "{volatile}",
       "conclusion": "succeeded",
       "external_id": "{volatile}",
-      "name": "Test unset env var reference",
+      "name": "Run # Unset env var should be empty/null",
       "number": 5,
       "started_at": "{volatile}",
       "status": "completed",
@@ -72,7 +72,7 @@
       "completed_at": "{volatile}",
       "conclusion": "succeeded",
       "external_id": "{volatile}",
-      "name": "Test step output that is never set",
+      "name": "Run # Intentionally don't set never_set_var",
       "number": 6,
       "started_at": "{volatile}",
       "status": "completed",
@@ -84,7 +84,7 @@
       "completed_at": "{volatile}",
       "conclusion": "succeeded",
       "external_id": "{volatile}",
-      "name": "Access undefined step output",
+      "name": "Run ${{ format('UNDEFINED=''{0}''",
       "number": 7,
       "started_at": "{volatile}",
       "status": "completed",
@@ -96,7 +96,7 @@
       "completed_at": "{volatile}",
       "conclusion": "succeeded",
       "external_id":
... truncated ...
```

### `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt`

#### occurrence 1
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 10933,
-  "sha256": "d96bdc029be2f1986bcc3242a70dc7221a2f467c19d77ac83a2250364e8b4dff"
+  "bytes": 8751,
+  "sha256": "2bdcbd68022d6e16ef345d50ad7326251671ef436d9e2a0076521de048cec5db"
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
-  "bytes": 976,
-  "sha256": "b9d3b74dfc2c81eda79f328aeeb844227a0cedb01d8f1b8636d0b1a7b039406f"
+  "bytes": 468,
+  "sha256": "18f259f321aa6dc53de663c205c179425c9a1ff09c9fd7559367aff8bfacc56a"
 }
```

#### occurrence 2
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 366,
-  "sha256": "83fd11d25810c35a9600e151d228b3207d6bf50f9ad0605ee28c4cf0c055c488"
+  "bytes": 339,
+  "sha256": "4621d642d67503f70e199a63e6f5b44264fc7a5b8d7159f3948ba1ed5ae9115c"
 }
```

#### occurrence 3
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 712,
-  "sha256": "732a98c696d030398782024e668b3b5d2ad81834e944d06540413f371ea9e11e"
+  "bytes": 595,
+  "sha256": "f21ab4b8dfc95d02cc278aefbde9626a32f5e28faed1da22de78688dd0db902e"
 }
```

#### occurrence 4
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1176,
-  "sha256": "e241df7141f66b010b021e11ddf7e3633a425f629d1eb9e45565f2b9afed7c6e"
+  "bytes": 950,
+  "sha256": "596413323a0c51a8e0d5deb5087987b026e9c324c44f4610bc993136d6838f33"
 }
```

#### occurrence 5
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1316,
-  "sha256": "77da8fcf29ef824ddb029636fcbc11e01808d1381a46a3bf5fce99fc0c72069c"
+  "bytes": 1075,
+  "sha256": "f30ae5ef43c5116da116e80856f1054fcde04200ab257261eb7cd2189cc84bb6"
 }
```

#### occurrence 6
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 436,
-  "sha256": "944fb978cc17b53d3b7a6af91704fc175a3e1bc40268ecbc216e67fd848dde90"
+  "bytes": 409,
+  "sha256": "e05bd39e4f2f7684ba3b9966904fabc9ccc77c301ee9ca4ac429f4452f1fe2aa"
 }
```

#### occurrence 7
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 723,
-  "sha256": "bc1ddcd3aa766f5ba773473e49e419ce4b5110e1fd2c883e5cbde810267d70a7"
+  "bytes": 606,
+  "sha256": "8f907aac7c45cf709621872cb04ee28690424ae3aa8feb7c8490602217973ee3"
 }
```

#### occurrence 8
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 863,
-  "sha256": "8ab1e1f87399dd44087222471230b673bec49acfb07da686753db784ea91ed61"
+  "bytes": 731,
+  "sha256": "1b7f0b007d72055ba7b8926fa2650bcad273af151db27921e8a88ccaad6f09ce"
 }
```

#### occurrence 9
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 858,
-  "sha256": "daf26224f8e9abc08cb1bc104d6bb01c10954f871c7d404611b0ed8d3a715e4d"
+  "bytes": 615,
+  "sha256": "8642706020b6d667d6f876859d8f2093ac0011d5c553ea4ce9f5eed6e1b77d4e"
 }
```

#### occurrence 10
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 824,
-  "sha256": "3dc6e51d08a2cda5d4e4450efb2abe258d70006758699c8931cf8c3450544d36"
+  "bytes": 692,
+  "sha256": "11b3dc76d666c5c0c156e32b71b148e9465b7ebbf2ea72d127a6de79daf7e34b"
 }
```

#### occurrence 11
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1208,
-  "sha256": "a94810e25cec9aa7bfce09284c8c3cd7ae03fd4b67a50f964f793104a61aa573"
+  "bytes": 982,
+  "sha256": "30856212822487ea214893b1edef3e3e195dd327aec9c1949e81e852a9ab3086"
 }
```

#### occurrence 12
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 339,
-  "sha256": "b5063c62394db59c8d41040978961722502a302117556636c046eac302bb7441"
+  "bytes": 316,
+  "sha256": "a1433f1776eb5a57f0a25d7710316c6092df7a58d80475414488a6ec0fcb9b72"
 }
```

#### occurrence 13
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1114,
-  "sha256": "474ab4350a11f94b032a6bb8c6c9540bfe11d14d778cb90357a3fcd1026df007"
+  "bytes": 918,
+  "sha256": "e5767180dd04173308bbfbc1aaae08b9b44e8a2801c3846b9047f8e7a13880b3"
 }
```

#### occurrence 14
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 61,
-  "sha256": "45017f1945f6ce8397591ab53ef5dc1b90faa7b4558a754d58a084cc0ea6f9cc"
+  "bytes": 53,
+  "sha256": "99c93c42f31a3cf768218b9d4679ff3ed8a28be78f64e7baffe41f83a02e804e"
 }
```

## Verdict

FAIL: 32 contract differences found.

- endpoint-sequence: 1
- request-binary: 17
- request-value: 7
- response-binary: 1
- response-schema: 1
- response-value: 4
- status: 1
