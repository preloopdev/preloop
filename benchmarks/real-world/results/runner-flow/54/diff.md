# Runner flow diff: 54-job-annotations

- official capture: `benchmarks/real-world/results/runner-flow/54-job-annotations/official/latest`
- aksh capture: `benchmarks/real-world/results/runner-flow/54-job-annotations/aksh/latest`
- official summary: status=ok flows=39
- aksh summary: status=ok flows=39

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
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 4 | 4 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 4 | 4 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 4 | 4 |

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
@@ -16,11 +16,11 @@
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob",
   "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
   "GET broker.actions.githubusercontent.com/health",
   "GET token.actions.githubusercontent.com/ready",
   "GET run.actions.githubusercontent.com/health",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
@@ -32,10 +32,10 @@
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
-  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/179/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
-  "messageId": 3518029983685218592,
+  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/198/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
+  "messageId": 7226054016319490618,
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
-      "size": 3,
+      "size": 6,
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
-  "bytes": 50,
-  "sha256": "75b40eb0403e0b1469430bab30d2a024e94c39086611f9034c788bfc77e48063"
+  "bytes": 51,
+  "sha256": "55b8f27e58ad260affee9858fe806df4ae5fcd44512119ddcd192a723443a7da"
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
-  "ownerName": "container (PID: 4012)",
+  "ownerName": "container (PID: 690)",
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
-  "ownerName": "container (PID: 4012)",
+  "ownerName": "container (PID: 690)",
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
-      "modulus": "{token}/d8Sm4vGZyKiqX+GnPVmx/f5TBgZ/cl2wMvhwp/{token}+WVoFY+{token}+{token}/{token}+{token}+{token}/EHPfIyml4hV8EvLxqAxMJt+EWweKbd1GAKYNNPMw=="
+      "modulus": "{token}/udfIyO85nMQKd/9k/{token}+{token}/lZ14MaGia10YDd+WXnz51soYV3Z++{token}/{token}/{token}+UfdOhHd+{token}=="
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
-      "modulus": "{token}/d8Sm4vGZyKiqX+GnPVmx/f5TBgZ/cl2wMvhwp/{token}+WVoFY+{token}+{token}/{token}+{token}+{token}/EHPfIyml4hV8EvLxqAxMJt+EWweKbd1GAKYNNPMw=="
+      "modulus": "{token}/udfIyO85nMQKd/9k/{token}+{token}/lZ14MaGia10YDd+WXnz51soYV3Z++{token}/{token}/{token}+UfdOhHd+{token}=="
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
-  "queueName": "taskagent-764",
+  "queueName": "taskagent-763",
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
-  "sha256": "4ccf310158cbf54da92d9951c4b1e061cdf7034362609c19c114d41a03a2db69"
+  "sha256": "97a3201101cd0c6b74d6ebc7364e5bd378727c8b810446c7c4236009e9acb648"
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
-  "sha256": "b6cffd4134bdfdc1826e4a39520e0f2db452a096817b949241efb2429667af8a"
+  "sha256": "73ff7a9b02f6c8413f9e36033bb5bd56f06c1fcc5c5d222343d9361a34f47d8c"
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
-  "line_count": 32,
+  "line_count": 25,
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
-          "v": "29041170222"
+          "v": "29041001513"
         },
         {
           "k": "run_number",
-          "v": "5"
+          "v": "4"
         },
         {
           "k": "retention_days",
@@ -839,7 +839,7 @@
     },
     {
       "type": "regex",
-      "value": "{token}\\.yryuK3zQYL0Gxmp"
+      "value": "{token}\\.R8NMNvYvXxMJMTL"
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
@@ -17,27 +17,6 @@
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
-          "stepNumber": "number",
-          "title": "string"
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
-  "bytes": 2461,
-  "sha256": "f1989bdd4b494c9264fc90570bf8f943174a09613c35211f3b7ad7bbb85ec05c"
+  "bytes": 1929,
+  "sha256": "c2a09918d8f26b18105f868c700119ce824ab8ccf217d9cb4345168f8467f5e4"
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
-  "bytes": 965,
-  "sha256": "f2048e62030dddf92ab7947dc58173840bcb0d83ee83b45bf70b4a567f45cc66"
+  "bytes": 457,
+  "sha256": "63efa3407112dbd1767006519b8a30461377697cc0dd47c3cd27dd9fdbb91529"
 }
```

#### occurrence 2
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 947,
-  "sha256": "1cd4448e0e987b3f8d6fe5bcb5b5e4ebd1a859a7c6d2d31e03144f24dc1e461d"
+  "bytes": 932,
+  "sha256": "9e504997412a400aded021e497f2c1efaae052c07de005a8fd05304e688c44fd"
 }
```

#### occurrence 3
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 497,
-  "sha256": "9206d97da629dec422ab86cfa0cf122580dcfab923fa9a51601a82a39366449f"
+  "bytes": 485,
+  "sha256": "99559b02a10bab62c8192b64800588d39077f38d9661fa762d8c9045731ff64b"
 }
```

#### occurrence 4
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 61,
-  "sha256": "28a6d5cbfface834df507f08811f7dc5c079bb5e6b2db69491942450be87c85b"
+  "bytes": 53,
+  "sha256": "1d04bd95e60ef07019c96d57ab727001ddce3ec4d4753d0b7218b3725cfc6439"
 }
```

## Verdict

FAIL: 21 contract differences found.

- endpoint-sequence: 1
- request-binary: 7
- request-schema: 1
- request-value: 4
- response-binary: 1
- response-schema: 1
- response-value: 5
- status: 1
