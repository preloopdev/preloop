# Runner flow diff: 56-problem-matcher-frompath

- official capture: `benchmarks/real-world/results/runner-flow/56-problem-matcher-frompath/official/latest`
- aksh capture: `benchmarks/real-world/results/runner-flow/56-problem-matcher-frompath/aksh/latest`
- official summary: status=ok flows=45
- aksh summary: status=ok flows=45

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/health` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 1 |
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
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 6 | 6 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 6 | 6 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 6 | 6 |

## Endpoint sequence diff

```diff
--- official
+++ aksh
@@ -17,9 +17,9 @@
   "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
   "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
+  "GET run.actions.githubusercontent.com/health",
   "GET token.actions.githubusercontent.com/ready",
   "GET broker.actions.githubusercontent.com/health",
-  "GET run.actions.githubusercontent.com/health",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
@@ -38,10 +38,10 @@
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
-  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/101/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
-  "messageId": 9106779870904058723,
+  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/115/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
+  "messageId": 8421365366866092385,
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
+      "size": 4,
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
-  "bytes": 49,
-  "sha256": "1bf3b0d3c2d61579063fa80d1215b20fd675d053d428a6906f054aafbe682e3d"
+  "bytes": 50,
+  "sha256": "2d1a57d3dc05384fc1d84188339d336ae6f8610dde81a90d77d778199fc78c62"
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
-  "ownerName": "container (PID: 4185)",
+  "ownerName": "container (PID: 406)",
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
-  "ownerName": "container (PID: 4185)",
+  "ownerName": "container (PID: 406)",
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
-      "modulus": "rqZMqokz1h/KiwqDZGpg+{token}/{token}/xN5GS5H7Krupq0q9ITYgc/{token}/HBeOwor4tJmUSCK0Ut9/pTkHWlkHIws5Xsne5xEA2+{token}+h8lsWSunT/HHGSw=="
+      "modulus": "zHMj+/{token}+{token}/SPoHzXmm7WZI5P3bMM1/UxCGtZnCfHjGvL/1DBVXzR3HZU313t/a/FDBwomNAZcLR/{token}+KXrSgv/{token}+{token}+vQ=="
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
-      "modulus": "rqZMqokz1h/KiwqDZGpg+{token}/{token}/xN5GS5H7Krupq0q9ITYgc/{token}/HBeOwor4tJmUSCK0Ut9/pTkHWlkHIws5Xsne5xEA2+{token}+h8lsWSunT/HHGSw=="
+      "modulus": "zHMj+/{token}+{token}/SPoHzXmm7WZI5P3bMM1/UxCGtZnCfHjGvL/1DBVXzR3HZU313t/a/FDBwomNAZcLR/{token}+KXrSgv/{token}+{token}+vQ=="
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
-  "queueName": "taskagent-765",
+  "queueName": "taskagent-761",
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
-  "sha256": "be37628a1a3fb2d797d19c63ff3dde3c4b88cbe8cf032153aeaedb068e61a98e"
+  "sha256": "d5c343e8205c965f01c4a5ce466ec8db5d162851441949d8bcbeb1e835f63e33"
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
-  "sha256": "7eeec5b048a485762c3aa721acefd470a455526c774533b243bfca063a37196d"
+  "sha256": "eacefc7f8bbc6fda46d8144dea2add0dff91db3c61ce60c04fddbbbbdb9eb2e9"
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
-  "line_count": 64,
+  "line_count": 59,
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

#### occurrence 3
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 6,
+  "line_count": 7,
   "step_backend_id": "{volatile}",
   "uploaded_at": "{time}",
   "workflow_job_run_backend_id": "{volatile}",
```

#### occurrence 5
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 6,
+  "line_count": 7,
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
-          "v": "29041222853"
+          "v": "29041000017"
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
-      "value": "{token}\\.UA7_I8XuCJzkTix"
+      "value": "{token}\\.JJFZWV8D9MD_Io9"
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
@@ -17,28 +17,6 @@
       "started_at": "string",
       "status": "string",
       "type": "string"
-    },
-    {
-      "action_name": "string",
-      "annotations": [
-        {
-          "endColumn": "number",
-          "endLine": "number",
-          "level": "string",
-          "message": "string",
-          "startColumn": "number",
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
-  "bytes": 4173,
-  "sha256": "01dd86ed81512b303e4200433603934f7a65b29bbf08265221f03e3f8cceae0b"
+  "bytes": 3344,
+  "sha256": "0170ed717bbcb74a86ff3b2396919f8d4b38d647f0dab169307b6b90b12353b6"
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
-  "bytes": 973,
-  "sha256": "5461ede84253edba9484e5531dec440d79339bbee7cf01aa652774f9fdaf538c"
+  "bytes": 465,
+  "sha256": "bc1eeab53bc468584944580928ec418b726f032f9b3e89a6a1f7ce227068a736"
 }
```

#### occurrence 2
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1538,
-  "sha256": "f27c202927aa81ae81340fc6e46b216c50518967b72a82c0a2f5ac2ce861f1a9"
+  "bytes": 1211,
+  "sha256": "80b8ee619f32b72b2d60f21293565e5c687af02372000b4b154756094a60d970"
 }
```

#### occurrence 3
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 406,
-  "sha256": "a52953e783f6e3a903b5b66be178196cec8439d21507d2973ab52834e0bebd69"
+  "bytes": 451,
+  "sha256": "90b3cce8c3136985aee4303ee083871fbb87429e33b4cfa1243707b788374c22"
 }
```

#### occurrence 4
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 814,
-  "sha256": "50617e3314e9f3cd088cc0390f0ad036b8b9b0fb77cb2572413d34888b705a51"
+  "bytes": 729,
+  "sha256": "00771ef871adf6fbdf52902af515b96e173cf15d900df2aa560e52a21ceafeb9"
 }
```

#### occurrence 5
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 396,
-  "sha256": "a032178eda4080d585a8cbaafacaf52138c0ca3a55051fd9a9d5e2f1773d3876"
+  "bytes": 433,
+  "sha256": "89d4d5df08a5c0fe14a2dc8cad9b31593526d71914f8a3c0696a38915b2239e0"
 }
```

#### occurrence 6
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 61,
-  "sha256": "c45748c3416dcec7da88c4bada24170cfeef9c17d5c5d48f46076953110b95dc"
+  "bytes": 53,
+  "sha256": "cda9cea236795eb4c2b021e262012cee97493f3d1fbaf36d2e61011bc6b4a9d9"
 }
```

## Verdict

FAIL: 25 contract differences found.

- endpoint-sequence: 1
- request-binary: 9
- request-schema: 1
- request-value: 6
- response-binary: 1
- response-schema: 1
- response-value: 5
- status: 1
