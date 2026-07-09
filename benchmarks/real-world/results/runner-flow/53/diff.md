# Runner flow diff: 53-secret-masking

- official capture: `benchmarks/real-world/results/runner-flow/53-secret-masking/official/latest`
- aksh capture: `benchmarks/real-world/results/runner-flow/53-secret-masking/aksh/latest`
- official summary: status=ok flows=54
- aksh summary: status=ok flows=54

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
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 9 | 9 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 9 | 9 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 9 | 9 |

## Endpoint sequence diff

```diff
--- official
+++ aksh
@@ -16,11 +16,11 @@
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob",
   "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "GET broker.actions.githubusercontent.com/health",
+  "GET run.actions.githubusercontent.com/health",
   "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
-  "GET broker.actions.githubusercontent.com/health",
   "GET token.actions.githubusercontent.com/ready",
-  "GET run.actions.githubusercontent.com/health",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
   "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
@@ -47,10 +47,10 @@
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
-  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/10/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
-  "messageId": 3286100653657241520,
+  "body": "{\"runner_request_id\":\"{guid}\",\"run_service_url\":\"https://{token}.actions.githubusercontent.com/35/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
+  "messageId": 6286778141453998329,
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
-      "size": 4,
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
   "bytes": 51,
-  "sha256": "e444adbac6c4f55dcd0b3a4094508345a0aed484ab859e08d38079594cb60fa7"
+  "sha256": "2ba673412eb3ca9cf3d797b4b888d9c67e848cd1cecdc370e086bca8ddd0302a"
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
-  "ownerName": "container (PID: 128)",
+  "ownerName": "container (PID: 448)",
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
-  "ownerName": "container (PID: 128)",
+  "ownerName": "container (PID: 448)",
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
-      "modulus": "{token}/{token}+su3ZjqTg7uQoQ9J/{token}/3i1ejpPszBSSZg0Al037n0H+{token}+RukLviTwP9CpCEHe4S8++{token}/ta1nf93WOQ=="
+      "modulus": "{token}+{token}+{token}+KXxrlCangSxbHER340b5LE/{token}/{token}/{token}+e9vPUk0fmN0QE4HHv+i4F4/O4I/Ost5dn0HbiHsSSr8U+vBR0w=="
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
-      "modulus": "{token}/{token}+su3ZjqTg7uQoQ9J/{token}/3i1ejpPszBSSZg0Al037n0H+{token}+RukLviTwP9CpCEHe4S8++{token}/ta1nf93WOQ=="
+      "modulus": "{token}+{token}+{token}+KXxrlCangSxbHER340b5LE/{token}/{token}/{token}+e9vPUk0fmN0QE4HHv+i4F4/O4I/Ost5dn0HbiHsSSr8U+vBR0w=="
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
-  "queueName": "taskagent-745",
+  "queueName": "taskagent-759",
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
-  "sha256": "9e49aff96413749fe6f4b3e0ed934368902578c20f9094f088fc622a3e8c1077"
+  "sha256": "195e50af1bd90179f0205940a6fe9e337ad8df2c87cf881d5bb18dccd7c37e7d"
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
-  "sha256": "67b3409a35cb5c6e3de5007b03f6e33027ff70aefab449798d20fbce829ad6a6"
+  "sha256": "7c30061ba781da5865fe213cdd6ed92853f2fc64ab77ed77d9f79c8ac33b357f"
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
-  "line_count": 96,
+  "line_count": 70,
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
-  "line_count": 11,
+  "line_count": 9,
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
-  "line_count": 16,
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
-  "line_count": 9,
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
-  "line_count": 9,
+  "line_count": 7,
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
-  "line_count": 13,
+  "line_count": 11,
   "step_backend_id": "{volatile}",
   "uploaded_at": "{time}",
   "workflow_job_run_backend_id": "{volatile}",
```

#### occurrence 7
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 11,
+  "line_count": 10,
   "step_backend_id": "{volatile}",
   "uploaded_at": "{time}",
   "workflow_job_run_backend_id": "{volatile}",
```

#### occurrence 8
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "line_count": 11,
+  "line_count": 9,
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
-          "v": "28998886809"
+          "v": "29040599029"
         },
         {
           "k": "run_number",
-          "v": "6"
+          "v": "7"
         },
         {
           "k": "retention_days",
@@ -864,7 +864,7 @@
     },
     {
       "type": "regex",
-      "value": "{token}\\.26Jnmk_9jFd-6Tn"
+      "value": "{token}\\.K6PY3Y3T_-kfEc0"
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
@@ -117,16 +117,8 @@
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
+      "message": "{\"ClassType\":\"StepsRunner\",\"FinishResult\":\"succeeded\"}",
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
-  "bytes": 6324,
-  "sha256": "b757abcbc6bb0048b10e2197ad9002357ae6a4edf495fb35494c5c9b5d7b391d"
+  "bytes": 4456,
+  "sha256": "a98033227bdf6f57e08a2ef7303645c2fca9200c85d0e278877bff63b1109e12"
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
-  "bytes": 958,
-  "sha256": "fe521bd99cdbdd802f175a069264b286d04771dbdce128456a467879c850e820"
+  "bytes": 450,
+  "sha256": "4df2631c6296972e75efbb4b72eede9d4e0c946453c35f71e59db5b321341078"
 }
```

#### occurrence 2
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 665,
-  "sha256": "7618405d6676e79c803b5960040d23d39477ede1cd237b694dfa2a4030bba822"
+  "bytes": 557,
+  "sha256": "9af1978b76e5d1ae329a7c84e07ed21a6673eff2a6f556ea0602f6e087412471"
 }
```

#### occurrence 3
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 1340,
-  "sha256": "718fa4018552cb3361a049d5ce8c1b6a3ca59517d4d59239a48f6ab9d53c26c8"
+  "bytes": 564,
+  "sha256": "ea96e1f69b47e960319ec7afb51c60786f8a840a091505d000307dcb1cba9f28"
 }
```

#### occurrence 4
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 539,
-  "sha256": "6ee3848ec7d0af4717f544879394aa9c22eda0d947010a861ed4316e0d91bc28"
+  "bytes": 450,
+  "sha256": "a0ce92dc20db1cb252ca11bf1e2b46ff15c298c29f9bf8b88dc586bc66ad4e9e"
 }
```

#### occurrence 5
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 563,
-  "sha256": "f9d93d8be0d2694fa211c34ff63d44ef3ee26c0be0f3568e393e4d5de3a22814"
+  "bytes": 474,
+  "sha256": "cce35b7562fb0149af5faf9025d3c91f18fe99f24304b55622be76c7d937b109"
 }
```

#### occurrence 6
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 690,
-  "sha256": "775cdded061c42a9852bdb97ced749489f2fcdc25f1ff4042267f8e8a01d50e8"
+  "bytes": 563,
+  "sha256": "4f658cb9794ac901581fe33badbb6e72eb69f36a67caa4b6facda6e0719d2a24"
 }
```

#### occurrence 7
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 662,
-  "sha256": "d2407a6c752fbbe253dea9114b256dc396566bcfbee6112ee2863e433ce5c24a"
+  "bytes": 605,
+  "sha256": "cf9e9fd5c243ace40d624520589767f0f14c1b37f272b5f84f4d250f10c35724"
 }
```

#### occurrence 8
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 870,
-  "sha256": "b5fa8b34984bb38ece0a0effb65a430e6b205cce555071603e845039ba20f42d"
+  "bytes": 738,
+  "sha256": "b44615ea0ae564b11fa915705442319e29dbc1bbe867d27a3f03f68a11b0121b"
 }
```

#### occurrence 9
- request binary body differs
```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "bytes": 61,
-  "sha256": "655a496f565ba292c74cb7c7b4a19ba4d8960422480aa91f2f270a0b10d93846"
+  "bytes": 53,
+  "sha256": "4b8c437d90bc2f81fb25754c3aec0b483c752d0722f13655b4525d6d08f1bacf"
 }
```

## Verdict

FAIL: 33 contract differences found.

- endpoint-sequence: 1
- request-binary: 12
- request-value: 12
- response-binary: 1
- response-schema: 1
- response-value: 5
- status: 1
