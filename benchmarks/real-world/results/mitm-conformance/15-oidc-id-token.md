# Runner flow diff: 15-oidc-id-token

- official capture: `/Users/bnjoroge/cachingv4/.runner-watch/golden/v2.335.1/15-oidc-id-token`
- aksh capture: `/Users/bnjoroge/cachingv4/experiments/mitm/captures/aksh/15-oidc-id-token/latest`
- official summary: status=ok flows=29
- aksh summary: status=None flows=39

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 0 | 1 ⚠ |
| `GET broker.actions.githubusercontent.com/health` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 0 | 1 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}` | 1 | 0 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 0 | 1 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}` | 2 | 0 ⚠ |
| `GET nodejs.org/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 ⚠ |
| `GET nodejs.org/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 6 | 6 |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 1 | 1 |
| `GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 0 | 1 ⚠ |
| `GET run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}//idtoken/{guid}/{guid}?api-version=2.0&audience=api://aksh` | 1 | 0 ⚠ |
| `GET run.actions.githubusercontent.com/health` | 1 | 1 |
| `GET token.actions.githubusercontent.com/ready` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 0 | 1 ⚠ |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 1 | 0 ⚠ |
| `POST broker.actions.githubusercontent.com/session` | 1 | 1 |
| `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents` | 1 | 1 |
| `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/oauth2/token` | 0 | 2 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 2 | 2 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 0 | 1 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 0 | 3 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 3 | 3 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `POST tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}` | 2 | 0 ⚠ |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 0 | 1 ⚠ |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 0 | 3 ⚠ |

## Endpoint sequence diff

```diff
--- official
+++ aksh
@@ -8,24 +8,34 @@
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
   "GET pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "POST tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}",
+  "GET nodejs.org/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz",
+  "GET nodejs.org/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz",
+  "POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/oauth2/token",
   "POST broker.actions.githubusercontent.com/session",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}",
-  "POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64",
-  "POST tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}",
+  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
+  "POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64",
+  "POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/oauth2/token",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}",
+  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "GET broker.actions.githubusercontent.com/health",
+  "GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
+  "GET run.actions.githubusercontent.com/health",
   "GET token.actions.githubusercontent.com/ready",
-  "GET broker.actions.githubusercontent.com/health",
-  "GET run.actions.githubusercontent.com/health",
+  "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
   "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
-  "GET run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}//idtoken/{guid}/{guid}?api-version=2.0&audience=api://aksh",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
+  "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
   "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
+  "PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt",
+  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
   "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}"
+  "DELETE broker.actions.githubusercontent.com/session"
 ]
```

## Per-flow contract differences

### `GET broker.actions.githubusercontent.com/health`

#### occurrence 1
- response schema differs
```diff
--- official
+++ aksh
@@ -1,4 +1 @@
-{
-  "bytes": "number",
-  "sha256": "string"
-}
+null
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

#### occurrence 2
- response redacted value differs
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
     "serviceOwner": "{guid}"
   }
 }
```

#### occurrence 3
- response redacted value differs
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
     "serviceOwner": "{guid}"
   }
 }
```

#### occurrence 4
- response redacted value differs
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
     "serviceOwner": "{guid}"
   }
 }
```

#### occurrence 5
- response redacted value differs
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
     "serviceOwner": "{guid}"
   }
 }
```

#### occurrence 6
- response redacted value differs
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
     "serviceOwner": "{guid}"
   }
 }
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
-      "size": 5,
+      "size": 0,
       "targetSize": null
     },
     {
@@ -22,7 +22,7 @@
       "isInternal": false,
       "name": "GitHub Actions",
       "scope": "{guid}",
-      "size": 1,
+      "size": 20,
       "targetSize": 1
     }
   ]
```

### `GET run.actions.githubusercontent.com/health`

#### occurrence 1
- response schema differs
```diff
--- official
+++ aksh
@@ -1,4 +1 @@
-{
-  "bytes": "number",
-  "sha256": "string"
-}
+null
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
-  "ownerName": "Nuraydias-Mac-Studio (PID: 49714)",
+  "ownerName": "container (PID: 3608)",
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
-  "ownerName": "Nuraydias-Mac-Studio (PID: 49714)",
+  "ownerName": "container (PID: 3608)",
   "sessionId": "{guid}"
 }
```

### `POST pipelinesghubeus24.actions.githubusercontent.com/BFN7BKzVl83fPD2KzdF2rk4xW0Zdbq5VxD0SKYZ56hyKNWR3f3/_apis/distributedtask/pools//{n}/agents`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -2,12 +2,12 @@
   "authorization": {
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "sEKqlp/{token}/{token}/{token}+MfJLiT+kls3/O9b1vR3ldlA6t98/{token}+tvXVzIckJB/lZ+AvOTU1/e6LBv6jDQH/{token}/{token}/5VnhLgScu/L/{token}=="
+      "modulus": "pLoewDsBgGVJenT//{token}+{token}+{token}+k7q4HsZY1faAr6BfD+XnXbapLthDoAkGy388+u/PBDPnUMKi7ZMpaDoGJJMu7D+{token}/O6ESO2Wby32aZjiXksu4fY/Un04F8MaUhai/VZQ=="
     }
   },
   "createdOn": "{time}",
-  "disableUpdate": false,
-  "ephemeral": false,
+  "disableUpdate": true,
+  "ephemeral": true,
   "id": "{volatile}",
   "labels": [
     {
@@ -17,7 +17,7 @@
     },
     {
       "id": "{volatile}",
-      "name": "macOS",
+      "name": "Linux",
       "type": "system"
     },
     {
@@ -27,13 +27,28 @@
     },
     {
       "id": "{volatile}",
+      "name": "self-hosted",
+      "type": "user"
+    },
+    {
+      "id": "{volatile}",
+      "name": "linux",
+      "type": "user"
+    },
+    {
+      "id": "{volatile}",
+      "name": "x64",
+      "type": "user"
+    },
+    {
+      "id": "{volatile}",
       "name": "mitm",
       "type": "user"
     }
   ],
   "maxParallelism": 1,
   "name": "{token}",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "osDescription": "linux aarch64",
   "provisioningState": "Provisioned",
   "status": 0,
   "version": "2.335.1"
```
- response redacted value differs
```diff
--- official
+++ aksh
@@ -1,17 +1,17 @@
 {
   "authorization": {
-    "authorizationUrl": "https://tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}",
+    "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/{token}/_apis/oauth2/token",
     "clientId": "{guid}",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "sEKqlp/{token}/{token}/{token}+MfJLiT+kls3/O9b1vR3ldlA6t98/{token}+tvXVzIckJB/lZ+AvOTU1/e6LBv6jDQH/{token}/{token}/5VnhLgScu/L/{token}=="
+      "modulus": "pLoewDsBgGVJenT//{token}+{token}+{token}+k7q4HsZY1faAr6BfD+XnXbapLthDoAkGy388+u/PBDPnUMKi7ZMpaDoGJJMu7D+{token}/O6ESO2Wby32aZjiXksu4fY/Un04F8MaUhai/VZQ=="
     }
   },
   "createdOn": "{time}",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
-  "ephemeral": false,
+  "ephemeral": true,
   "id": "{volatile}",
   "isElastic": false,
   "isVirtual": false,
@@ -23,7 +23,7 @@
     },
     {
       "id": "{volatile}",
-      "name": "macOS",
+      "name": "Linux",
       "type": "system"
     },
     {
@@ -33,13 +33,18 @@
     },
     {
       "id": "{volatile}",
+      "name": "x64",
+      "type": "user"
+    },
+    {
+      "id": "{volatile}",
       "name": "mitm",
       "type": "user"
     }
   ],
   "maxParallelism": 1,
   "name": "{token}",
-  "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
+  "osDescription": "linux aarch64",
   "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
@@ -60,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-12",
+  "queueName": "taskagent-689",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
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
-      "conclusion": 2,
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

### `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob`

#### occurrence 1
- request redacted value differs
```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "billingOwnerId": "O_kgDOEbddog",
   "jobMessageId": "{guid}",
-  "runnerOS": "macOS"
+  "runnerOS": "Linux"
 }
```
- response redacted value differs
```diff
--- official
+++ aksh
@@ -29,11 +29,11 @@
         },
         {
           "k": "run_id",
-          "v": "28458434874"
+          "v": "28972783313"
         },
         {
           "k": "run_number",
-          "v": "1"
+          "v": "14"
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
@@ -839,11 +839,7 @@
     },
     {
       "type": "regex",
-      "value": "{token}"
-    },
-    {
-      "type": "regex",
-      "value": "{token}\\.{token}"
+      "value": "{token}\\.10edUboo6sBsBDh"
     },
     {
       "type": "regex",
@@ -866,7 +862,7 @@
           "CacheServerUrl": "https://artifactcache.actions.githubusercontent.com/{token}/",
           "ConnectivityChecks": "[\"https://broker.actions.githubusercontent.com/health\",\"https://to
... truncated ...
```

### `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob`

#### occurrence 1
- request schema differs
```diff
--- official
+++ aksh
@@ -17,6 +17,26 @@
       "started_at": "string",
       "status": "string",
       "type": "string"
+    },
+    {
+      "action_name": "string",
+      "annotations": [
+        {
+          "endLine": "number",
+          "level": "string",
+          "message": "string",
+          "startLine": "number",
+          "stepNumber": "number"
+        }
+      ],
+      "completed_at": "string",
+      "conclusion": "string",
+      "external_id": "string",
+      "name": "string",
+      "number": "number",
+      "started_at": "string",
+      "status": "string",
+      "type": "string"
     }
   ],
   "telemetry": [
```

## Verdict

FAIL: 19 contract differences found.

- endpoint-sequence: 1
- request-schema: 2
- request-value: 4
- response-schema: 3
- response-value: 9
