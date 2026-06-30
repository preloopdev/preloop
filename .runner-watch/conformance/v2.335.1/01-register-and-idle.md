# MITM comparison: 01-register-and-idle

**official**: captured — 52 flows
**aksh**: captured — 52 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1` | 3 | 3 | 78.2 | 1.3 | 200, 200, 200 | 200, 200, 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 3 | 3 | 37.0 | 0.2 | 200, 200, 200 | 200, 200, 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=mitm-official&includeCapabilities=False` | 1 | 1 | 26.7 | 0.3 | 200 | 200 |
| GET | `/_apis/distributedtask/pools/{n}/messages?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false&waitSeconds={n}` | 4 | 4 | 190.3 | 0.3 | 200, 200, 200, 200 | 200, 200, 200, 200 |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 21.7 | 0.3 | 200 | 200 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 179.3 | 0.4 | 200 | 200 |
| POST | `/_apis/distributedtask/pools/{n}/sessions` | 1 | 1 | 31.1 | 0.3 | 201 | 201 |
| POST | `/_apis/v1/AgentRequest/{n}/{n}?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 4 | 4 | 41.6 | 0.2 | 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/_apis/v1/oauth2/token` | 5 | 5 | 25.2 | 0.2 | 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| POST | `/api/v3/actions/runner-registration` | 1 | 1 | 300.4 | 0.4 | 200 | 200 |
| POST | `/broker/{n}/acquirejob` | 4 | 4 | 614.1 | 0.3 | 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/broker/{n}/completejob` | 3 | 3 | 40.6 | 0.2 | 204, 204, 204 | 204, 204, 204 |
| POST | `/broker/{n}/renewjob` | 4 | 4 | 43.8 | 0.2 | 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 3 | 3 | 152.2 | 0.2 | 200, 200, 200 | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 3 | 3 | 29.1 | 0.2 | 200, 200, 200 | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 11 | 11 | 43.3 | 0.2 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |

## Missing endpoints

_No endpoints present only in official._

_No endpoints present only in aksh._

## Per-endpoint comparison

### `GET /_apis/connectionData?connectOptions={n}&lastChangeId=-1&lastChangeId64=-1`

**Header key differences:**

- official only: `{'x-vss-senderdeploymentid', 'pragma', 'activityid', 'x-tfs-processid', 'cache-control', 'strict-transport-security'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,885 +1,430 @@
 {
-  "deploymentId": "fc1cfc90-fc40-edcb-6d0f-3b1380ee7b68",
-  "deploymentType": "hosted",
-  "instanceId": "9f1fe989-7d0d-4a9b-a9bf-11330ab257c1",
+  "deploymentId": "00000000-0000-0000-0000-000000000000",
+  "deploymentType": "selfHosted",
+  "instanceId": "01b920b8-59bd-4040-acec-cca5e507e6b6",
   "locationServiceData": {
     "accessMappings": [
       {
-        "accessPoint": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1",
-        "displayName": "Host Guid Access Mapping",
-        "moniker": "HostGuidAccessMapping",
-        "serviceOwner": "00000000-0000-0000-0000-000000000000"
-      },
-      {
-        "accessPoint": "https://pipelines.actions.githubusercontent.com/***REDACTED***/",
+        "accessPoint": "http://127.0.0.1:9090",
         "displayName": "Public Access Mapping",
         "moniker": "PublicAccessMapping",
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "virtualDirectory": ""
       },
       {
-        "accessPoint": "https://pipelinesghubeus7aks.eastus.cloudapp.azure.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1",
-        "displayName": "Azure Instance Mapping",
-        "moniker": "AzureInstanceMapping",
-        "serviceOwner": "00000000-0000-0000-0000-000000000000"
-      },
-      {
-        "accessPoint": "https://pipelines.actions.githubusercontent.com/***REDACTED***/",
-        "displayName": "Codex Access Mapping",
-        "moniker": "CodexAccessMapping",
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "virtualDirectory": ""
-      },
-      {
-        "accessPoint": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/",
+        "accessPoint": "http://127.0.0.1:9090/runner/server",
         "displayName": "Scale Unit Access Mapping",
         "moniker": "ScaleUnitMapping",
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "virtualDirectory": ""
       }
     ],
-    "defaultAccessMappingMoniker": "ScaleUnitMapping",
-    "lastChangeId": 4952065,
-    "lastChangeId64": 4952065,
+    "defaultAccessMappingMoniker": "PublicAccessMapping",
+    "lastChangeId": 1,
+    "lastChangeId64": 1,
     "serviceDefinitions": [
       {
-        "description": "Location Service for GitHub Actions Server.",
+        "description": "Location Service",
         "displayName": "Location Service",
         "identifier": "9f1fe989-7d0d-4a9b-a9bf-11330ab257c1",
         "locationMappings": [
           {
             "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/***REDACTED***/"
+            "location": "http://127.0.0.1:9090"
           },
           {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
-            "accessMappingMoniker": "AzureInstanceMapping",
-            "location": "https://pipelinesghubeus7aks.eastus.cloudapp.azure.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
             "accessMappingMoniker": "ScaleUnitMapping",
-            "location": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
+            "location": "http://127.0.0.1:9090"
           }
         ],
-        "properties": {
-          "Microsoft.TeamFoundation.Location.CollectionName": {
-            "$type": "System.String",
-            "$value": "***REDACTED***"
-          }
-        },
+        "properties": {},
         "relativeToSetting": "fullyQualified",
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "LocationService2",
         "toolId": "Framework"
       },
       {
-        "description": "Location Service for GitHub Actions Server.",
-        "displayName": "Location Service",
-        "identifier": "8d299418-9467-402b-a171-9165e2f703e2",
-        "locationMappings": [],
-        "properties": {},
-        "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "LocationService2",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Area",
+        "description": "distributedtask",
         "displayName": "distributedtask",
         "identifier": "a85b8835-c1a1-4aac-ae97-1c3d0ba72dbd",
         "locationMappings": [
           {
             "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/***REDACTED***/"
+            "location": "http://127.0.0.1:9090/runner/server"
           },
           {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
             "accessMappingMoniker": "ScaleUnitMapping",
-            "location": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
+            "location": "http://127.0.0.1:9090/runner/server"
           }
         ],
-        "parentIdentifier": "0000005a-0000-8888-8000-000000000000",
-        "parentServiceType": "LocationService2",
         "properties": {},
         "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-8888-8000-000000000000",
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "LocationService2",
         "toolId": "Framework"
       },
       {
-        "description": "Resource Area",
-        "displayName": "build",
-        "identifier": "5d6898bb-45ec-463f-95f9-54d49c71752e",
-        "locationMappings": [
-          {
-            "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/***REDACTED***/"
-          },
-          {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
-            "accessMappingMoniker": "ScaleUnitMapping",
-            "location": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
-          }
-        ],
-        "parentIdentifier": "0000005a-0000-8888-8000-000000000000",
-        "parentServiceType": "LocationService2",
-        "properties": {},
-        "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-8888-8000-000000000000",
-        "serviceType": "LocationService2",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Area",
-        "displayName": "runtime",
-        "identifier": "d5366ebe-2295-4205-984e-62916e51f1eb",
-        "locationMappings": [
-          {
-            "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/***REDACTED***/"
-          },
-          {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
-            "accessMappingMoniker": "ScaleUnitMapping",
-            "location": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
-          }
-        ],
-        "parentIdentifier": "0000005a-0000-8888-8000-000000000000",
-        "parentServiceType": "LocationService2",
-        "properties": {},
-        "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-8888-8000-000000000000",
-        "serviceType": "LocationService2",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Area",
-        "displayName": "PipelinesChecks",
-        "identifier": "4a933897-0488-45af-bd82-6fd3ad33f46a",
-        "locationMappings": [
-          {
-            "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/***REDACTED***/"
-          },
-          {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
-            "accessMappingMoniker": "ScaleUnitMapping",
-            "location": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
-          }
-        ],
-        "parentIdentifier": "0000005a-0000-8888-8000-000000000000",
-        "parentServiceType": "LocationService2",
-        "properties": {},
-        "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-8888-8000-000000000000",
-        "serviceType": "LocationService2",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Area",
-        "displayName": "Build",
-        "identifier": "965220d5-5bb9-42cf-8d67-9b146df2a5a4",
-        "locationMappings": [
-          {
-            "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/***REDACTED***/"
-          },
-          {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
-            "accessMappingMoniker": "ScaleUnitMapping",
-            "location": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
-          }
-        ],
-        "parentIdentifier": "0000005a-0000-8888-8000-000000000000",
-        "parentServiceType": "LocationService2",
-        "properties": {},
-        "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-8888-8000-000000000000",
-        "serviceType": "LocationService2",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Area",
+        "description": "pipelines",
         "displayName": "pipelines",
         "identifier": "2e0bf237-8973-4ec9-a581-9c3d679d1776",
         "locationMappings": [
           {
             "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/***REDACTED***/"
+            "location": "http://127.0.0.1:9090"
           },
           {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
             "accessMappingMoniker": "ScaleUnitMapping",
-            "location": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
+            "location": "http://127.0.0.1:9090"
           }
         ],
-        "parentIdentifier": "0000005a-0000-8888-8000-000000000000",
-        "parentServiceType": "LocationService2",
         "properties": {},
         "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-8888-8000-000000000000",
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "LocationService2",
         "toolId": "Framework"
       },
       {
-        "description": "Resource Area",
-        "displayName": "runner",
-        "identifier": "73f6b305-6840-4983-b200-d72ccece0013",
-        "locationMappings": [
-          {
-            "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://runner.actions.githubusercontent.com/***REDACTED***/"
-          },
-          {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://runner.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          }
-        ],
-        "parentIdentifier": "0000006f-0000-8888-8000-000000000000",
-        "parentServiceType": "LocationService2",
-        "properties": {},
-        "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-8888-8000-000000000000",
-        "serviceType": "LocationService2",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Area",
+        "description": "oauth2",
         "displayName": "oauth2",
         "identifier": "a7b3b527-4f4f-4dac-8e84-f144fa6d554b",
         "locationMappings": [
           {
             "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/***REDACTED***/"
+            "location": "http://127.0.0.1:9090/runner/server"
           },
           {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
             "accessMappingMoniker": "ScaleUnitMapping",
-            "location": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
+            "location": "http://127.0.0.1:9090/runner/server"
           }
         ],
-        "parentIdentifier": "0000005a-0000-8888-8000-000000000000",
-        "parentServiceType": "LocationService2",
         "properties": {},
         "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-8888-8000-000000000000",
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "LocationService2",
         "toolId": "Framework"
       },
       {
-        "description": "Resource Area",
-        "displayName": "actions",
-        "identifier": "1644b0a3-b109-43d6-a0e7-f20d9dfb7508",
-        "locationMappings": [
-          {
-            "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/***REDACTED***/"
-          },
-          {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
-            "accessMappingMoniker": "ScaleUnitMapping",
-            "location": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
-          }
-        ],
-        "parentIdentifier": "0000005a-0000-8888-8000-000000000000",
-        "parentServiceType": "LocationService2",
-        "properties": {},
-        "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-8888-8000-000000000000",
-        "serviceType": "LocationService2",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Area",
-        "displayName": "serviceendpoint",
-        "identifier": "1814ab31-2f4f-4a9f-8761-f4d77dc5a5d7",
-        "locationMappings": [
-          {
-            "accessMappingMoniker": "PublicAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/***REDACTED***/"
-          },
-          {
-            "accessMappingMoniker": "HostGuidAccessMapping",
-            "location": "https://pipelines.actions.githubusercontent.com/serviceHosts/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1"
-          },
-          {
-            "accessMappingMoniker": "ScaleUnitMapping",
-            "location": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
-          }
-        ],
-        "parentIdentifier": "0000005a-0000-8888-8000-000000000000",
-        "parentServiceType": "LocationService2",
-        "properties": {},
-        "relativeToSetting": "fullyQualified",
-        "serviceOwner": "00000000-0000-8888-8000-000000000000",
-        "serviceType": "LocationService2",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Location Service for GitHub Actions Server.",
-        "displayName": "Location Service",
-        "identifier": "464ccb8d-abaf-4793-b927-cfdc107791ee",
-        "locationMappings": [],
-        "properties": {},
-        "relativePath": "/",
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "LocationService2",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "gates",
-        "identifier": "beb126ff-77cd-4476-83ff-877b31fab2b0",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}/{gateId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "actions",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "Containers",
+        "description": "AgentPools",
+        "displayName": "AgentPools",
+        "identifier": "a8c47e17-4d56-4a56-92bb-de7ea7dc65be",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/AgentPools",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "AgentPools",
+        "status": 1,
+        "toolId": "AgentPools"
+      },
+      {
+        "description": "Agent",
+        "displayName": "Agent",
+        "identifier": "e298ef32-5878-4cab-993c-043836571f42",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/Agent/{poolId}/{agentId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "Agent",
+        "status": 1,
+        "toolId": "Agent"
+      },
+      {
+        "description": "AgentSession",
+        "displayName": "AgentSession",
+        "identifier": "134e239e-2df3-4794-a6f6-24f1f19ec8dc",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/AgentSession/{poolId}/{sessionId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "AgentSession",
+        "status": 1,
+        "toolId": "AgentSession"
+      },
+      {
+        "description": "Message",
+        "displayName": "Message",
+        "identifier": "c3a054f6-7a8a-49c0-944e-3a8e5d7adfd7",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/Message/{poolId}/{messageId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "Message",
+        "status": 1,
+        "toolId": "Message"
+      },
+      {
+        "description": "AgentRequest",
+        "displayName": "AgentRequest",
+        "identifier": "fc825784-c92a-4299-9221-998a02d1b54f",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/AgentRequest/{poolId}/{requestId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "AgentRequest",
+        "status": 1,
+        "toolId": "AgentRequest"
+      },
+      {
+        "description": "ActionDownloadInfo",
+        "displayName": "ActionDownloadInfo",
+        "identifier": "27d7f831-88c1-4719-8ca1-6a061dad90eb",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/ActionDownloadInfo/{scopeIdentifier}/{hubName}/{planId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "ActionDownloadInfo",
+        "status": 1,
+        "toolId": "ActionDownloadInfo"
+      },
+      {
+        "description": "TimeLineWebConsoleLog",
+        "displayName": "TimeLineWebConsoleLog",
+        "identifier": "858983e4-19bd-4c5e-864c-507b59b58b12",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/TimeLineWebConsoleLog/{scopeIdentifier}/{hubName}/{planId}/{timelineId}/{recordId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "TimeLineWebConsoleLog",
+        "status": 1,
+        "toolId": "TimeLineWebConsoleLog"
+      },
+      {
+        "description": "TimelineRecords",
+        "displayName": "TimelineRecords",
+        "identifier": "8893bc5b-35b2-4be7-83cb-99e683551db4",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/{timelineId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "TimelineRecords",
+        "status": 1,
+        "toolId": "TimelineRecords"
+      },
+      {
+        "description": "Logfiles",
+        "displayName": "Logfiles",
+        "identifier": "46f5667d-263a-4684-91b1-dff7fdcf64e2",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/Logfiles/{scopeIdentifier}/{hubName}/{planId}/{logId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "Logfiles",
+        "status": 1,
+        "toolId": "Logfiles"
+      },
+      {
+        "description": "FinishJob",
+        "displayName": "FinishJob",
+        "identifier": "557624af-b29e-4c20-8ab0-0399d2204f3f",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/FinishJob/{scopeIdentifier}/{hubName}/{planId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "FinishJob",
+        "status": 1,
+        "toolId": "FinishJob"
+      },
+      {
+        "description": "Artifact",
+        "displayName": "Artifact",
+        "identifier": "85023071-bd5e-4438-89b0-2a5bf362a19d",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/pipelines/workflows/{runId}/artifacts",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "Artifact",
+        "status": 1,
+        "toolId": "Artifact"
+      },
+      {
+        "description": "ArtifactFileContainer",
+        "displayName": "ArtifactFileContainer",
         "identifier": "e4f5c81e-e250-447b-9fef-bd48471bea5e",
         "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/resources/{resource}/{containerId}/{*itemPath}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 4,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "Container",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "Containers",
-        "identifier": "e71a64ac-b2b5-4230-a4c0-dad657cf97e2",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.1",
-        "properties": {},
-        "relativePath": "_apis/{resource}/{container}/{*itemPath}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 3,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "Container",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "enterpriseaccesspolicies",
-        "identifier": "9f904df8-b9f4-11ed-afa1-0242ac120002",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "events",
-        "identifier": "557624af-b29e-4c20-8ab0-0399d2204f3f",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.0",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/plans/{planId}/{resource}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "agents",
-        "identifier": "e298ef32-5878-4cab-993c-043836571f42",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/pools/{poolId}/{resource}/{agentId}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 2,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "agentCloudRequestMessages",
-        "identifier": "bd247656-4d13-49af-80c1-1891bb057a93",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/agentclouds/{agentCloudId}/requests/{agentCloudRequestId}/messages",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "jitconfig",
-        "identifier": "3ecd9bbb-1cc8-4817-9e57-20e4a3dbf6a2",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/agents/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "sessions",
-        "identifier": "134e239e-2df3-4794-a6f6-24f1f19ec8dc",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/pools/{poolId}/{resource}/{sessionId}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "timelines",
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/pipelines/workflows/container/{containerId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "ArtifactFileContainer",
+        "status": 1,
+        "toolId": "ArtifactFileContainer"
+      },
+      {
+        "description": "TimelineAttachments",
+        "displayName": "TimelineAttachments",
+        "identifier": "7898f959-9cdf-4096-b29e-7f293031629e",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/{timelineId}/attachments/{recordId}/{type}/{name}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "TimelineAttachments",
+        "status": 1,
+        "toolId": "TimelineAttachments"
+      },
+      {
+        "description": "Timeline",
+        "displayName": "Timeline",
         "identifier": "83597576-cc2c-453c-bea6-2882ae6a1653",
         "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.0",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/plans/{planId}/{resource}/{timelineId}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "feedstream",
-        "identifier": "be5e691c-1592-40d4-a039-2fee0e7cc6b8",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/plans/{planId}/timelines/{timelineId}/records/{recordId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "packagedownload",
-        "identifier": "af19090b-d86c-4bcf-80fe-30444d639087",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}/{packageType}/{platform}/{version}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "attachments",
-        "identifier": "eb55e5d6-2f30-4295-b5ed-38da50b1fc52",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.1",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/plans/{planId}/{resource}/{type}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "concurrencylimitoverrides",
-        "identifier": "7c8b5d3e-4f2a-4e9b-8c1d-3a5b7e9f1c2d",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "messages",
-        "identifier": "c3a054f6-7a8a-49c0-944e-3a8e5d7adfd7",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/pools/{poolId}/{resource}/{messageId}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "plans",
-        "identifier": "5cecd946-d704-471e-a45f-3b4064fcfaba",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.0",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/{resource}/{planId}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 2,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "accesspolicy",
-        "identifier": "a60d0d28-8e2f-4ce2-bbaf-471bf3bf0bfc",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/pools/{poolId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "timelines",
-        "identifier": "ffe38397-3a9d-4ca6-b06d-49303f287ba5",
-        "locationMappings": [],
-        "maxVersion": "1.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/plans/{planId}/{resource}/{timelineId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "updates",
-        "identifier": "8cc1b02b-ae49-4516-b5ad-4f9b29967c30",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "3.2",
-        "properties": {},
-        "relativePath": "_apis/{area}/pools/{poolId}/agents/{agentId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "feed",
-        "identifier": "858983e4-19bd-4c5e-864c-507b59b58b12",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.0",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/plans/{planId}/timelines/{timelineId}/records/{recordId}/{resource}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/timeline/{timelineId}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "Timeline",
+        "status": 1,
+        "toolId": "Timeline"
+      },
+      {
+        "description": "CustomerIntelligence",
+        "displayName": "CustomerIntelligence",
+        "identifier": "b5cc35c2-ff2b-491d-a085-24b6e9f396fd",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/tasks",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "CustomerIntelligence",
+        "status": 1,
+        "toolId": "CustomerIntelligence"
+      },
+      {
+        "description": "Tasks",
+        "displayName": "Tasks",
+        "identifier": "60aac929-f0cd-4bc8-9ce4-6b30e8f1b1bd",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "/_apis/v1/tasks/{taskId}/{versionString}",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "Tasks",
+        "status": 1,
+        "toolId": "Tasks"
+      },
+      {
+        "description": "Cache",
+        "displayName": "Cache",
+        "identifier": "a7c78d38-31a8-417e-ba6b-7e58b352f304",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "_apis/artifactcache",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "Cache",
+        "status": 1,
+        "toolId": "Cache"
+      },
+      {
+        "description": "BuildArtifacts",
+        "displayName": "BuildArtifacts",
+        "identifier": "1db06c96-014e-44e1-ac91-90b2d4b3e984",
+        "locationMappings": [],
+        "maxVersion": "12.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "_apis/pipelines/workflows/{buildId}/artifacts",
+        "relativeToSetting": 2,
+        "resourceVersion": 6,
+        "serviceOwner": "00000000-0000-0000-0000-000000000000",
+        "serviceType": "BuildArtifacts",
+        "status": 1,
+        "toolId": "BuildArtifacts"
+      },
+      {
+        "description": "brokerlistener",
         "displayName": "brokerlistener",
         "identifier": "38f00041-0953-4d24-86c3-5432d23e2205",
         "locationMappings": [],
         "maxVersion": "6.0",
-        "minVersion": "6.0",
+        "minVersion": "1.0",
         "properties": {},
         "relativePath": "_apis/{area}/{resource}",
         "releasedVersion": "0.0",
         "resourceVersion": 1,
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "packages",
-        "identifier": "8ffcd551-079c-493a-9c02-54346299d144",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}/{packageType}/{platform}/{version}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 2,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "labels",
-        "identifier": "68ba3f2c-5f79-4d8b-a48a-57c7b46ed3e0",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}/{labelId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "actiondownloadinfo",
-        "identifier": "27d7f831-88c1-4719-8ca1-6a061dad90eb",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/plans/{planId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
+        "status": 1,
+        "toolId": "distributedtask"
+      },
+      {
+        "description": "createdsession",
         "displayName": "createdsession",
         "identifier": "a4e1f2b5-0c3d-4e8a-9f6d-7b5c1a0e2d3f",
         "locationMappings": [],
         "maxVersion": "6.0",
-        "minVersion": "6.0",
+        "minVersion": "1.0",
         "properties": {},
         "relativePath": "_apis/{area}/brokerlistener/{resource}",
         "releasedVersion": "0.0",
         "resourceVersion": 1,
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "attachments",
-        "identifier": "7898f959-9cdf-4096-b29e-7f293031629e",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.1",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/plans/{planId}/timelines/{timelineId}/records/{recordId}/{resource}/{type}/{name}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "requesttypelimits",
-        "identifier": "4eedf63a-38ba-42ea-8525-81f7eb96c0e3",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}/{poolId}/{requestType}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "logs",
-        "identifier": "15344176-9e77-4cf4-a7c3-8bc4d0a3c4eb",
-        "locationMappings": [],
-        "maxVersion": "1.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/plans/{planId}/{resource}/{logId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "jobrequests",
-        "identifier": "fc825784-c92a-4299-9221-998a02d1b54f",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/pools/{poolId}/{resource}/{requestId}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "records",
-        "identifier": "8893bc5b-35b2-4be7-83cb-99e683551db4",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.0",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/plans/{planId}/timelines/{timelineId}/{resource}/{recordId}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "agentclouds",
-        "identifier": "bfa72b3d-0fc6-43fb-932b-a7f6559f93b9",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}/{agentCloudId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "events",
-        "identifier": "dfed02fb-deee-4039-a04d-aa21d0241995",
-        "locationMappings": [],
-        "maxVersion": "1.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/plans/{planId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "feed",
-        "identifier": "9ae056f6-d4e4-4d0c-bd26-aee2a22f01f2",
-        "locationMappings": [],
-        "maxVersion": "1.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/plans/{planId}/timelines/{timelineId}/records/{recordId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "admintoken",
-        "identifier": "9236daac-313e-4760-8245-b0a8bfca212a",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/pools/{pool}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "agentCloudRequest",
-        "identifier": "4ebade4d-ba5d-43bf-a047-b58cee747c84",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/pools/{poolId}/requests/{agentRequestid}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
+        "status": 1,
+        "toolId": "distributedtask"
+      },
+      {
+        "description": "runnermessages",
         "displayName": "runnermessages",
         "identifier": "25adab70-1379-4186-be8e-b643061ebe3a",
         "locationMappings": [],
@@ -887,703 +432,93 @@
         "minVersion": "1.0",
         "properties": {},
         "relativePath": "_apis/{area}/{resource}/{messageId}",
-        "releasedVersion": "5.1",
+        "releasedVersion": "0.0",
         "resourceVersion": 1,
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
+        "status": 1,
+        "toolId": "distributedtask"
+      },
+      {
+        "description": "runnerconfigrefresh",
         "displayName": "runnerconfigrefresh",
         "identifier": "13b5d709-74aa-470b-a8e9-bf9f3ded3f18",
         "locationMappings": [],
         "maxVersion": "6.0",
-        "minVersion": "6.0",
+        "minVersion": "1.0",
         "properties": {},
         "relativePath": "_apis/{area}/agents/{agentId}/{resource}/{configType}",
         "releasedVersion": "0.0",
         "resourceVersion": 1,
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "plans",
-        "identifier": "f8d10759-6e90-48bc-96b0-d19440116797",
-        "locationMappings": [],
-        "maxVersion": "1.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}/{planId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "pools",
-        "identifier": "a8c47e17-4d56-4a56-92bb-de7ea7dc65be",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}/{poolId}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "logs",
-        "identifier": "46f5667d-263a-4684-91b1-dff7fdcf64e2",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.0",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/plans/{planId}/{resource}/{logId}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "records",
-        "identifier": "50170d5d-f122-492f-9816-e2ef9f8d1756",
-        "locationMappings": [],
-        "maxVersion": "1.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/plans/{planId}/timelines/{timelineId}/{resource}/{recordId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "idtoken",
-        "identifier": "69a319f4-28c1-4bfd-93e6-ea0ff5c6f1a2",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.0",
-        "properties": {},
-        "relativePath": "{scopeIdentifier}/_apis/{area}/hubs/{hubName}/plans/{planId}/jobs/{jobId}/{resource}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "queuedrequests",
-        "identifier": "4a35de01-9369-4f33-af9a-eb94ea60f6d2",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "requests",
-        "identifier": "20189bd7-5134-49c2-b8e9-f9e856eea2b2",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/agentclouds/{agentCloudId}/{resource}/{agentCloudRequestId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "distributedtask",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "NotFound",
-        "identifier": "232b00f3-c6b8-48c6-883f-1a8dc6cbef8a",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{*params}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "Fallback",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "FeatureFlags",
-        "identifier": "3e2b80f8-9e6f-441e-8393-005610692d9c",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{resource}/{name}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "FeatureAvailability",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "ConnectionData",
-        "identifier": "00d9565f-ed9c-4a06-9a50-00e7896ccab4",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "Location",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "ResourceAreas",
-        "identifier": "e81700f7-3be2-46de-8624-2eb35882fcaa",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "3.2",
-        "properties": {},
-        "relativePath": "_apis/{resource}/{areaId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "Location",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "ServiceDefinitions",
-        "identifier": "d810a47d-f4f4-4a62-a03f-fa1860585c4c",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{resource}/{serviceType}/{identifier}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "Location",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
+        "status": 1,
+        "toolId": "distributedtask"
+      },
+      {
+        "description": "token",
         "displayName": "token",
         "identifier": "10d13a60-2758-406c-8ab7-cffccb21fcf4",
         "locationMappings": [],
         "maxVersion": "6.0",
-        "minVersion": "0.0",
+        "minVersion": "1.0",
         "properties": {},
         "relativePath": "_apis/{area}/{resource}",
-        "releasedVersion": "5.1",
+        "releasedVersion": "0.0",
         "resourceVersion": 1,
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "oauth2",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "operations",
-        "identifier": "7f82df6d-7d09-46c1-a015-643b556b3a1e",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "4.0",
-        "properties": {},
-        "relativePath": "_apis/{resource}/{pluginId}/{operationId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "operations",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "operations",
-        "identifier": "9a1b74b4-2ca8-4a9f-8470-c2f2e6fdc949",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "2.0",
-        "properties": {},
-        "relativePath": "_apis/{resource}/{operationId}",
-        "releasedVersion": "5.1",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "operations",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "planLogs",
-        "identifier": "c6f7a235-42d9-4921-b721-0e29f91e15a5",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/plans/{orchestrationIdentifier}/logs",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "artifacts",
-        "identifier": "85023071-bd5e-4438-89b0-2a5bf362a19d",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.2",
-        "properties": {},
-        "relativePath": "{project}/_apis/{area}/{pipelineId}/runs/{runId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "signedartifactscontent",
-        "identifier": "6b2ac16f-cd00-4df9-a13b-3a1cc8afb188",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.2",
-        "properties": {},
-        "relativePath": "{project}/_apis/{area}/{pipelineId}/runs/{runId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "live",
-        "identifier": "c41b3775-6d50-48bd-b261-42da7f0f1ba0",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.2",
-        "properties": {},
-        "relativePath": "{project}/_apis/{area}/{pipelineId}/runs/{runId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 2,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "githubbillingtags",
-        "identifier": "6bada0b9-cb9c-435d-9584-4e0e730001bb",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/orgs/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "signedsummary",
-        "identifier": "ef349595-f66b-4ccb-a216-50c77479cd17",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/plans/{planId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "workflowArtifacts",
-        "identifier": "3fccc81a-f469-4633-bd4a-581c11a24de1",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/workflows/{workflowRunId}/artifacts",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
+        "status": 1,
+        "toolId": "oauth2"
+      },
+      {
+        "description": "steps",
         "displayName": "steps",
         "identifier": "99ea91b7-bbe9-4bd3-a924-874f13205b21",
         "locationMappings": [],
         "maxVersion": "6.0",
-        "minVersion": "6.0",
+        "minVersion": "1.0",
         "properties": {},
         "relativePath": "_apis/{area}/plans/{planId}/jobs/{jobId}/{resource}",
         "releasedVersion": "0.0",
         "resourceVersion": 1,
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "summary",
-        "identifier": "01d75881-6892-4ec6-8dca-91ecfb0dc048",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/plans/{planId}/{resource}",
+        "status": 1,
+        "toolId": "pipelines"
+      },
+      {
+        "description": "jobs",
+        "displayName": "jobs",
+        "identifier": "4818972d-29fa-4b86-92c1-de5ae7ef33f5",
+        "locationMappings": [],
+        "maxVersion": "6.0",
+        "minVersion": "1.0",
+        "properties": {},
+        "relativePath": "_apis/{area}/plans/{planId}/{resource}/{jobId}",
         "releasedVersion": "0.0",
         "resourceVersion": 1,
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "planArtifacts",
-        "identifier": "cfe7c963-19d0-4451-9ae8-96009ee26441",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/plans/{orchestrationIdentifier}/artifacts",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "orgs",
-        "identifier": "cd70ba1a-d59a-4e0b-9934-97998159ccc8",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.1",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "signalr",
-        "identifier": "1ffe4916-ac72-4566-add0-9bab31e44fcf",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.2",
-        "properties": {},
-        "relativePath": "{project}/_apis/{area}/{pipelineId}/runs/{runId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "runinfo",
-        "identifier": "366b03b8-6a21-4631-bc9e-9c8e7b5b361a",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "{project}/_apis/{area}/{resource}/{orchestrationIdentifier}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
+        "status": 1,
+        "toolId": "pipelines"
+      },
+      {
+        "description": "logs",
         "displayName": "logs",
         "identifier": "fb1b6d27-3957-43d5-a14b-a2d70403e545",
         "locationMappings": [],
         "maxVersion": "6.0",
-        "minVersion": "5.1",
+        "minVersion": "1.0",
         "properties": {},
         "relativePath": "{project}/_apis/{area}/{pipelineId}/runs/{runId}/{resource}/{logId}",
         "releasedVersion": "0.0",
         "resourceVersion": 1,
         "serviceOwner": "00000000-0000-0000-0000-000000000000",
         "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "runs",
-        "identifier": "7859261e-d2e9-4a68-b820-a5d84cc5bb3d",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.2",
-        "properties": {},
-        "relativePath": "{project}/_apis/{area}/{pipelineId}/{resource}/{runId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 2,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "adminevents",
-        "identifier": "a75c63c4-1ade-40eb-a0ce-a9378d13ed5a",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "{project}/_apis/{area}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "pipelines",
-        "identifier": "28e1305e-2afe-47bf-abaf-cbb0e6a91988",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.1",
-        "properties": {},
-        "relativePath": "{project}/_apis/{area}/{pipelineId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "jobs",
-        "identifier": "4818972d-29fa-4b86-92c1-de5ae7ef33f5",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/plans/{planId}/{resource}/{jobId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "signedlogcontent",
-        "identifier": "74f99e32-e2c4-44f4-93dc-dec0bca530a5",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "5.1",
-        "properties": {},
-        "relativePath": "{project}/_apis/{area}/{pipelineId}/runs/{runId}/{resource}/{logId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "pipelines",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "generatejitconfig",
-        "identifier": "35931bc4-ad7b-443a-a004-05e196e6aca3",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/runnerscalesets/{runnerScaleSetId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "runtime",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "acquirablejobs",
-        "identifier": "eecccaab-67d0-4c74-ab62-0c420001c513",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/runnerscalesets/{runnerScaleSetId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "runtime",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "runnerscalesets",
-        "identifier": "d5d2a677-b1ad-4e16-bba0-2b0275ddc338",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}/{runnerScaleSetId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "runtime",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "visibility",
-        "identifier": "29fbb88c-d23d-4921-b5d4-473639ca6ccf",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/runnergroups/{groupId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "runtime",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "runnergroups",
-        "identifier": "70bd3705-14b4-4c74-8480-67316bd79fe9",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/{resource}/{groupId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "runtime",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "acquirejobs",
-        "identifier": "9267acd8-c7c6-45d8-b50e-883fe06a2b9a",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/runnerscalesets/{runnerScaleSetId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "runtime",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "acquire",
-        "identifier": "057fef20-bfa7-4621-a06d-cf39eede64a7",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/runnerscalesets/{runnerScaleSetId}/jobs/{requestId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "runtime",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "acquire",
-        "identifier": "a0facaaa-c2f6-4d34-ae1f-d0cd687d7576",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/runnerscalesets/{runnerScaleSetId}/jobs/{requestId}/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "runtime",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "sessions",
-        "identifier": "15bc8dc1-f86c-44e6-b220-d2a1d9d14b2f",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/runnerscalesets/{runnerScaleSetId}/{resource}/{sessionId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "runtime",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "messages",
-        "identifier": "c9b03fd5-6283-460a-90f5-e1adeaeae5ad",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "6.0",
-        "properties": {},
-        "relativePath": "_apis/{area}/runnerscalesets/{runnerScaleSetId}/{resource}/{messageId}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "runtime",
-        "toolId": "Framework"
-      },
-      {
-        "description": "Resource Location",
-        "displayName": "ServiceLevel",
-        "identifier": "3c4bfe05-aeb6-45f8-93a6-929468401657",
-        "locationMappings": [],
-        "maxVersion": "6.0",
-        "minVersion": "1.0",
-        "properties": {},
-        "relativePath": "_apis/{resource}",
-        "releasedVersion": "0.0",
-        "resourceVersion": 1,
-        "serviceOwner": "00000000-0000-0000-0000-000000000000",
-        "serviceType": "Servicing",
-        "toolId": "Framework"
+        "status": 1,
+        "toolId": "pipelines"
       }
-    ],
-    "serviceOwner": "0000005a-0000-8888-8000-000000000000"
+    ]
   }
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 82.0 / aksh 1.2 | p95: official 98.5 / aksh 1.4

### `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`

**Header key differences:**

- official only: `{'x-vss-senderdeploymentid', 'pragma', 'activityid', 'x-tfs-processid', 'cache-control', 'strict-transport-security'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,12 +1,11 @@
 {
-  "deploymentId": "fc1cfc90-fc40-edcb-6d0f-3b1380ee7b68",
-  "deploymentType": "hosted",
-  "instanceId": "9f1fe989-7d0d-4a9b-a9bf-11330ab257c1",
+  "deploymentId": "00000000-0000-0000-0000-000000000000",
+  "deploymentType": "selfHosted",
+  "instanceId": "76bda2f7-068e-446d-ad21-67575bf79b6f",
   "locationServiceData": {
     "clientCacheFresh": true,
     "defaultAccessMappingMoniker": "ScaleUnitMapping",
-    "lastChangeId": 4952065,
-    "lastChangeId64": 4952065,
-    "serviceOwner": "0000005a-0000-8888-8000-000000000000"
+    "lastChangeId": 1,
+    "lastChangeId64": 1
   }
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 44.9 / aksh 0.2 | p95: official 46.1 / aksh 0.2

### `GET /_apis/distributedtask/pools/{n}/agents?agentName=mitm-official&includeCapabilities=False`

**Header key differences:**

- official only: `{'x-vss-senderdeploymentid', 'x-frame-options', 'pragma', 'activityid', 'transfer-encoding', 'x-tfs-processid', 'cache-control', 'strict-transport-security'}`

**Response body diff:**

_identical_

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 26.7 / aksh 0.3 | p95: official 26.7 / aksh 0.3

### `GET /_apis/distributedtask/pools/{n}/messages?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate=false&waitSeconds={n}`

**Header key differences:**

- official only: `{'x-github-request-id', 'x-github-backend'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "body": "{\"runner_request_id\":\"c76d4e37-60d1-5a84-8a2c-85f144d31a12\",\"run_service_url\":\"https://run-actions-3-azure-eastus.actions.githubusercontent.com/9/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
-  "messageId": 8940805291766790408,
+  "body": "{\"billing_owner_id\":\"local\",\"run_service_url\":\"http://127.0.0.1:9090/broker/1/\",\"runner_request_id\":\"368f4162-506b-47c1-ac4f-5d11af10cdc0\",\"should_acknowledge\":true}",
+  "messageId": "368f4162-506b-47c1-ac4f-5d11af10cdc0",
   "messageType": "RunnerJobRequest"
 }
```

**Status codes:** official: [200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 203.5 / aksh 0.3 | p95: official 477.1 / aksh 0.3

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{'x-vss-senderdeploymentid', 'x-frame-options', 'pragma', 'activityid', 'transfer-encoding', 'x-tfs-processid', 'cache-control', 'strict-transport-security'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,29 +1,11 @@
 {
-  "count": 2,
+  "count": 1,
   "value": [
     {
-      "agentCloudId": null,
-      "autoSize": true,
-      "createdOn": "2026-06-25T22:15:07.107Z",
       "id": 1,
       "isHosted": false,
-      "isInternal": true,
       "name": "Default",
-      "scope": "9f1fe989-7d0d-4a9b-a9bf-11330ab257c1",
-      "size": 0,
-      "targetSize": null
-    },
-    {
-      "agentCloudId": 1,
-      "autoSize": true,
-      "createdOn": "2026-06-25T22:15:07.42Z",
-      "id": 2,
-      "isHosted": true,
-      "isInternal": false,
-      "name": "GitHub Actions",
-      "scope": "9f1fe989-7d0d-4a9b-a9bf-11330ab257c1",
-      "size": 20,
-      "targetSize": 1
+      "poolType": 1
     }
   ]
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 21.7 / aksh 0.3 | p95: official 21.7 / aksh 0.3

### `POST /_apis/distributedtask/pools/{n}/agents`

**Header key differences:**

- official only: `{'x-vss-senderdeploymentid', 'x-frame-options', 'pragma', 'activityid', 'x-tfs-processid', 'cache-control', 'strict-transport-security'}`

**Request body diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,38 +1,33 @@
 {
   "authorization": {
-    "authorizationUrl": "https://tokenghub.actions.githubusercontent.com/_apis/oauth2/token/9f1fe989-7d0d-4a9b-a9bf-11330ab257c1",
-    "clientId": "be762a47-a172-4543-a511-2a1ea626a8e8",
+    "authorizationUrl": "http://127.0.0.1:9090/runner/server/_apis/v1/oauth2/token",
+    "clientId": "1a9624fe-e32b-44d3-970e-31d7d979b5a0",
     "publicKey": {
       "exponent": "AQAB",
       "modulus": "***REDACTED***+TTPPSwlGtdEM+jIBwtgHKdP/q6pIHk/YxxmEX4YoUDuZ8U+lmA+ah36bym5kiRg4fCJ3wb5cuR/0XpJMPJtir0/JneZmG/UvaKKIhHe05a3o8nwgV+***REDACTED***+***REDACTED***/wtoZXtaAlXPw=="
     }
   },
-  "createdOn": "2026-06-29T13:43:11.223Z",
   "currentParallelism": 0,
   "disableUpdate": false,
   "enabled": true,
   "ephemeral": false,
-  "id": 21,
+  "id": 1,
   "isElastic": false,
   "isVirtual": false,
   "labels": [
     {
-      "id": 1,
       "name": "self-hosted",
-      "type": "system"
+      "type": "user"
     },
     {
-      "id": 2,
       "name": "macOS",
-      "type": "system"
+      "type": "user"
     },
     {
-      "id": 3,
       "name": "ARM64",
-      "type": "system"
+      "type": "user"
     },
     {
-      "id": 4,
       "name": "mitm",
       "type": "user"
     }
@@ -40,7 +35,6 @@
   "maxParallelism": 1,
   "name": "mitm-official",
   "osDescription": "Darwin 25.4.0 Darwin Kernel Version 25.4.0: Thu Mar 19 19:33:25 PDT 2026; root:xnu-12377.101.15~1/RELEASE_ARM64_T6041",
-  "owningTenant": null,
   "properties": {
     "RequireFipsCryptography": {
       "$type": "System.Boolean",
@@ -48,11 +42,11 @@
     },
     "ServerUrl": {
       "$type": "System.String",
-      "$value": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
+      "$value": "http://127.0.0.1:9090/runner/server"
     },
     "ServerUrlV2": {
       "$type": "System.String",
-      "$value": "https://broker.actions.githubusercontent.com/"
+      "$value": "http://127.0.0.1:9090/runner/server"
     },
     "UseV2Flow": {
       "$type": "System.Boolean",
@@ -60,7 +54,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-21",
+  "queueName": "taskagent-1",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 179.3 / aksh 0.4 | p95: official 179.3 / aksh 0.4

### `POST /_apis/distributedtask/pools/{n}/sessions`

**Header key differences:**

- official only: `{'x-github-request-id', 'x-github-backend'}`

**Request body diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -2,5 +2,5 @@
   "assignmentQueued": false,
   "orchestrationId": "",
   "ownerName": "Nuraydias-Mac-Studio (PID: 80120)",
-  "sessionId": "f05a0f24-1fe5-4fe6-8ab8-0c275b14fd18"
+  "sessionId": "5ab7760f-5773-441b-b29f-f8ab5cc3b5a4"
 }
```

**Status codes:** official: [201] | aksh: [201]

**Timing (ms):** p50: official 31.1 / aksh 0.3 | p95: official 31.1 / aksh 0.3

### `POST /_apis/v1/AgentRequest/{n}/{n}?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64`

**Header key differences:**

- official only: `{'x-github-request-id', 'x-github-backend'}`

**Request body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 36.6 / aksh 0.2 | p95: official 59.3 / aksh 0.2

### `POST /_apis/v1/oauth2/token`

**Header key differences:**

- official only: `{'x-vss-senderdeploymentid', 'pragma', 'activityid', 'x-tfs-processid', 'x-tfs-session', 'cache-control', 'strict-transport-security'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "access_token": "***REDACTED***",
+  "access_token": "***REDACTED***",
   "expires_in": 2999,
   "token_type": "JWT"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200]

**Timing (ms):** p50: official 25.7 / aksh 0.2 | p95: official 27.2 / aksh 0.3

### `POST /api/v3/actions/runner-registration`

**Header key differences:**

- official only: `{'etag', 'x-content-type-options', 'x-frame-options', 'vary', 'x-xss-protection', 'x-ratelimit-remaining', 'content-security-policy', 'access-control-expose-headers', 'x-github-api-version-selected', 'cache-control', 'x-github-request-id', 'x-ratelimit-limit', 'access-control-allow-origin', 'x-ratelimit-used', 'strict-transport-security', 'x-ratelimit-resource', 'x-ratelimit-reset', 'x-github-media-type', 'referrer-policy'}`

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
-  "url": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/"
+  "url": "http://127.0.0.1:9090/runner/server"
 }
```

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 300.4 / aksh 0.4 | p95: official 300.4 / aksh 0.4

### `POST /broker/{n}/acquirejob`

**Header key differences:**

- official only: `{'x-github-actions-orchestration-id', 'transfer-encoding', 'x-plan-id', 'x-github-request-id', 'x-job-name', 'x-github-backend'}`

**Request body diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,1462 +1,143 @@
 {
-  "billingOwnerId": "O_kgDOEbddog",
+  "actionsDownloadInfo": {},
   "contextData": {
+    "env": {
+      "t": 2
+    },
     "github": {
       "d": [
         {
-          "k": "ref",
-          "v": "refs/heads/autoresearch/session-20260628"
+          "k": "event",
+          "v": {
+            "d": [
+              {
+                "k": "commits",
+                "v": {
+                  "t": 1
+                }
+              },
+              {
+                "k": "ref",
+                "v": "refs/heads/replay"
+              }
+            ],
+            "t": 2
+          }
         },
         {
-          "k": "sha",
-          "v": "***REDACTED***"
+          "k": "event_name",
+          "v": "push"
+        },
+        {
+          "k": "ref",
+          "v": "refs/heads/replay"
         },
         {
           "k": "repository",
           "v": "preloopdev/aksh"
         },
         {
-          "k": "repository_owner",
-          "v": "preloopdev"
-        },
-        {
-          "k": "repository_owner_id",
-          "v": "297229730"
-        },
-        {
-          "k": "repositoryUrl",
-          "v": "git://github.com/preloopdev/aksh.git"
-        },
-        {
           "k": "run_id",
-          "v": "28338183540"
-        },
-        {
-          "k": "run_number",
-          "v": "2"
-        },
-        {
-          "k": "retention_days",
-          "v": "90"
-        },
-        {
-          "k": "run_attempt",
-          "v": "1"
-        },
-        {
-          "k": "artifact_cache_size_limit",
-          "v": "10"
-        },
-        {
-          "k": "repository_visibility",
-          "v": "private"
-        },
-        {
-          "k": "actor_id",
-          "v": "46893322"
-        },
-        {
-          "k": "actor",
-          "v": "Bnjoroge1"
-        },
-        {
-          "k": "workflow",
-          "v": "dogfood"
-        },
-        {
-          "k": "head_ref",
-          "v": ""
-        },
-        {
-          "k": "base_ref",
-          "v": ""
-        },
-        {
-          "k": "event_name",
-          "v": "push"
+          "v": "3ea18ff8-c4e3-4bee-b896-81d267086171"
         },
         {
           "k": "server_url",
-          "v": "https://github.com"
-        },
-        {
-          "k": "api_url",
-          "v": "https://api.github.com"
-        },
-        {
-          "k": "graphql_url",
-          "v": "https://api.github.com/graphql"
-        },
-        {
-          "k": "ref_name",
-          "v": "autoresearch/session-20260628"
-        },
-        {
-          "k": "ref_protected",
-          "v": false
-        },
-        {
-          "k": "ref_type",
-          "v": "branch"
-        },
-        {
-          "k": "secret_source",
-          "v": "Actions"
-        },
-        {
-          "k": "event",
-          "v": {
-            "d": [
-              {
-                "k": "after",
-                "v": "***REDACTED***"
-              },
-              {
-                "k": "base_ref",
-                "v": null
-              },
-              {
-                "k": "before",
-                "v": "***REDACTED***"
-              },
-              {
-                "k": "commits",
-                "v": {
-                  "a": [
-                    {
-                      "d": [
-                        {
-                          "k": "author",
-                          "v": {
-                            "d": [
-                              {
-                                "k": "email",
-                                "v": "williamsriunge@gmail.com"
-                              },
-                              {
-                                "k": "name",
-                                "v": "Bill Njoroge"
-                              },
-                              {
-                                "k": "username",
-                                "v": "Bnjoroge1"
-                              }
-                            ],
-                            "t": 2
-                          }
-                        },
-                        {
-                          "k": "committer",
-                          "v": {
-                            "d": [
-                              {
-                                "k": "email",
-                                "v": "williamsriunge@gmail.com"
-                              },
-                              {
-                                "k": "name",
-                                "v": "Bill Njoroge"
-                              },
-                              {
-                                "k": "username",
-                                "v": "Bnjoroge1"
-                              }
-                            ],
-                            "t": 2
-                          }
-                        },
-                        {
-                          "k": "distinct",
-                          "v": true
-                        },
-                        {
-                          "k": "id",
-                          "v": "***REDACTED***"
-                        },
-                        {
-                          "k": "message",
-                          "v": "fix: TaskStep Deserialize and dogfood variable resolution\n\n- Add custom Deserialize for TaskStep to handle the TemplateToken\n  map format for env/inputs fields (produced by our Serialize impl).\n  The extract_template_map helper handles both the new {type:2,map:[...]}\n  format and the old plain-object format.\n\n- Fix dogfood.yml to use ${{ vars.AKSH_REPO_ROOT }} expression\n  syntax instead of raw $AKSH_REPO_ROOT shell variable. The runner\n  injects workflow variables into expressions, not shell env vars.\n  The previous cd \"$AKSH_REPO_ROOT\" was silently going to $HOME.\n\n- Downgrade clippy from -D warnings to warnings-only (missing_docs\n  warnings in azdo.rs DTOs are pre-existing; -D would block CI)."
-                        },
-                        {
-                          "k": "timestamp",
-                          "v": "2026-06-28T18:19:46-04:00"
-                        },
-                        {
-                          "k": "tree_id",
-                          "v": "***REDACTED***"
-                        },
-                        {
-                          "k": "url",
-                          "v": "https://github.com/preloopdev/aksh/commit/***REDACTED***"
-                        }
-                      ],
-                      "t": 2
-                    },
-                    {
-                      "d": [
-                        {
-                          "k": "author",
-                          "v": {
-                            "d": [
-                              {
-                                "k": "email",
-                                "v": "williamsriunge@gmail.com"
-                              },
-                              {
-                                "k": "name",
-                                "v": "Bill Njoroge"
-                              },
-                              {
-                                "k": "username",
-                                "v": "Bnjoroge1"
-                              }
-                            ],
-                            "t": 2
-                          }
-                        },
-                        {
-                          "k": "committer",
-                          "v": {
-                            "d": [
-                              {
-                                "k": "email",
-                                "v": "williamsriunge@gmail.com"
-                              },
-                              {
-                                "k": "name",
-                                "v": "Bill Njoroge"
-                              },
-                              {
-                                "k": "username",
-                                "v": "Bnjoroge1"
-                              }
-                            ],
-                            "t": 2
-                          }
-                        },
-                        {
-                          "k": "distinct",
-                          "v": true
-                        },
-                        {
-                          "k": "id",
-                          "v": "***REDACTED***"
-                        },
-                        {
-                          "k": "message",
-                          "v": "nit"
-                        },
-                        {
-                          "k": "timestamp",
-                          "v": "2026-06-28T18:27:38-04:00"
-                        },
-                        {
-                          "k": "tree_id",
-                          "v": "***REDACTED***"
-                        },
-                        {
-                          "k": "url",
-                          "v": "https://github.com/preloopdev/aksh/commit/***REDACTED***"
-                        }
-                      ],
-                      "t": 2
-                    }
-                  ],
-                  "t": 1
-                }
-              },
-              {
-                "k": "compare",
-                "v": "https://github.com/preloopdev/aksh/compare/6263f4e55a64...dc414158abbe"
-              },
-              {
-                "k": "created",
-                "v": false
-              },
-              {
-                "k": "deleted",
-                "v": false
-              },
-              {
-                "k": "forced",
-                "v": true
-              },
-              {
-                "k": "head_commit",
-                "v": {
-                  "d": [
-                    {
-                      "k": "author",
-                      "v": {
-                        "d": [
-                          {
-                            "k": "email",
-                            "v": "williamsriunge@gmail.com"
-                          },
-                          {
-                            "k": "name",
-                            "v": "Bill Njoroge"
-                          },
-                          {
-                            "k": "username",
-                            "v": "Bnjoroge1"
-                          }
-                        ],
-                        "t": 2
-                      }
-                    },
-                    {
-                      "k": "committer",
-                      "v": {
-                        "d": [
-                          {
-                            "k": "email",
-                            "v": "williamsriunge@gmail.com"
-                          },
-                          {
-                            "k": "name",
-                            "v": "Bill Njoroge"
-                          },
-                          {
-                            "k": "username",
-                            "v": "Bnjoroge1"
-                          }
-                        ],
-                        "t": 2
-                      }
-                    },
-                    {
-                      "k": "distinct",
-                      "v": true
-                    },
-                    {
-                      "k": "id",
-                      "v": "***REDACTED***"
-                    },
-                    {
-                      "k": "message",
-                      "v": "nit"
-                    },
-                    {
-                      "k": "timestamp",
-                      "v": "2026-06-28T18:27:38-04:00"
-                    },
-                    {
-                      "k": "tree_id",
-                      "v": "***REDACTED***"
-                    },
-                    {
-                      "k": "url",
-                      "v": "https://github.com/preloopdev/aksh/commit/***REDACTED***"
-                    }
-                  ],
-                  "t": 2
-                }
-              },
-              {
-                "k": "organization",
-                "v": {
-                  "d": [
-                    {
-                      "k": "avatar_url",
-                      "v": "https://avatars.githubusercontent.com/u/297229730?v=4"
-                    },
-                    {
-                      "k": "description",
-                      "v": null
-                    },
-                    {
-                      "k": "events_url",
-                      "v": "https://api.github.com/orgs/preloopdev/events"
-                    },
-                    {
-                      "k": "hooks_url",
-                      "v": "https://api.github.com/orgs/preloopdev/hooks"
-                    },
-                    {
-                      "k": "id",
-                      "v": 297229730
-                    },
-                    {
-                      "k": "issues_url",
-                      "v": "https://api.github.com/orgs/preloopdev/issues"
-                    },
-                    {
-                      "k": "login",
-                      "v": "preloopdev"
-                    },
-                    {
-                      "k": "members_url",
-                      "v": "https://api.github.com/orgs/preloopdev/members{/member}"
-                    },
-                    {
-                      "k": "node_id",
-                      "v": "O_kgDOEbddog"
-                    },
-                    {
-                      "k": "public_members_url",
-                      "v": "https://api.github.com/orgs/preloopdev/public_members{/member}"
-                    },
-                    {
-                      "k": "repos_url",
-                      "v": "https://api.github.com/orgs/preloopdev/repos"
-                    },
-                    {
-                      "k": "url",
-                      "v": "https://api.github.com/orgs/preloopdev"
-                    }
-                  ],
-                  "t": 2
-                }
-              },
-              {
-                "k": "pusher",
-                "v": {
-                  "d": [
-                    {
-                      "k": "email",
-                      "v": "williamsriunge@gmail.com"
-                    },
-                    {
-                      "k": "name",
-                      "v": "Bnjoroge1"
-                    }
-                  ],
-                  "t": 2
-                }
-              },
-              {
-                "k": "ref",
-                "v": "refs/heads/autoresearch/session-20260628"
-              },
-              {
-                "k": "repository",
-                "v": {
-                  "d": [
-                    {
-                      "k": "allow_forking",
-                      "v": false
-                    },
-                    {
-                      "k": "archive_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/{archive_format}{/ref}"
-                    },
-                    {
-                      "k": "archived",
-                      "v": false
-                    },
-                    {
-                      "k": "assignees_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/assignees{/user}"
-                    },
-                    {
-                      "k": "blobs_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/git/blobs{/sha}"
-                    },
-                    {
-                      "k": "branches_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/branches{/branch}"
-                    },
-                    {
-                      "k": "clone_url",
-                      "v": "https://github.com/preloopdev/aksh.git"
-                    },
-                    {
-                      "k": "collaborators_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/collaborators{/collaborator}"
-                    },
-                    {
-                      "k": "comments_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/comments{/number}"
-                    },
-                    {
-                      "k": "commits_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/commits{/sha}"
-                    },
-                    {
-                      "k": "compare_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/compare/{base}...{head}"
-                    },
-                    {
-                      "k": "contents_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/contents/{+path}"
-                    },
-                    {
-                      "k": "contributors_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/contributors"
-                    },
-                    {
-                      "k": "created_at",
-                      "v": 1782425694
-                    },
-                    {
-                      "k": "custom_properties",
-                      "v": {
-                        "d": [],
-                        "t": 2
-                      }
-                    },
-                    {
-                      "k": "default_branch",
-                      "v": "main"
-                    },
-                    {
-                      "k": "deployments_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/deployments"
-                    },
-                    {
-                      "k": "description",
-                      "v": null
-                    },
-                    {
-                      "k": "disabled",
-                      "v": false
-                    },
-                    {
-                      "k": "downloads_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/downloads"
-                    },
-                    {
-                      "k": "events_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/events"
-                    },
-                    {
-                      "k": "fork",
-                      "v": false
-                    },
-                    {
-                      "k": "forks",
-                      "v": 0
-                    },
-                    {
-                      "k": "forks_count",
-                      "v": 0
-                    },
-                    {
-                      "k": "forks_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/forks"
-                    },
-                    {
-                      "k": "full_name",
-                      "v": "preloopdev/aksh"
-                    },
-                    {
-                      "k": "git_commits_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/git/commits{/sha}"
-                    },
-                    {
-                      "k": "git_refs_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/git/refs{/sha}"
-                    },
-                    {
-                      "k": "git_tags_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/git/tags{/sha}"
-                    },
-                    {
-                      "k": "git_url",
-                      "v": "git://github.com/preloopdev/aksh.git"
-                    },
-                    {
-                      "k": "has_discussions",
-                      "v": false
-                    },
-                    {
-                      "k": "has_downloads",
-                      "v": true
-                    },
-                    {
-                      "k": "has_issues",
-                      "v": true
-                    },
-                    {
-                      "k": "has_pages",
-                      "v": false
-                    },
-                    {
-                      "k": "has_projects",
-                      "v": true
-                    },
-                    {
-                      "k": "has_pull_requests",
-                      "v": true
-                    },
-                    {
-                      "k": "has_wiki",
-                      "v": true
-                    },
-                    {
-                      "k": "homepage",
-                      "v": null
-                    },
-                    {
-                      "k": "hooks_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/hooks"
-                    },
-                    {
-                      "k": "html_url",
-                      "v": "https://github.com/preloopdev/aksh"
-                    },
-                    {
-                      "k": "id",
-                      "v": 1280732127
-                    },
-                    {
-                      "k": "is_template",
-                      "v": false
-                    },
-                    {
-                      "k": "issue_comment_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/issues/comments{/number}"
-                    },
-                    {
-                      "k": "issue_events_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/issues/events{/number}"
-                    },
-                    {
-                      "k": "issues_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/issues{/number}"
-                    },
-                    {
-                      "k": "keys_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/keys{/key_id}"
-                    },
-                    {
-                      "k": "labels_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/labels{/name}"
-                    },
-                    {
-                      "k": "language",
-                      "v": "Rust"
-                    },
-                    {
-                      "k": "languages_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/languages"
-                    },
-                    {
-                      "k": "license",
-                      "v": null
-                    },
-                    {
-                      "k": "master_branch",
-                      "v": "main"
-                    },
-                    {
-                      "k": "merges_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/merges"
-                    },
-                    {
-                      "k": "milestones_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/milestones{/number}"
-                    },
-                    {
-                      "k": "mirror_url",
-                      "v": null
-                    },
-                    {
-                      "k": "name",
-                      "v": "aksh"
-                    },
-                    {
-                      "k": "node_id",
-                      "v": "R_kgDOTFZr3w"
-                    },
-                    {
-                      "k": "notifications_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/notifications{?since,all,participating}"
-                    },
-                    {
-                      "k": "open_issues",
-                      "v": 1
-                    },
-                    {
-                      "k": "open_issues_count",
-                      "v": 1
-                    },
-                    {
-                      "k": "organization",
-                      "v": "preloopdev"
-                    },
-                    {
-                      "k": "owner",
-                      "v": {
-                        "d": [
-                          {
-                            "k": "avatar_url",
-                            "v": "https://avatars.githubusercontent.com/u/297229730?v=4"
-                          },
-                          {
-                            "k": "email",
-                            "v": null
-                          },
-                          {
-                            "k": "events_url",
-                            "v": "https://api.github.com/users/preloopdev/events{/privacy}"
-                          },
-                          {
-                            "k": "followers_url",
-                            "v": "https://api.github.com/users/preloopdev/followers"
-                          },
-                          {
-                            "k": "following_url",
-                            "v": "https://api.github.com/users/preloopdev/following{/other_user}"
-                          },
-                          {
-                            "k": "gists_url",
-                            "v": "https://api.github.com/users/preloopdev/gists{/gist_id}"
-                          },
-                          {
-                            "k": "gravatar_id",
-                            "v": ""
-                          },
-                          {
-                            "k": "html_url",
-                            "v": "https://github.com/preloopdev"
-                          },
-                          {
-                            "k": "id",
-                            "v": 297229730
-                          },
-                          {
-                            "k": "login",
-                            "v": "preloopdev"
-                          },
-                          {
-                            "k": "name",
-                            "v": "preloopdev"
-                          },
-                          {
-                            "k": "node_id",
-                            "v": "O_kgDOEbddog"
-                          },
-                          {
-                            "k": "organizations_url",
-                            "v": "https://api.github.com/users/preloopdev/orgs"
-                          },
-                          {
-                            "k": "received_events_url",
-                            "v": "https://api.github.com/users/preloopdev/received_events"
-                          },
-                          {
-                            "k": "repos_url",
-                            "v": "https://api.github.com/users/preloopdev/repos"
-                          },
-                          {
-                            "k": "site_admin",
-                            "v": false
-                          },
-                          {
-                            "k": "starred_url",
-                            "v": "https://api.github.com/users/preloopdev/starred{/owner}{/repo}"
-                          },
-                          {
-                            "k": "subscriptions_url",
-                            "v": "https://api.github.com/users/preloopdev/subscriptions"
-                          },
-                          {
-                            "k": "type",
-                            "v": "Organization"
-                          },
-                          {
-                            "k": "url",
-                            "v": "https://api.github.com/users/preloopdev"
-                          },
-                          {
-                            "k": "user_view_type",
-                            "v": "public"
-                          }
-                        ],
-                        "t": 2
-                      }
-                    },
-                    {
-                      "k": "private",
-                      "v": true
-                    },
-                    {
-                      "k": "pull_request_creation_policy",
-                      "v": "all"
-                    },
-                    {
-                      "k": "pulls_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/pulls{/number}"
-                    },
-                    {
-                      "k": "pushed_at",
-                      "v": 1782685833
-                    },
-                    {
-                      "k": "releases_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/releases{/id}"
-                    },
-                    {
-                      "k": "size",
-                      "v": 458
-                    },
-                    {
-                      "k": "ssh_url",
-                      "v": "git@github.com:preloopdev/aksh.git"
-                    },
-                    {
-                      "k": "stargazers",
-                      "v": 1
-                    },
-                    {
-                      "k": "stargazers_count",
-                      "v": 1
-                    },
-                    {
-                      "k": "stargazers_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/stargazers"
-                    },
-                    {
-                      "k": "statuses_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/statuses/{sha}"
-                    },
-                    {
-                      "k": "subscribers_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/subscribers"
-                    },
-                    {
-                      "k": "subscription_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/subscription"
-                    },
-                    {
-                      "k": "svn_url",
-                      "v": "https://github.com/preloopdev/aksh"
-                    },
-                    {
-                      "k": "tags_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/tags"
-                    },
-                    {
-                      "k": "teams_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/teams"
-                    },
-                    {
-                      "k": "topics",
-                      "v": {
-                        "a": [],
-                        "t": 1
-                      }
-                    },
-                    {
-                      "k": "trees_url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh/git/trees{/sha}"
-                    },
-                    {
-                      "k": "updated_at",
-                      "v": "2026-06-28T21:43:13Z"
-                    },
-                    {
-                      "k": "url",
-                      "v": "https://api.github.com/repos/preloopdev/aksh"
-                    },
-                    {
-                      "k": "visibility",
-                      "v": "private"
-                    },
-                    {
-                      "k": "watchers",
-                      "v": 1
-                    },
-                    {
-                      "k": "watchers_count",
-                      "v": 1
-                    },
-                    {
-                      "k": "web_commit_signoff_required",
-                      "v": false
-                    }
-                  ],
-                  "t": 2
-                }
-              },
-              {
-                "k": "sender",
-                "v": {
-                  "d": [
-                    {
-                      "k": "avatar_url",
-                      "v": "https://avatars.githubusercontent.com/u/46893322?v=4"
-                    },
-                    {
-                      "k": "events_url",
-                      "v": "https://api.github.com/users/Bnjoroge1/events{/privacy}"
-                    },
-                    {
-                      "k": "followers_url",
-                      "v": "https://api.github.com/users/Bnjoroge1/followers"
-                    },
-                    {
-                      "k": "following_url",
-                      "v": "https://api.github.com/users/Bnjoroge1/following{/other_user}"
-                    },
-                    {
-                      "k": "gists_url",
-                      "v": "https://api.github.com/users/Bnjoroge1/gists{/gist_id}"
-                    },
-                    {
-                      "k": "gravatar_id",
-                      "v": ""
-                    },
-                    {
-                      "k": "html_url",
-                      "v": "https://github.com/Bnjoroge1"
-                    },
-                    {
-                      "k": "id",
-                      "v": 46893322
-                    },
-                    {
-                      "k": "login",
-                      "v": "Bnjoroge1"
-                    },
-                    {
-                      "k": "node_id",
-                      "v": "MDQ6VXNlcjQ2ODkzMzIy"
-                    },
-                    {
-                      "k": "organizations_url",
-                      "v": "https://api.github.com/users/Bnjoroge1/orgs"
-                    },
-                    {
-                      "k": "received_events_url",
-                      "v": "https://api.github.com/users/Bnjoroge1/received_events"
-                    },
-                    {
-                      "k": "repos_url",
-                      "v": "https://api.github.com/users/Bnjoroge1/repos"
-                    },
-                    {
-                      "k": "site_admin",
-                      "v": false
-                    },
-                    {
-                      "k": "starred_url",
-                      "v": "https://api.github.com/users/Bnjoroge1/starred{/owner}{/repo}"
-                    },
-                    {
-                      "k": "subscriptions_url",
-                      "v": "https://api.github.com/users/Bnjoroge1/subscriptions"
-                    },
-                    {
-                      "k": "type",
-                      "v": "User"
-                    },
-                    {
-                      "k": "url",
-                      "v": "https://api.github.com/users/Bnjoroge1"
-                    },
-                    {
-                      "k": "user_view_type",
-                      "v": "public"
-                    }
-                  ],
-                  "t": 2
-                }
-              }
-            ],
-            "t": 2
-          }
-        },
-        {
-          "k": "workflow_ref",
-          "v": "preloopdev/aksh/.github/workflows/dogfood.yml@refs/heads/autoresearch/session-20260628"
-        },
-        {
-          "k": "workflow_sha",
-          "v": "***REDACTED***"
-        },
-        {
-          "k": "repository_id",
-          "v": "1280732127"
-        },
-        {
-          "k": "triggering_actor",
-          "v": "Bnjoroge1"
+          "v": "http://localhost"
         }
       ],
       "t": 2
     },
-    "inputs": {
-      "d": [],
+    "matrix": {
       "t": 2
     },
-    "job": {
+    "needs": {
+      "t": 2
+    },
+    "strategy": {
+      "t": 2
+    },
+    "system": {
       "d": [
         {
-          "k": "check_run_id",
-          "v": 83947953509
+          "k": "jobDisplayName",
+          "v": "replay_0"
         },
         {
-          "k": "workflow_ref",
-          "v": "preloopdev/aksh/.github/workflows/dogfood.yml@refs/heads/autoresearch/session-20260628"
+          "k": "jobId",
+          "v": "368f4162-506b-47c1-ac4f-5d11af10cdc0"
         },
         {
-          "k": "workflow_sha",
-          "v": "***REDACTED***"
+          "k": "orchestrationId",
+          "v": "368f4162-506b-47c1-ac4f-5d11af10cdc0"
         },
         {
-          "k": "workflow_repository",
-          "v": "preloopdev/aksh"
+          "k": "planId",
+          "v": "368f4162-506b-47c1-ac4f-5d11af10cdc0"
         },
         {
-          "k": "workflow_file_path",
-          "v": ".github/workflows/dogfood.yml"
+          "k": "timelineId",
+          "v": "8626da46-ab0f-4986-a85a-c42c27fe5acb"
         }
       ],
       "t": 2
-    },
-    "matrix": null,
-    "needs": {
-      "d": [],
-      "t": 2
-    },
-    "strategy": {
-      "d": [
-        {
-          "k": "fail-fast",
-          "v": true
-        },
-        {
-          "k": "job-index",
-          "v": 0
-        },
-        {
-          "k": "job-total",
-          "v": 1
-        },
-        {
-          "k": "max-parallel",
-          "v": 1
-        }
-      ],
-      "t": 2
-    },
-    "vars": {
-      "d": [],
-      "t": 2
     }
   },
-  "defaults": [],
-  "environmentVariables": [],
-  "fileTable": [
-    ".github/workflows/dogfood.yml"
-  ],
-  "jobContainer": null,
-  "jobDisplayName": "rust",
-  "jobId": "c76d4e37-60d1-5a84-8a2c-85f144d31a12",
-  "jobName": "__default",
-  "jobOutputs": null,
-  "jobServiceContainers": null,
-  "lockedUntil": "0001-01-01T00:00:00",
-  "mask": [
-    {
-      "type": "regex",
-      "value": "\\b(?:eyJ0eXAiOi|eyJhbGciOi|eyJ4NXQiOi|eyJraWQiOi)[^\\s'\";]+"
-    },
-    {
-      "type": "regex",
-      "value": "\\bBearer\\s+[^\\s'\";]+"
-    },
-    {
-      "type": "regex",
-      "value": "\\b(?i:Password|Pwd)=(?:[^\\s'\";]+|\"[^\"]+\")"
-    },
-    {
-      "type": "regex",
-      "value": "\\s+-(?i:Password|Pwd)\\s+(?:[^\\s'\";]+|\"[^\"]+\")"
-    },
-    {
-      "type": "regex",
-      "value": "\\bv1\\.[0-9A-Fa-f]{40}\\b"
-    },
-    {
-      "type": "regex",
-      "value": "\\bgh[pousr]{1}_[A-Za-z0-9]{36}\\b"
-    },
-    {
-      "type": "regex",
-      "value": "\\bgithub_pat_[0-9][A-Za-z0-9]{21}_[A-Za-z0-9]{59}\\b"
-    },
-    {
-      "type": "regex",
-      "value": "(?:[a-zA-Z][a-zA-Z\\d+-.]*):\\/\\/([a-zA-Z\\d\\-._~\\!$&'()*+,;=%]+):([a-zA-Z\\d\\-._~\\!$&'()*+,;=:%]*)@"
-    },
-    {
-      "type": "regex",
-      "value": "\\b[0-9A-Za-z-_~.]{3}7Q~[0-9A-Za-z-_~.]{31}\\b|\\b[0-9A-Za-z-_~.]{3}8Q~[0-9A-Za-z-_~.]{34}\\b"
-    },
-    {
-      "type": "regex",
-      "value": "(?:^|[^0-9A-Za-z+/])[0-9A-Za-z+/]{76}(APIM|ACDb|\\+(ABa|AMC|ASt))[0-9A-Za-z+/]{5}[AQgw]=="
-    },
-    {
-      "type": "regex",
-      "value": "(?:^|[^0-9A-Za-z+/])[0-9A-Za-z+/]{33}(AIoT|\\+(ASb|AEh|ARm))[A-P][0-9A-Za-z+/]{5}="
-    },
-    {
-      "type": "regex",
-      "value": "\\b[0-9A-Za-z_\\-]{44}AzFu[0-9A-Za-z\\-_]{5}[AQgw]=="
-    },
-    {
-      "type": "regex",
-      "value": "\\b[0-9A-Za-z]{42}AzSe[A-D][0-9A-Za-z]{5}\\b"
-    },
-    {
-      "type": "regex",
-      "value": "\\b[0-9A-Za-z+/]{42}\\+ACR[A-D][0-9A-Za-z+/]{5}\\b"
-    },
-    {
-      "type": "regex",
-      "value": "\\b[0-9A-Za-z]{33}AzCa[A-P][0-9A-Za-z]{5}="
-    },
-    {
-      "type": "regex",
-      "value": "\\boy2[a-p][0-9a-z]{15}[aq][0-9a-z]{11}[eu][bdfhjlnprtvxz357][a-p][0-9a-z]{11}[aeimquy4]\\b"
-    },
-    {
-      "type": "regex",
-      "value": "\\bnpm_[0-9A-Za-z]{36}\\b"
-    },
-    {
-      "type": "regex",
-      "value": "\\bx-ghcr-signature=[^&]+"
-    },
-    {
-      "type": "regex",
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
-    }
-  ],
-  "messageType": "RunnerJobRequest",
+  "displayName": "replay_0",
+  "jobDisplayName": "replay_0",
+  "jobId": "368f4162-506b-47c1-ac4f-5d11af10cdc0",
+  "maskHints": [],
   "plan": {
-    "artifactLocation": "",
-    "artifactUri": "",
-    "planId": "25667da9-97c8-4a6c-8823-7c020d6bd86e",
-    "planType": "actions",
-    "version": 0
+    "planId": "368f4162-506b-47c1-ac4f-5d11af10cdc0",
+    "planType": "Job"
   },
-  "requestId": 0,
+  "requestId": 1,
   "resources": {
     "endpoints": [
       {
         "authorization": {
           "parameters": {
-            "AccessToken": "***REDACTED***"
+            "AccessToken": "***REDACTED***.e30.ZmFrZXNpZw"
           },
           "scheme": "OAuth"
         },
-        "data": {
-          "CacheServerUrl": "https://artifactcache.actions.githubusercontent.com/***REDACTED***/",
-          "ConnectivityChecks": "[\"https://broker.actions.githubusercontent.com/health\",\"https://token.actions.githubusercontent.com/ready\",\"https://run.actions.githubusercontent.com/health\"]",
-          "FeedStreamUrl": "wss://results-receiver.actions.githubusercontent.com/_ws/ingest.sock",
-          "GenerateIdTokenUrl": "",
-          "PipelinesServiceUrl": "https://pipelinesghubeus7.actions.githubusercontent.com/***REDACTED***/",
-          "ResultsServiceUrl": "https://results-receiver.actions.githubusercontent.com/",
-          "ServerId": "",
-          "ServerName": ""
-        },
-        "isReady": true,
+        "data": {},
         "isShared": false,
         "name": "SystemVssConnection",
-        "url": "https://run-actions-3-azure-eastus.actions.githubusercontent.com/9/"
+        "serviceOwner": "github",
+        "type": "azdoserver",
+        "url": "http://localhost"
       }
-    ]
+    ],
+    "repositories": []
   },
-  "snapshot": null,
   "steps": [
     {
       "condition": "success()",
-      "contextName": "__run",
-      "continueOnError": null,
-      "displayNameToken": {
-        "col": 15,
-        "file": 1,
-        "line": 14,
-        "lit": "cargo fmt",
-        "type": 0
+      "environment": {
+        "type": 2
       },
-      "id": "c0a06269-4fc3-47fb-bf0c-cf5ac3e5d305",
+      "id": "a0c88a64-7168-47b4-bdc7-9f09e700e5de",
       "inputs": {
         "map": [
           {
-            "Key": {
-              "lit": "script",
-              "type": 0
-            },
-            "Value": {
-              "col": 14,
-              "expr": "format('cd \"{0}\" && cargo fmt --all --check', vars.AKSH_REPO_ROOT)",
-              "file": 1,
-              "line": 15,
-              "type": 3
-            }
+            "key": "script",
+            "value": "echo replay 0"
           }
         ],
         "type": 2
       },
-      "name": "__run",
       "reference": {
         "type": "script"
       },
-      "timeoutInMinutes": null,
-      "type": "action"
-    },
-    {
-      "condition": "success()",
-      "contextName": "__run_2",
-      "continueOnError": null,
-      "displayNameToken": {
-        "col": 15,
-        "file": 1,
-        "line": 16,
-        "lit": "cargo clippy",
-        "type": 0
-      },
-      "id": "4876be4b-28b7-4384-9cab-b9efdeebe787",
-      "inputs": {
-        "map": [
-          {
-            "Key": {
-              "lit": "script",
-              "type": 0
-            },
-            "Value": {
-              "col": 14,
-              "expr": "format('cd \"{0}\" && cargo clippy --workspace --all-targets', vars.AKSH_REPO_ROOT)",
-              "file": 1,
-              "line": 17,
-              "type": 3
-            }
-          }
-        ],
-        "type": 2
-      },
-      "name": "__run_2",
-      "reference": {
-        "type": "script"
-      },
-      "timeoutInMinutes": null,
-      "type": "action"
-    },
-    {
-      "condition": "success()",
-      "contextName": "__run_3",
-      "continueOnError": null,
-      "displayNameToken": {
-        "col": 15,
-        "file": 1,
-        "line": 18,
-        "lit": "cargo test",
-        "type": 0
-      },
-      "id": "00522050-4852-4d73-93ec-9e11458dee6c",
-      "inputs": {
-        "map": [
-          {
-            "Key": {
-              "lit": "script",
-              "type": 0
-            },
-            "Value": {
-              "col": 14,
-              "expr": "format('cd \"{0}\" && cargo test --workspace --quiet', vars.AKSH_REPO_ROOT)",
-              "file": 1,
-              "line": 19,
-              "type": 3
-            }
-          }
-        ],
-        "type": 2
-      },
-      "name": "__run_3",
-      "reference": {
-        "type": "script"
-      },
-      "timeoutInMinutes": null,
       "type": "action"
     }
   ],
   "timeline": {
-    "changeId": 0,
-    "id": "25667da9-97c8-4a6c-8823-7c020d6bd86e",
-    "location": null
+    "id": "8626da46-ab0f-4986-a85a-c42c27fe5acb"
   },
   "variables": {
-    "Actions.EnableHttpRedirects": {
-      "value": "true"
-    },
-    "DistributedTask.AddWarningToNode12Action": {
-      "value": "true"
-    },
-    "DistributedTask.AddWarningToNode16Action": {
-      "value": "true"
-    },
-    "DistributedTask.AllowRunnerContainerHooks": {
-      "value": "true"
-    },
-    "DistributedTask.DeprecateStepOutputCommands": {
-      "value": "true"
-    },
-    "DistributedTask.DetailUntarFailure": {
-      "value": "true"
-    },
-    "DistributedTask.EnableCompositeActions": {
-      "value": "true"
-    },
-    "DistributedTask.EnableJobServerQueueTelemetry": {
-      "value": "true"
-    },
-    "DistributedTask.EnhancedAnnotations": {
-      "value": "true"
-    },
-    "DistributedTask.***REDACTED***": {
-      "value": "true"
-    },
-    "DistributedTask.***REDACTED***": {
-      "value": "true"
-    },
-    "DistributedTask.***REDACTED***": {
-      "value": "true"
-    },
-    "DistributedTask.MarkJobAsFailedOnWorkerCrash": {
-      "value": "true"
-    },
-    "DistributedTask.NewActionMetadata": {
-      "value": "true"
-    },
-    "DistributedTask.UploadStepSummary": {
-      "value": "true"
-    },
-    "DistributedTask.UseActionArchiveCache": {
-      "value": "true"
-    },
-    "DistributedTask.UseWhich2": {
-      "value": "true"
-    },
-    "RunService.FixEmbeddedIssues": {
-      "value": "true"
-    },
-    "actions.runner.requirenode24": {
-      "value": "false"
-    },
-    "actions.runner.usenode24bydefault": {
-      "value": "true"
-    },
-    "actions.runner.warnonnode20": {
-      "value": "true"
-    },
-    "***REDACTED***": {
-      "value": "true"
-    },
-    "***REDACTED***": {
-      "value": "false"
-    },
-    "***REDACTED***": {
-      "value": "true"
-    },
-    "***REDACTED***": {
-      "value": "true"
-    },
-    "***REDACTED***": {
-      "value": "false"
-    },
-    "***REDACTED***": {
-      "value": "true"
-    },
-    "***REDACTED***": {
-      "value": "false"
-    },
-    "***REDACTED***": {
-      "value": "false"
-    },
-    "***REDACTED***": {
+    "system.pullRequestTargetBranch": {
       "value": ""
-    },
-    "***REDACTED***": {
-      "value": "June 16th, 2026"
-    },
-    "***REDACTED***": {
-      "value": "true"
-    },
-    "***REDACTED***": {
-      "value": "true"
-    },
-    "***REDACTED***": {
-      "value": "true"
-    },
-    "***REDACTED***": {
-      "value": "false"
-    },
-    "***REDACTED***": {
-      "value": "false"
-    },
-    "***REDACTED***": {
-      "value": "false"
-    },
-    "actions_uses_cache_service_v2": {
-      "value": "true"
-    },
-    "github_token": {
-      "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
-    },
-    "system.from_run_service": {
-      "value": "true"
-    },
-    "system.github.job": {
-      "value": "rust"
-    },
-    "system.github.launch_endpoint": {
-      "value": "https://launch.actions.githubusercontent.com"
-    },
-    "system.github.results_endpoint": {
-      "value": "https://results-receiver.actions.githubusercontent.com/"
-    },
-    "system.github.results_upload_with_sdk": {
-      "value": "true"
-    },
-    "system.github.token": {
-      "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
-    },
-    "system.github.token.permissions": {
-      "value": "{\"Contents\":\"read\",\"Metadata\":\"read\",\"Packages\":\"read\"}"
-    },
-    "system.orchestrationId": {
-      "value": "25667da9-97c8-4a6c-8823-7c020d6bd86e.rust.__default"
-    },
-    "system.phaseDisplayName": {
-      "value": "rust"
-    },
-    "system.runner.lowdiskspacethreshold": {
-      "value": "100"
-    },
-    "system.runnerEnvironment": {
-      "value": "self-hosted"
-    },
-    "system.runnerGroupName": {
-      "value": "Default"
     }
   }
 }
```

**Status codes:** official: [200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 653.1 / aksh 0.3 | p95: official 669.0 / aksh 0.4

### `POST /broker/{n}/completejob`

**Header key differences:**

- official only: `{'x-job-name', 'x-github-request-id', 'x-plan-id', 'x-github-backend'}`

**Request body diff:**

_identical_

**Status codes:** official: [204, 204, 204] | aksh: [204, 204, 204]

**Timing (ms):** p50: official 43.3 / aksh 0.2 | p95: official 44.9 / aksh 0.2

### `POST /broker/{n}/renewjob`

**Header key differences:**

- official only: `{'x-job-name', 'x-github-request-id', 'x-plan-id', 'x-github-backend'}`

**Request body diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-06-29T13:53:13.996828592Z"
+  "lockedUntil": "2099-12-31T23:59:59Z"
 }
```

**Status codes:** official: [200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 39.6 / aksh 0.2 | p95: official 67.0 / aksh 0.2

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'x-github-request-id', 'x-github-backend'}`

**Request body diff:**

_identical_

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 59.9 / aksh 0.2 | p95: official 350.9 / aksh 0.3

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-github-request-id', 'x-github-backend'}`

**Request body diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa12.blob.core.windows.net/actions-results/25667da9-97c8-4a6c-8823-7c020d6bd86e/workflow-job-run-c76d4e37-60d1-5a84-8a2c-85f144d31a12/logs/job/job-logs.txt?se=2026-06-29T14%3A43%3A33Z&sig=LCBUSd4r5cn2%2Fbw89vx4wl5VvcQ%2FcBch6xtmO0okM5k%3D&ske=2026-06-29T16%3A18%3A50Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-29T12%3A18%3A50Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-29T13%3A43%3A28Z&sv=2025-11-05"
+  "logs_url": "http://127.0.0.1:9090/replay/results/25667da9-97c8-4a6c-8823-7c020d6bd86e/c76d4e37-60d1-5a84-8a2c-85f144d31a12/job-logs.txt"
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 31.0 / aksh 0.2 | p95: official 31.0 / aksh 0.2

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-github-request-id', 'x-github-backend'}`

**Request body diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa12.blob.core.windows.net/actions-results/25667da9-97c8-4a6c-8823-7c020d6bd86e/workflow-job-run-c76d4e37-60d1-5a84-8a2c-85f144d31a12/logs/steps/step-logs-28664512-d0e7-4cd6-a467-4e2625fbe5e6.txt?se=2026-06-29T14%3A43%3A15Z&sig=0Q8f0%2BvKCdUoZswZQ%2Fi9%2FCQNok79pkk4YoBeYuMrTWk%3D&ske=2026-06-29T16%3A19%3A51Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-06-29T12%3A19%3A51Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-06-29T13%3A43%3A10Z&sv=2025-11-05",
+  "logs_url": "http://127.0.0.1:9090/replay/results/25667da9-97c8-4a6c-8823-7c020d6bd86e/c76d4e37-60d1-5a84-8a2c-85f144d31a12/step-28664512-d0e7-4cd6-a467-4e2625fbe5e6.txt",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 42.6 / aksh 0.2 | p95: official 75.9 / aksh 0.2
