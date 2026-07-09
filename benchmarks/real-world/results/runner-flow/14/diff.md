# Runner flow diff: 14-annotations

- official capture: `/Users/bnjoroge/runner-watcher/.runner-watch/conformance/v2.335.1/14-annotations/official-filtered`
- aksh capture: `/Users/bnjoroge/runner-watcher/.runner-watch/conformance/v2.335.1/14-annotations/aksh`
- official summary: status=captured flows=22
- aksh summary: status=captured flows=23

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 0 | 6 ⚠ |
| `GET ?/runner/server/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 0 | 1 ⚠ |
| `GET ?/runner/server/_apis/distributedtask/pools//{n}/messages?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}&waitSeconds=0` | 0 | 1 ⚠ |
| `GET ?/runner/server/_apis/distributedtask/pools?poolType=Automation` | 0 | 1 ⚠ |
| `GET broker.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/messages?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}&waitSeconds=0` | 1 | 0 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 6 | 0 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 1 | 0 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools?poolType=Automation` | 1 | 0 ⚠ |
| `POST ?/api/v3/actions/runner-registration` | 0 | 1 ⚠ |
| `POST ?/broker//{n}/acquirejob` | 0 | 1 ⚠ |
| `POST ?/broker//{n}/completejob` | 0 | 1 ⚠ |
| `POST ?/broker//{n}/renewjob` | 0 | 1 ⚠ |
| `POST ?/runner/server/_apis/distributedtask/pools//{n}/agents` | 0 | 1 ⚠ |
| `POST ?/runner/server/_apis/distributedtask/pools//{n}/sessions` | 0 | 1 ⚠ |
| `POST ?/runner/server/_apis/v1/AgentRequest//{n}//{n}?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 0 | 1 ⚠ |
| `POST ?/runner/server/_apis/v1/oauth2/token` | 0 | 2 ⚠ |
| `POST ?/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 0 | 1 ⚠ |
| `POST ?/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 0 | 1 ⚠ |
| `POST ?/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 0 | 3 ⚠ |
| `POST broker.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/sessions` | 1 | 0 ⚠ |
| `POST broker.actions.githubusercontent.com/runner/server/_apis/v1/AgentRequest//{n}//{n}?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 1 | 0 ⚠ |
| `POST pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/agents` | 1 | 0 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 0 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 0 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 3 | 0 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com/broker//{n}/acquirejob` | 1 | 0 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com/broker//{n}/completejob` | 1 | 0 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com/broker//{n}/renewjob` | 1 | 0 ⚠ |
| `POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token` | 2 | 0 ⚠ |

## Endpoint sequence diff

```diff
--- official
+++ aksh
@@ -1,24 +1,25 @@
 [
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools?poolType=Automation",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False",
-  "POST pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/agents",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST broker.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/sessions",
-  "GET broker.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/messages?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}&waitSeconds=0",
-  "POST broker.actions.githubusercontent.com/runner/server/_apis/v1/AgentRequest//{n}//{n}?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com/broker//{n}/acquirejob",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com/broker//{n}/renewjob",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com/broker//{n}/completejob"
+  "POST ?/api/v3/actions/runner-registration",
+  "GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET ?/runner/server/_apis/distributedtask/pools?poolType=Automation",
+  "GET ?/runner/server/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False",
+  "POST ?/runner/server/_apis/distributedtask/pools//{n}/agents",
+  "GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "POST ?/runner/server/_apis/v1/oauth2/token",
+  "POST ?/runner/server/_apis/distributedtask/pools//{n}/sessions",
+  "GET ?/runner/server/_apis/distributedtask/pools//{n}/messages?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}&waitSeconds=0",
+  "POST ?/runner/server/_apis/v1/AgentRequest//{n}//{n}?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64",
+  "POST ?/runner/server/_apis/v1/oauth2/token",
+  "POST ?/broker//{n}/acquirejob",
+  "POST ?/broker//{n}/renewjob",
+  "POST ?/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "POST ?/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "POST ?/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
+  "POST ?/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
+  "POST ?/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
+  "POST ?/broker//{n}/completejob"
 ]
```

## Per-flow contract differences

_No per-flow status/schema/redacted-value differences._

## Verdict

FAIL: 1 contract differences found.

- endpoint-sequence: 1
