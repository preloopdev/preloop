# Runner flow diff: 07-step-failure

- official capture: `/Users/bnjoroge/runner-watcher/.runner-watch/conformance/v2.335.1/07-step-failure/official-filtered`
- aksh capture: `/Users/bnjoroge/runner-watcher/.runner-watch/conformance/v2.335.1/07-step-failure/aksh`
- official summary: status=captured flows=43
- aksh summary: status=captured flows=45

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE ?/runner/server/_apis/distributedtask/pools//{n}/agents//{n}` | 0 | 1 ⚠ |
| `DELETE ?/runner/server/_apis/distributedtask/pools//{n}/sessions` | 0 | 1 ⚠ |
| `DELETE broker.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/sessions` | 1 | 0 ⚠ |
| `DELETE pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/agents//{n}` | 1 | 0 ⚠ |
| `GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 0 | 9 ⚠ |
| `GET ?/runner/server/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 0 | 2 ⚠ |
| `GET ?/runner/server/_apis/distributedtask/pools//{n}/messages?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}&waitSeconds=0` | 0 | 3 ⚠ |
| `GET ?/runner/server/_apis/distributedtask/pools?poolType=Automation` | 0 | 1 ⚠ |
| `GET broker.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/messages?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}&waitSeconds=0` | 3 | 0 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 9 | 0 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 2 | 0 ⚠ |
| `GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools?poolType=Automation` | 1 | 0 ⚠ |
| `POST ?/api/v3/actions/runner-registration` | 0 | 2 ⚠ |
| `POST ?/broker//{n}/acquirejob` | 0 | 1 ⚠ |
| `POST ?/broker//{n}/completejob` | 0 | 1 ⚠ |
| `POST ?/broker//{n}/renewjob` | 0 | 1 ⚠ |
| `POST ?/runner/server/_apis/distributedtask/pools//{n}/agents` | 0 | 1 ⚠ |
| `POST ?/runner/server/_apis/distributedtask/pools//{n}/sessions` | 0 | 1 ⚠ |
| `POST ?/runner/server/_apis/v1/AgentRequest//{n}//{n}?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 0 | 1 ⚠ |
| `POST ?/runner/server/_apis/v1/oauth2/token` | 0 | 14 ⚠ |
| `POST ?/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 0 | 1 ⚠ |
| `POST ?/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 0 | 1 ⚠ |
| `POST ?/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 0 | 4 ⚠ |
| `POST broker.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/sessions` | 1 | 0 ⚠ |
| `POST broker.actions.githubusercontent.com/runner/server/_apis/v1/AgentRequest//{n}//{n}?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 1 | 0 ⚠ |
| `POST pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/agents` | 1 | 0 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 0 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 0 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 4 | 0 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com/broker//{n}/acquirejob` | 1 | 0 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com/broker//{n}/completejob` | 1 | 0 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com/broker//{n}/renewjob` | 1 | 0 ⚠ |
| `POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token` | 14 | 0 ⚠ |

## Endpoint sequence diff

```diff
--- official
+++ aksh
@@ -1,45 +1,47 @@
 [
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET broker.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/messages?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}&waitSeconds=0",
-  "GET pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False",
-  "DELETE pipelinesghubeus24.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/agents//{n}",
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
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com/broker//{n}/completejob",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "GET broker.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/messages?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}&waitSeconds=0",
-  "DELETE broker.actions.githubusercontent.com/runner/server/_apis/distributedtask/pools//{n}/sessions",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token",
-  "POST tokenghub.actions.githubusercontent.com/runner/server/_apis/v1/oauth2/token"
+  "POST ?/api/v3/actions/runner-registration",
+  "GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET ?/runner/server/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
+  "GET ?/runner/server/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False",
+  "D
... truncated ...
```

## Per-flow contract differences

_No per-flow status/schema/redacted-value differences._

## Verdict

FAIL: 1 contract differences found.

- endpoint-sequence: 1
