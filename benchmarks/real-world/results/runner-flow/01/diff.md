# Runner flow diff: 01

- official capture: `/Users/bnjoroge/mitm-proxy/experiments/mitm/captures/official/01-register-and-idle/2026-06-29T13-43-03Z`
- aksh capture: `/Users/bnjoroge/mitm-proxy/experiments/mitm/captures/aksh/01-register-and-idle/2026-06-27T04-39-23Z`
- official summary: status=ok flows=67
- aksh summary: status=None flows=1

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `GET 127.0.0.1/healthz` | 0 | 1 ⚠ |
| `GET broker.actions.githubusercontent.com/health` | 4 | 0 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}` | 4 | 0 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}` | 4 | 0 ⚠ |
| `GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 3 | 0 ⚠ |
| `GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}` | 3 | 0 ⚠ |
| `GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 1 | 0 ⚠ |
| `GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/distributedtask/pools?poolType=Automation` | 1 | 0 ⚠ |
| `GET run.actions.githubusercontent.com/health` | 4 | 0 ⚠ |
| `GET token.actions.githubusercontent.com/ready` | 4 | 0 ⚠ |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64` | 4 | 0 ⚠ |
| `POST broker.actions.githubusercontent.com/session` | 1 | 0 ⚠ |
| `POST pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/distributedtask/pools//{n}/agents` | 1 | 0 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 3 | 0 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 3 | 0 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 11 | 0 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 4 | 0 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 3 | 0 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 4 | 0 ⚠ |
| `POST tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}` | 5 | 0 ⚠ |

## Endpoint sequence diff

```diff
--- official
+++ aksh
@@ -1,69 +1,3 @@
 [
-  "GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/distributedtask/pools?poolType=Automation",
-  "GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False",
-  "POST pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/distributedtask/pools//{n}/agents",
-  "GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "GET pipelinesghubeus7.actions.githubusercontent.com/f8fkc6uZ8wJlJreDFyN5HIZlz4p7hQc0a08eakyxqm86rYYhmm/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}",
-  "POST tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}",
-  "POST broker.actions.githubusercontent.com/session",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}",
-  "POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64",
-  "POST tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
-  "GET token.actions.githubusercontent.com/ready",
-  "GET broker.actions.githubusercontent.com/health",
-  "GET run.actions.githubusercontent.com/health",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}",
-  "POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64",
-  "POST tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
-  "GET broker.actions.githubusercontent.com/health",
-  "GET run.actions.githubusercontent.com/health",
-  "GET token.actions.githubusercontent.com/ready",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
-  "POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}",
-  "POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64",
-  "POST tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob",
-  "GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=macOS&architecture=ARM64&disableUpdate={volatile}",
-  "POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob",
-  "GET broker.actions.githubusercontent.com/health",
-  "GET token.actions.githubusercontent.com/ready",
-  "GET run.actions.githubusercontent.com/health",
-  "POST results-receiver.actions.githubusercontent.com/twirp/res
... truncated ...
```

## Per-flow contract differences

_No per-flow status/schema/redacted-value differences._

## Verdict

FAIL: 1 contract differences found.

- endpoint-sequence: 1
