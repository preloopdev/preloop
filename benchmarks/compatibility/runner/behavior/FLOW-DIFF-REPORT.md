# MITM Flow Comparison Report

## 101-dynamic-matrix-dataflow: FAIL: 88 contract differences found.

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 7 | 7 |
| `GET broker.actions.githubusercontent.com/health` | 7 | 7 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 7 | 7 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 20 | 13 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 42 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1` | 0 | 8 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}` | 6 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 8 | 8 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools?poolType=Automation` | 8 | 8 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 7 | 7 |
| `GET run.actions.githubusercontent.com/health` | 7 | 7 |
| `GET token.actions.githubusercontent.com/ready` | 7 | 7 |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 7 | 7 |
| `POST broker.actions.githubusercontent.com/session` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/oauth2/token` | 15 | 15 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 7 | 9 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 7 | 7 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 21 | 21 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 7 | 7 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 21 | 21 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 7 | 7 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 7 | 7 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 7 | 7 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 7 | 7 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 21 | 21 |

## Endpoint sequence diff

## 102-failure-needs-conditions: FAIL: 69 contract differences found.

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 3 | 3 |
| `GET broker.actions.githubusercontent.com/health` | 3 | 3 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 3 | 3 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 13 | 15 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 48 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1` | 0 | 8 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 8 | 8 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools?poolType=Automation` | 8 | 8 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 3 | 3 |
| `GET run.actions.githubusercontent.com/health` | 3 | 3 |
| `GET token.actions.githubusercontent.com/ready` | 3 | 3 |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 3 | 3 |
| `POST broker.actions.githubusercontent.com/session` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/oauth2/token` | 11 | 11 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 3 | 6 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 3 | 3 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 13 | 13 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 3 | 3 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 13 | 13 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 3 | 3 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 3 | 3 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 3 | 3 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 3 | 3 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 13 | 13 |

## Endpoint sequence diff

## 103-cancellation-background-post: FAIL: 111 contract differences found.

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 0 ⚠ |
| `GET broker.actions.githubusercontent.com/health` | 1 | 2 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 7 | 127 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 55 | 4599 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 48 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1` | 0 | 8 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 8 | 8 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools?poolType=Automation` | 8 | 8 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 1 | 2 ⚠ |
| `GET run.actions.githubusercontent.com/health` | 1 | 2 ⚠ |
| `GET token.actions.githubusercontent.com/ready` | 1 | 2 ⚠ |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 2 ⚠ |
| `POST broker.actions.githubusercontent.com/session` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/oauth2/token` | 9 | 0 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 3 | 2 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 2 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 5 | 10 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 2 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 5 | 10 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 2 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 2 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 6 | 7 ⚠ |
| `POST tokenghub.actions.githubusercontent.com/_apis/oauth2/token/{guid}` | 0 | 10 ⚠ |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 2 ⚠ |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 5 | 10 ⚠ |

## Endpoint sequence diff

## 104-nested-lifecycle: FAIL: 46 contract differences found.

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/health` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 12 | 15 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 24 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}` | 24 | 8 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 8 | 8 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools?poolType=Automation` | 8 | 8 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 1 | 1 |
| `GET run.actions.githubusercontent.com/health` | 1 | 1 |
| `GET token.actions.githubusercontent.com/ready` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/session` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/oauth2/token` | 9 | 9 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 8 | 8 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 8 | 8 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepSummarySignedBlobURL` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 8 | 8 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/summaries/summary-{guid}.md` | 1 | 1 |

## Endpoint sequence diff

## 105-command-logs-annotations: FAIL: 52 contract differences found.

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/health` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 12 | 14 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 48 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1` | 0 | 8 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 8 | 8 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools?poolType=Automation` | 8 | 8 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 1 | 1 |
| `GET run.actions.githubusercontent.com/health` | 1 | 1 |
| `GET token.actions.githubusercontent.com/ready` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/session` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/oauth2/token` | 9 | 9 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 5 | 5 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 5 | 5 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 5 | 5 |

## Endpoint sequence diff

## 106-cache-artifact-pipeline: FAIL: 53 contract differences found.

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/health` | 0 | 1 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 2 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 12 | 13 ⚠ |
| `GET codeload.github.com/actions/cache/legacy.tar.gz/ea165f8d65b6e75b540449e92b4886f43607fa02` | 0 | 1 ⚠ |
| `GET codeload.github.com/actions/upload-artifact/legacy.tar.gz/0057852bfaa89a56745cba8c7296529d2fc39830` | 0 | 1 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 48 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1` | 0 | 8 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 8 | 8 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools?poolType=Automation` | 8 | 8 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 1 | 1 |
| `GET run.actions.githubusercontent.com/health` | 0 | 1 ⚠ |
| `GET token.actions.githubusercontent.com/ready` | 0 | 1 ⚠ |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/session` | 8 | 8 |
| `POST launch.actions.githubusercontent.com/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 1 | 1 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/oauth2/token` | 9 | 9 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 4 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 1 | 4 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 1 | 4 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 1 | 4 ⚠ |

## Endpoint sequence diff

## 107-remote-action-resolution: FAIL: 52 contract differences found.

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/health` | 0 | 1 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 2 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 12 | 15 ⚠ |
| `GET codeload.github.com/Bnjoroge1/aksh-conformance/legacy.tar.gz/4bfcaee40744bf9a0a7555d66088f64bf35a963a` | 0 | 1 ⚠ |
| `GET codeload.github.com/actions/checkout/legacy.tar.gz/34e114876b0b11c390a56381ad16ebd13914f8d5` | 0 | 1 ⚠ |
| `GET codeload.github.com/actions/checkout/tar.gz/34e114876b0b11c390a56381ad16ebd13914f8d5` | 0 | 1 ⚠ |
| `GET codeload.github.com/actions/github-script/legacy.tar.gz/d3f86a106a0bac45b974a628896c90dbdf5c8093` | 0 | 1 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 48 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1` | 0 | 8 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 8 | 8 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools?poolType=Automation` | 8 | 8 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 1 | 1 |
| `GET run.actions.githubusercontent.com/health` | 0 | 1 ⚠ |
| `GET token.actions.githubusercontent.com/ready` | 0 | 1 ⚠ |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/session` | 8 | 8 |
| `POST launch.actions.githubusercontent.com/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 1 | 1 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/oauth2/token` | 9 | 9 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 4 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 1 | 5 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 1 | 5 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 1 | 5 ⚠ |

## Endpoint sequence diff

## 108-environment-shell-filesystem: FAIL: 53 contract differences found.

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/health` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 12 | 13 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 48 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1` | 0 | 8 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 8 | 8 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools?poolType=Automation` | 8 | 8 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 1 | 1 |
| `GET run.actions.githubusercontent.com/health` | 1 | 1 |
| `GET token.actions.githubusercontent.com/ready` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/session` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/oauth2/token` | 9 | 9 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 5 | 5 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 5 | 5 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 5 | 5 |

## Endpoint sequence diff

## 109-dag-matrix-scheduler: FAIL: 96 contract differences found.

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 7 | 7 |
| `GET broker.actions.githubusercontent.com/health` | 6 | 7 ⚠ |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 7 | 7 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 14 | 16 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 48 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1&lastChangeId={volatile}&lastChangeId64={volatile}` | 0 | 8 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 8 | 8 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools?poolType=Automation` | 8 | 8 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 7 | 7 |
| `GET run.actions.githubusercontent.com/health` | 6 | 7 ⚠ |
| `GET token.actions.githubusercontent.com/ready` | 6 | 7 ⚠ |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 7 | 7 |
| `POST broker.actions.githubusercontent.com/session` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/oauth2/token` | 15 | 15 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 8 | 9 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 7 | 7 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 21 | 21 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 7 | 7 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 21 | 21 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 7 | 7 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 7 | 7 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 7 | 7 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 7 | 7 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 21 | 21 |

## Endpoint sequence diff

## 110-synthetic-workspace-checkout: FAIL: 50 contract differences found.

## Endpoint counts

| endpoint | official | aksh |
|---|---:|---:|
| `DELETE broker.actions.githubusercontent.com/session` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/health` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 1 | 1 |
| `GET broker.actions.githubusercontent.com/message?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate={volatile}` | 12 | 14 ⚠ |
| `GET codeload.github.com/Bnjoroge1/aksh-conformance/legacy.tar.gz/4bfcaee40744bf9a0a7555d66088f64bf35a963a` | 1 | 1 |
| `GET codeload.github.com/actions/checkout/tar.gz/34e114876b0b11c390a56381ad16ebd13914f8d5` | 1 | 1 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=0&lastChangeId={volatile}&lastChangeId64={volatile}` | 48 | 0 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/connectionData?connectOptions=1` | 0 | 8 ⚠ |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents?agentName={volatile}&includeCapabilities=False` | 8 | 8 |
| `GET pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools?poolType=Automation` | 8 | 8 |
| `GET results-receiver.actions.githubusercontent.com/_ws/ingest.sock` | 1 | 1 |
| `GET run.actions.githubusercontent.com/health` | 1 | 1 |
| `GET token.actions.githubusercontent.com/ready` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/acknowledge?sessionId={volatile}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 1 |
| `POST broker.actions.githubusercontent.com/session` | 8 | 8 |
| `POST launch.actions.githubusercontent.com/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 1 | 1 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/distributedtask/pools//{n}/agents` | 8 | 8 |
| `POST pipelinesghubeus11.actions.githubusercontent.com/VgCrQiPH6Pm1oZbc5WhpIqR4yO3Cd3aE6IQSVXV3HKeN28m8DA/_apis/oauth2/token` | 9 | 9 |
| `POST results-receiver.actions.githubusercontent.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 2 | 3 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 5 | 4 ⚠ |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 1 |
| `POST results-receiver.actions.githubusercontent.com/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 5 | 4 ⚠ |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/acquirejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/completejob` | 1 | 1 |
| `POST run-actions-{n}-azure-eastus.actions.githubusercontent.com//{n}/renewjob` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt` | 1 | 1 |
| `PUT productionresultssa{n}.blob.core.windows.net/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt` | 5 | 4 ⚠ |

## Endpoint sequence diff

---
**Total**: 10 scenarios with flow captures compared
