# MITM comparison: 63-mega-runner-stress

**official**: ok — 363 flows
**aksh**: ok — 401 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/session` | 9 | 0 | 44.3 | - | 204, 204, 204, 204, 204, 204, 204, 204, 204 |  |
| DELETE | `/session?sessionId={guid}` | 0 | 4 | - | 30.6 |  | 204, 204, 204, 204 |
| GET | `/_apis/connectionData?connectOptions={n}` | 0 | 9 | - | 63.7 |  | 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 54 | 0 | 25.8 | - | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-1-93803&includeCapabilities=False` | 0 | 1 | - | 25.1 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-2-93803&includeCapabilities=False` | 0 | 1 | - | 22.5 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-3-93803&includeCapabilities=False` | 0 | 1 | - | 105.1 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-4-93803&includeCapabilities=False` | 0 | 1 | - | 21.3 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-5-93803&includeCapabilities=False` | 0 | 1 | - | 21.5 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-6-93803&includeCapabilities=False` | 0 | 1 | - | 23.7 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-7-93803&includeCapabilities=False` | 0 | 1 | - | 22.0 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-8-93803&includeCapabilities=False` | 0 | 1 | - | 23.4 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-9-93803&includeCapabilities=False` | 0 | 1 | - | 24.0 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-1-70609&includeCapabilities=False` | 1 | 0 | 105.3 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-2-70609&includeCapabilities=False` | 1 | 0 | 26.2 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-3-70609&includeCapabilities=False` | 1 | 0 | 23.4 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-4-70609&includeCapabilities=False` | 1 | 0 | 24.1 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-5-70609&includeCapabilities=False` | 1 | 0 | 101.2 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-6-70609&includeCapabilities=False` | 1 | 0 | 95.2 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-7-70609&includeCapabilities=False` | 1 | 0 | 21.0 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-8-70609&includeCapabilities=False` | 1 | 0 | 21.3 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-9-70609&includeCapabilities=False` | 1 | 0 | 105.3 | - | 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 9 | 9 | 86.0 | 36.9 | 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| GET | `/_ws/ingest.sock` | 5 | 4 | 30.8 | 28.9 | 101, 101, 101, 101, 101 | 101, 101, 101, 101 |
| GET | `/actions/checkout/tar.gz/***REDACTED***` | 2 | 2 | 137.7 | 163.6 | 200, 200 | 200, 200 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 9 | - | 1047.5 |  | 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 9 | - | 1076.3 |  | 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| GET | `/health` | 8 | 8 | 27.0 | 46.9 | 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 5 | 25 | 0 | 0 | None, None, None, None, None | None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 105 | 178 | 46758.5 | 246.9 | 200, 200, 200, 200, 200, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, None, None, None, None | 200, 200, 200, 200, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None |
| GET | `/ready` | 4 | 4 | 21.4 | 57.3 | 204, 204, 204, 204 | 204, 204, 204, 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 9 | 9 | 72.0 | 79.8 | 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/_apis/oauth2/token` | 14 | 13 | 98.9 | 86.0 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 5 | 4 | 41.5 | 44.0 | 200, 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 2 | 2 | 279.9 | 93.4 | 200, 200 | 200, 200 |
| POST | `/actions/runner-registration` | 9 | 9 | 252.6 | 211.4 | 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/session` | 9 | 9 | 38.4 | 62.8 | 201, 201, 201, 201, 201, 201, 201, 201, 201 | 201, 201, 201, 201, 201, 201, 201, 201, 201 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 15 | 7 | 49.0 | 61.3 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 5 | 4 | 55.5 | 35.9 | 200, 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 20 | 18 | 70.1 | 109.1 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 5 | 4 | 31.8 | 57.0 | 200, 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 20 | 18 | 52.9 | 67.8 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/{n}/acquirejob` | 5 | 4 | 410.0 | 374.7 | 200, 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/{n}/completejob` | 5 | 4 | 38.7 | 55.4 | 204, 204, 204, 204, 204 | 204, 204, 204, 204 |
| POST | `/{n}/renewjob` | 5 | 4 | 39.7 | 44.6 | 200, 200, 200, 200, 200 | 200, 200, 200, 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T04%3A54%3A26Z&sig=3b3kvlGVdMaUF2IeMbEyyctgf3R9Q%2F%2FPHnD3Rd1Tb7c%3D&ske=2026-07-10T07%3A44%3A50Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A50Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A21Z&sv=2025-11-05` | 1 | 0 | 84.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T04%3A54%3A35Z&sig=%***REDACTED***%3D&ske=2026-07-10T07%3A23%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A18Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A30Z&sv=2025-11-05` | 1 | 0 | 23.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T04%3A54%3A59Z&sig=KH%2Bj0Ndfv2oh%***REDACTED***%2FY%3D&ske=2026-07-10T07%3A23%3A49Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A49Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A54Z&sv=2025-11-05` | 1 | 0 | 26.8 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T04%3A55%3A08Z&sig=H97W6tTECa1aXqaT46ik2HVnivQMX%2F1p9zPN1IlEZgY%3D&ske=2026-07-10T07%3A36%3A12Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A36%3A12Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A03Z&sv=2025-11-05` | 1 | 0 | 31.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T04%3A55%3A12Z&sig=cl9rXsWK5svFq%2F7USCeB79ofzQLmI3Jvj%2FVll6Mho0E%3D&ske=2026-07-10T07%3A44%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A18Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A07Z&sv=2025-11-05` | 1 | 0 | 21.4 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T05%3A19%3A12Z&sig=EyrAu5a3kVJBr%2BPt2BtFDM2S%2FxsvqbRrII9kaLbzTs0%3D&ske=2026-07-10T07%3A43%3A52Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A52Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A07Z&sv=2025-11-05` | 0 | 1 | - | 82.5 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T05%3A19%3A41Z&sig=GrgDBYg%2B9juZsNO5nUemnvZtIFL%2BQKK9BWXG6Uh5q2s%3D&ske=2026-07-10T07%3A43%3A13Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A36Z&sv=2025-11-05` | 0 | 1 | - | 41.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T05%3A19%3A48Z&sig=uCkYHc3xtd2HD8k5JGH5%2B4wArm79rBsnD%2FumKrSldPM%3D&ske=2026-07-10T07%3A44%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A24Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A43Z&sv=2025-11-05` | 0 | 1 | - | 41.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T05%3A19%3A53Z&sig=QjbTD2%2Bm0Nl%2FuV%***REDACTED***%3D&ske=2026-07-10T07%3A35%3A56Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A35%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A48Z&sv=2025-11-05` | 0 | 1 | - | 81.5 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-__post_{guid}.txt?se=2026-07-10T05%3A19%3A40Z&sig=3BiwIdfcIC91ollCVw7qTM2h%2BTT5gkqiSgYgoPWY220%3D&ske=2026-07-10T07%3A44%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A18Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A35Z&sv=2025-11-05` | 0 | 1 | - | 38.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-__post_{guid}.txt?se=2026-07-10T05%3A19%3A48Z&sig=FzeoVv%***REDACTED***%2Bz04l8%3D&ske=2026-07-10T07%3A43%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A43Z&sv=2025-11-05` | 0 | 1 | - | 32.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A26Z&sig=Gs8ihUpmozpWA3KAJYgjBg%2BGXNPlkTQwuutST%2Bvjz1Y%3D&ske=2026-07-10T07%3A23%3A27Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A27Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A21Z&sv=2025-11-05` | 1 | 0 | 24.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A26Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A37%3A43Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A37%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A21Z&sv=2025-11-05` | 1 | 0 | 23.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A26Z&sig=jT5z3XAXv%2FDL%***REDACTED***%3D&ske=2026-07-10T07%3A44%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A21Z&sv=2025-11-05` | 1 | 0 | 25.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A35Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A35%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A35%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A30Z&sv=2025-11-05` | 1 | 0 | 25.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A35Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A36%3A46Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A36%3A46Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A30Z&sv=2025-11-05` | 1 | 0 | 26.2 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A35Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A23%3A05Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A05Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A30Z&sv=2025-11-05` | 1 | 0 | 165.5 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A59Z&sig=1ynhWsOJzjWzabj3W%2FqvrFeUh6FrcOCJrIek7XirEhw%3D&ske=2026-07-10T07%3A36%3A01Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A36%3A01Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A54Z&sv=2025-11-05` | 1 | 0 | 26.9 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A59Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A22%3A46Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A46Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A54Z&sv=2025-11-05` | 1 | 0 | 27.6 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A59Z&sig=TmqKyTgW%2BGzmr8TXwINW6ZndcXhdL1e3IO%2F7pVCYWKs%3D&ske=2026-07-10T07%3A23%3A49Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A49Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A54Z&sv=2025-11-05` | 1 | 0 | 175.5 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A06Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A22%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A01Z&sv=2025-11-05` | 1 | 0 | 28.8 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A06Z&sig=JMnN%***REDACTED***%2Fc%3D&ske=2026-07-10T07%3A22%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A01Z&sv=2025-11-05` | 1 | 0 | 81.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A06Z&sig=SLG%2BwE8Chqw4g4ux%2B8dtn%2F36CjQcVoIhre4Mu4HlSrM%3D&ske=2026-07-10T07%3A22%3A53Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A01Z&sv=2025-11-05` | 1 | 0 | 84.6 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A06Z&sig=bvpZysgvExd979wxjMMpVFh%2FoTIJtJTs0C6NP9N7n4Y%3D&ske=2026-07-10T07%3A36%3A48Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A36%3A48Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A01Z&sv=2025-11-05` | 1 | 0 | 159.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A22%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A15Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A02Z&sv=2025-11-05` | 1 | 0 | 22.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A07Z&sig=OHPMBq%2Fq4GazagnCfxt4%2BBNMgQLpS%2B8bVS936NRysMw%3D&ske=2026-07-10T07%3A23%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A02Z&sv=2025-11-05` | 1 | 0 | 149.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A36%3A04Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A36%3A04Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A02Z&sv=2025-11-05` | 1 | 0 | 80.8 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A07Z&sig=***REDACTED***%2BgEEpc%3D&ske=2026-07-10T07%3A22%3A56Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A02Z&sv=2025-11-05` | 1 | 0 | 78.3 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A11Z&sig=j1JKez2e1%***REDACTED***%3D&ske=2026-07-10T07%3A43%3A13Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A06Z&sv=2025-11-05` | 1 | 0 | 111.9 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A31Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A31Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A06Z&sv=2025-11-05` | 1 | 0 | 26.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A12Z&sig=3JAMUE4E9TPrhy2%2F3pvJXXK5Xb0Z1t%2BQLuU8%2BoOGyzM%3D&ske=2026-07-10T07%3A44%3A19Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A19Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A07Z&sv=2025-11-05` | 1 | 0 | 79.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A42Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A42Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A06Z&sv=2025-11-05` | 0 | 1 | - | 100.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A11Z&sig=crS8v%***REDACTED***%2Fq5h7HHXA%3D&ske=2026-07-10T07%3A44%3A46Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A46Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A06Z&sv=2025-11-05` | 0 | 1 | - | 38.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A42Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A42Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A06Z&sv=2025-11-05` | 0 | 1 | - | 122.8 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A15Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A06Z&sv=2025-11-05` | 0 | 1 | - | 40.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A40Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A53Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A35Z&sv=2025-11-05` | 0 | 1 | - | 30.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A41Z&sig=ITUPDQXhp9XrCn9g2nyGTDJyJQ%2BilA15AeFBZLcn7a4%3D&ske=2026-07-10T07%3A44%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A36Z&sv=2025-11-05` | 0 | 1 | - | 81.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A45Z&sig=***REDACTED***%2FI%3D&ske=2026-07-10T07%3A43%3A37Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A37Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A40Z&sv=2025-11-05` | 0 | 1 | - | 56.2 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A45Z&sig=tzoPSRQ%2FRbXFXOZ%2BWly7mNr5Af8IrEQruLtgHG5XvDk%3D&ske=2026-07-10T07%3A43%3A38Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A38Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A40Z&sv=2025-11-05` | 0 | 1 | - | 81.5 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A46Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A39Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A39Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A41Z&sv=2025-11-05` | 0 | 1 | - | 87.2 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A46Z&sig=wyc96CEdBFtFHnS%2BW3KRJ%2FGDMH82nbfn0BLs4uDtqJQ%3D&ske=2026-07-10T07%3A43%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A41Z&sv=2025-11-05` | 0 | 1 | - | 618.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A47Z&sig=BZRd5BtBAx1TtmdO%2BoBFLRJ7XzIlUx%2BiJ3ooqzimqTY%3D&ske=2026-07-10T07%3A43%3A50Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A50Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A42Z&sv=2025-11-05` | 0 | 1 | - | 78.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A47Z&sig=hs278nRu%***REDACTED***%3D&ske=2026-07-10T07%3A43%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A42Z&sv=2025-11-05` | 0 | 1 | - | 78.1 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A48Z&sig=5h1kEvjH8k4P%2F%2F3f6%2BWtBEygXw0SMyghCFcmZMXoHTU%3D&ske=2026-07-10T07%3A44%3A23Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A23Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A43Z&sv=2025-11-05` | 0 | 1 | - | 169.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A52Z&sig=Q6YCx8J5kU2aXWSf9M%2BEm6H%2BNGgIUHpLlgGGKEW3Uqg%3D&ske=2026-07-10T07%3A24%3A34Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A24%3A34Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A47Z&sv=2025-11-05` | 0 | 1 | - | 35.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A53Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A35%3A46Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A35%3A46Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A48Z&sv=2025-11-05` | 0 | 1 | - | 83.5 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A53Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A35%3A58Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A35%3A58Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A48Z&sv=2025-11-05` | 0 | 1 | - | 153.3 |  | 201 |

## Missing endpoints

### official only

- `DELETE /session`
- `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-1-70609&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-2-70609&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-3-70609&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-4-70609&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-5-70609&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-6-70609&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-7-70609&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-8-70609&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-63-mega-runner-stress-9-70609&includeCapabilities=False`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T04%3A54%3A26Z&sig=3b3kvlGVdMaUF2IeMbEyyctgf3R9Q%2F%2FPHnD3Rd1Tb7c%3D&ske=2026-07-10T07%3A44%3A50Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A50Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A21Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T04%3A54%3A35Z&sig=%***REDACTED***%3D&ske=2026-07-10T07%3A23%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A18Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A30Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T04%3A54%3A59Z&sig=KH%2Bj0Ndfv2oh%***REDACTED***%2FY%3D&ske=2026-07-10T07%3A23%3A49Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A49Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A54Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T04%3A55%3A08Z&sig=H97W6tTECa1aXqaT46ik2HVnivQMX%2F1p9zPN1IlEZgY%3D&ske=2026-07-10T07%3A36%3A12Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A36%3A12Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A03Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T04%3A55%3A12Z&sig=cl9rXsWK5svFq%2F7USCeB79ofzQLmI3Jvj%2FVll6Mho0E%3D&ske=2026-07-10T07%3A44%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A18Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A07Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A26Z&sig=Gs8ihUpmozpWA3KAJYgjBg%2BGXNPlkTQwuutST%2Bvjz1Y%3D&ske=2026-07-10T07%3A23%3A27Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A27Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A21Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A26Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A37%3A43Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A37%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A21Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A26Z&sig=jT5z3XAXv%2FDL%***REDACTED***%3D&ske=2026-07-10T07%3A44%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A21Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A35Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A35%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A35%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A30Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A35Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A36%3A46Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A36%3A46Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A30Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A35Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A23%3A05Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A05Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A30Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A59Z&sig=1ynhWsOJzjWzabj3W%2FqvrFeUh6FrcOCJrIek7XirEhw%3D&ske=2026-07-10T07%3A36%3A01Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A36%3A01Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A54Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A59Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A22%3A46Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A46Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A54Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A54%3A59Z&sig=TmqKyTgW%2BGzmr8TXwINW6ZndcXhdL1e3IO%2F7pVCYWKs%3D&ske=2026-07-10T07%3A23%3A49Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A49Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A54%3A54Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A06Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A22%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A01Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A06Z&sig=JMnN%***REDACTED***%2Fc%3D&ske=2026-07-10T07%3A22%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A01Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A06Z&sig=SLG%2BwE8Chqw4g4ux%2B8dtn%2F36CjQcVoIhre4Mu4HlSrM%3D&ske=2026-07-10T07%3A22%3A53Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A01Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A06Z&sig=bvpZysgvExd979wxjMMpVFh%2FoTIJtJTs0C6NP9N7n4Y%3D&ske=2026-07-10T07%3A36%3A48Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A36%3A48Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A01Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A22%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A15Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A02Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A07Z&sig=OHPMBq%2Fq4GazagnCfxt4%2BBNMgQLpS%2B8bVS936NRysMw%3D&ske=2026-07-10T07%3A23%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A23%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A02Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A36%3A04Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A36%3A04Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A02Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A07Z&sig=***REDACTED***%2BgEEpc%3D&ske=2026-07-10T07%3A22%3A56Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A22%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A02Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A11Z&sig=j1JKez2e1%***REDACTED***%3D&ske=2026-07-10T07%3A43%3A13Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A31Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A31Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T04%3A55%3A12Z&sig=3JAMUE4E9TPrhy2%2F3pvJXXK5Xb0Z1t%2BQLuU8%2BoOGyzM%3D&ske=2026-07-10T07%3A44%3A19Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A19Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A07Z&sv=2025-11-05`

### aksh only

- `DELETE /session?sessionId={guid}`
- `GET /_apis/connectionData?connectOptions={n}`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-1-93803&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-2-93803&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-3-93803&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-4-93803&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-5-93803&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-6-93803&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-7-93803&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-8-93803&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-63-mega-runner-stress-9-93803&includeCapabilities=False`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T05%3A19%3A12Z&sig=EyrAu5a3kVJBr%2BPt2BtFDM2S%2FxsvqbRrII9kaLbzTs0%3D&ske=2026-07-10T07%3A43%3A52Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A52Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A07Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T05%3A19%3A41Z&sig=GrgDBYg%2B9juZsNO5nUemnvZtIFL%2BQKK9BWXG6Uh5q2s%3D&ske=2026-07-10T07%3A43%3A13Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A36Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T05%3A19%3A48Z&sig=uCkYHc3xtd2HD8k5JGH5%2B4wArm79rBsnD%2FumKrSldPM%3D&ske=2026-07-10T07%3A44%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A24Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A43Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T05%3A19%3A53Z&sig=QjbTD2%2Bm0Nl%2FuV%***REDACTED***%3D&ske=2026-07-10T07%3A35%3A56Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A35%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A48Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-__post_{guid}.txt?se=2026-07-10T05%3A19%3A40Z&sig=3BiwIdfcIC91ollCVw7qTM2h%2BTT5gkqiSgYgoPWY220%3D&ske=2026-07-10T07%3A44%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A18Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A35Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-__post_{guid}.txt?se=2026-07-10T05%3A19%3A48Z&sig=FzeoVv%***REDACTED***%2Bz04l8%3D&ske=2026-07-10T07%3A43%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A43Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A42Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A42Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A11Z&sig=crS8v%***REDACTED***%2Fq5h7HHXA%3D&ske=2026-07-10T07%3A44%3A46Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A46Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A42Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A42Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A15Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A40Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A53Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A35Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A41Z&sig=ITUPDQXhp9XrCn9g2nyGTDJyJQ%2BilA15AeFBZLcn7a4%3D&ske=2026-07-10T07%3A44%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A36Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A45Z&sig=***REDACTED***%2FI%3D&ske=2026-07-10T07%3A43%3A37Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A37Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A40Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A45Z&sig=tzoPSRQ%2FRbXFXOZ%2BWly7mNr5Af8IrEQruLtgHG5XvDk%3D&ske=2026-07-10T07%3A43%3A38Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A38Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A40Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A46Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A43%3A39Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A39Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A41Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A46Z&sig=wyc96CEdBFtFHnS%2BW3KRJ%2FGDMH82nbfn0BLs4uDtqJQ%3D&ske=2026-07-10T07%3A43%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A41Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A47Z&sig=BZRd5BtBAx1TtmdO%2BoBFLRJ7XzIlUx%2BiJ3ooqzimqTY%3D&ske=2026-07-10T07%3A43%3A50Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A50Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A42Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A47Z&sig=hs278nRu%***REDACTED***%3D&ske=2026-07-10T07%3A43%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A43%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A42Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A48Z&sig=5h1kEvjH8k4P%2F%2F3f6%2BWtBEygXw0SMyghCFcmZMXoHTU%3D&ske=2026-07-10T07%3A44%3A23Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A44%3A23Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A43Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A52Z&sig=Q6YCx8J5kU2aXWSf9M%2BEm6H%2BNGgIUHpLlgGGKEW3Uqg%3D&ske=2026-07-10T07%3A24%3A34Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A24%3A34Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A47Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A53Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A35%3A46Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A35%3A46Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A48Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T05%3A19%3A53Z&sig=***REDACTED***%3D&ske=2026-07-10T07%3A35%3A58Z&skoid={guid}&sks=b&skt=2026-07-10T03%3A35%3A58Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A48Z&sv=2025-11-05`

## Per-endpoint comparison

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{'accept-encoding', 'x-tfs-fedauthredirect'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -10,7 +10,7 @@
       "isInternal": true,
       "name": "Default",
       "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 23,
+      "size": 26,
       "targetSize": null
     },
     {
```

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 29.8 / aksh 25.5 | p95: official 387.6 / aksh 80.3

### `GET /_ws/ingest.sock`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [101, 101, 101, 101, 101] | aksh: [101, 101, 101, 101]

**Timing (ms):** p50: official 23.6 / aksh 26.0 | p95: official 50.5 / aksh 47.6

### `GET /actions/checkout/tar.gz/***REDACTED***`

**Header key differences:**

- official only: `{'authorization'}`
- aksh only: `{'accept'}`

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 143.7 / aksh 227.4 | p95: official 143.7 / aksh 227.4

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 29.6 / aksh 32.6 | p95: official 39.5 / aksh 146.4

### `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Status codes:** official: [None, None, None, None, None] | aksh: [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]

**Timing (ms):** p50: official 0.0 / aksh 0.0 | p95: official 0.0 / aksh 0.0

### `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Status codes:** official: [200, 200, 200, 200, 200, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, 202, None, None, None, None] | aksh: [200, 200, 200, 200, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]

**Timing (ms):** p50: official 50045.5 / aksh 0.0 | p95: official 50095.4 / aksh 0.0

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204, 204, 204, 204] | aksh: [204, 204, 204, 204]

**Timing (ms):** p50: official 23.5 / aksh 30.7 | p95: official 27.8 / aksh 142.5

### `POST /_apis/distributedtask/pools/{n}/agents`

**Header key differences:**

- official only: `{'accept-encoding', 'x-tfs-fedauthredirect'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,11 +2,11 @@
   "authorization": {
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "***REDACTED***/On5fxXzV3Q6LC9fZoUa0ZvjZ/bPLnxswL32u+9rKwqLDOMbd6OX+1UOwayPhLIwoI0yF/PM//oLtB0EbuQL8E+***REDACTED***/TA6lzhZy5pShMKR+***REDACTED***+IOlbtIMmEPKl5z+vi/***REDACTED***=="
+      "modulus": "8ReQ71s1iV3rqEzZCiB1+cnh4Te+***REDACTED***/***REDACTED***+***REDACTED***+***REDACTED***/***REDACTED***=="
     }
   },
   "createdOn": "0001-01-01T00:00:00",
-  "disableUpdate": false,
+  "disableUpdate": true,
   "ephemeral": true,
   "id": 0,
   "labels": [
@@ -47,7 +47,7 @@
     }
   ],
   "maxParallelism": 1,
-  "name": "ephemeral-official-63-mega-runner-stress-1-70609",
+  "name": "ephemeral-aksh-63-mega-runner-stress-1-93803",
   "osDescription": "Ubuntu 24.04.4 LTS",
   "provisioningState": "Provisioned",
   "status": 0,
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,18 +1,18 @@
 {
   "authorization": {
     "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/_apis/oauth2/token",
-    "clientId": "426cce12-2c76-4960-9cac-35246c636245",
+    "clientId": "2fa5a9e2-3afa-49f2-83bd-fbba759d20f9",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "***REDACTED***/On5fxXzV3Q6LC9fZoUa0ZvjZ/bPLnxswL32u+9rKwqLDOMbd6OX+1UOwayPhLIwoI0yF/PM//oLtB0EbuQL8E+***REDACTED***/TA6lzhZy5pShMKR+***REDACTED***+IOlbtIMmEPKl5z+vi/***REDACTED***=="
+      "modulus": "8ReQ71s1iV3rqEzZCiB1+cnh4Te+***REDACTED***/***REDACTED***+***REDACTED***+***REDACTED***/***REDACTED***=="
     }
   },
-  "createdOn": "2026-07-10T03:53:02.957Z",
+  "createdOn": "2026-07-10T04:17:48.307Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
   "ephemeral": true,
-  "id": 831,
+  "id": 840,
   "isElastic": false,
   "isVirtual": false,
   "labels": [
@@ -43,7 +43,7 @@
     }
   ],
   "maxParallelism": 1,
-  "name": "ephemeral-official-63-mega-runner-stress-1-70609",
+  "name": "ephemeral-aksh-63-mega-runner-stress-1-93803",
   "osDescription": "Ubuntu 24.04.4 LTS",
   "owningTenant": null,
   "properties": {
@@ -65,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-831",
+  "queueName": "taskagent-840",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 69.8 / aksh 69.4 | p95: official 84.4 / aksh 156.5

### `POST /_apis/oauth2/token`

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

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 87.9 / aksh 84.7 | p95: official 154.9 / aksh 96.4

### `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "runnerRequestId": "6fa32102-5d50-5dc0-a030-964bc88a770c"
+  "runnerRequestId": "32a62b67-44ad-51fe-9060-8d129854d2f3"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 41.0 / aksh 45.3 | p95: official 45.7 / aksh 51.0

### `POST /actions/build/{guid}/jobs/{guid}/runnerresolve/actions`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

_identical_

**Response body diff:**

_identical_

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 434.4 / aksh 103.1 | p95: official 434.4 / aksh 103.1

### `POST /actions/runner-registration`

**Header key differences:**

- aksh only: `{'accept'}`

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
   "url": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 193.1 / aksh 192.2 | p95: official 614.8 / aksh 293.5

### `POST /session`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,14 +1,14 @@
 {
   "agent": {
     "ephemeral": null,
-    "id": 831,
-    "name": "ephemeral-official-63-mega-runner-stress-1-70609",
+    "id": 840,
+    "name": "ephemeral-aksh-63-mega-runner-stress-1-93803",
     "osDescription": "Ubuntu 24.04.4 LTS",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "container (PID: 92)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 30)",
+  "sessionId": "4c2270e8-23a3-4c10-b849-4cda12aab752",
   "useFipsEncryption": false
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,6 +1,6 @@
 {
   "assignmentQueued": false,
   "orchestrationId": "",
-  "ownerName": "container (PID: 92)",
-  "sessionId": "b432fb80-8746-467d-8ac1-09e906075a2a"
+  "ownerName": "container (PID: 30)",
+  "sessionId": "a188be6d-7b23-4aad-bc34-ee2e7d7bb068"
 }
```

**Status codes:** official: [201, 201, 201, 201, 201, 201, 201, 201, 201] | aksh: [201, 201, 201, 201, 201, 201, 201, 201, 201]

**Timing (ms):** p50: official 36.8 / aksh 41.1 | p95: official 53.2 / aksh 134.4

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,51 +2,24 @@
   "change_order": 1,
   "steps": [
     {
-      "completed_at": "2026-07-10T03:55:10.166Z",
+      "completed_at": "2026-07-10T04:19:52.207Z",
       "conclusion": 2,
-      "external_id": "958844f7-aa12-431c-b118-346351ebd10f",
+      "external_id": "ce503fa7-3310-4e8d-9e67-32bc608096ce",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-07-10T03:55:10.136Z",
+      "started_at": "2026-07-10T04:19:52.207Z",
       "status": 6
     },
     {
-      "completed_at": "2026-07-10T03:55:10.214Z",
+      "completed_at": "2026-07-10T04:19:52.884Z",
       "conclusion": 3,
-      "external_id": "46405566-a0e1-46a5-8e4f-6831bf7d9c25",
+      "external_id": "bec6535f-2d2e-4a57-8b6e-1e3e427c3b25",
       "name": "Check upstream job results",
       "number": 2,
-      "started_at": "2026-07-10T03:55:10.171Z",
-      "status": 6
-    },
-    {
-      "completed_at": "2026-07-10T03:55:10.215Z",
-      "conclusion": 7,
-      "external_id": "4f1d011b-752c-4ec3-9766-50af529b02f9",
-      "name": "Check propagated outputs",
-      "number": 3,
-      "started_at": "2026-07-10T03:55:10.215Z",
-      "status": 6
-    },
-    {
-      "completed_at": "2026-07-10T03:55:10.215Z",
-      "conclusion": 7,
-      "external_id": "450a5d2f-e451-49aa-8791-26c222ed085c",
-      "name": "Write final summary",
-      "number": 4,
-      "started_at": "2026-07-10T03:55:10.215Z",
-      "status": 6
-    },
-    {
-      "completed_at": "2026-07-10T03:55:10.509Z",
-      "conclusion": 2,
-      "external_id": "e0647ccd-e0ab-4a7d-9428-25a8270cd753",
-      "name": "Complete job",
-      "number": 5,
-      "started_at": "2026-07-10T03:55:10.219Z",
+      "started_at": "2026-07-10T04:19:52.869Z",
       "status": 6
     }
   ],
-  "workflow_job_run_backend_id": "6fa32102-5d50-5dc0-a030-964bc88a770c",
-  "workflow_run_backend_id": "480d2cc9-049f-4e66-9128-93fed57742d8"
+  "workflow_job_run_backend_id": "32a62b67-44ad-51fe-9060-8d129854d2f3",
+  "workflow_run_backend_id": "0cf62442-2a6f-4246-afee-138ad494ca10"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 37.9 / aksh 41.0 | p95: official 130.9 / aksh 119.8

### `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,6 +1,6 @@
 {
-  "line_count": 49,
-  "uploaded_at": "2026-07-10T03:55:12.482Z",
-  "workflow_job_run_backend_id": "6fa32102-5d50-5dc0-a030-964bc88a770c",
-  "workflow_run_backend_id": "480d2cc9-049f-4e66-9128-93fed57742d8"
+  "line_count": 18,
+  "uploaded_at": "2026-07-10T04:19:53.896Z",
+  "workflow_job_run_backend_id": "32a62b67-44ad-51fe-9060-8d129854d2f3",
+  "workflow_run_backend_id": "0cf62442-2a6f-4246-afee-138ad494ca10"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 38.3 / aksh 36.0 | p95: official 128.0 / aksh 38.8

### `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,7 +1,7 @@
 {
-  "line_count": 15,
-  "step_backend_id": "958844f7-aa12-431c-b118-346351ebd10f",
-  "uploaded_at": "2026-07-10T03:55:11.646Z",
-  "workflow_job_run_backend_id": "6fa32102-5d50-5dc0-a030-964bc88a770c",
-  "workflow_run_backend_id": "480d2cc9-049f-4e66-9128-93fed57742d8"
+  "line_count": 8,
+  "step_backend_id": "ce503fa7-3310-4e8d-9e67-32bc608096ce",
+  "uploaded_at": "2026-07-10T04:19:52.648Z",
+  "workflow_job_run_backend_id": "32a62b67-44ad-51fe-9060-8d129854d2f3",
+  "workflow_run_backend_id": "0cf62442-2a6f-4246-afee-138ad494ca10"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 44.7 / aksh 83.7 | p95: official 209.8 / aksh 213.4

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "6fa32102-5d50-5dc0-a030-964bc88a770c",
-  "workflow_run_backend_id": "480d2cc9-049f-4e66-9128-93fed57742d8"
+  "workflow_job_run_backend_id": "32a62b67-44ad-51fe-9060-8d129854d2f3",
+  "workflow_run_backend_id": "0cf62442-2a6f-4246-afee-138ad494ca10"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa7.blob.core.windows.net/actions-results/480d2cc9-049f-4e66-9128-93fed57742d8/workflow-job-run-6fa32102-5d50-5dc0-a030-964bc88a770c/logs/job/job-logs.txt?se=2026-07-10T04%3A55%3A12Z&sig=cl9rXsWK5svFq%2F7USCeB79ofzQLmI3Jvj%2FVll6Mho0E%3D&ske=2026-07-10T07%3A44%3A18Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T03%3A44%3A18Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A07Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa17.blob.core.windows.net/actions-results/0cf62442-2a6f-4246-afee-138ad494ca10/workflow-job-run-32a62b67-44ad-51fe-9060-8d129854d2f3/logs/job/job-logs.txt?se=2026-07-10T05%3A19%3A53Z&sig=QjbTD2%2Bm0Nl%2FuV%***REDACTED***%3D&ske=2026-07-10T07%3A35%3A56Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T03%3A35%3A56Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A48Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 31.6 / aksh 37.5 | p95: official 33.1 / aksh 125.6

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "958844f7-aa12-431c-b118-346351ebd10f",
-  "workflow_job_run_backend_id": "6fa32102-5d50-5dc0-a030-964bc88a770c",
-  "workflow_run_backend_id": "480d2cc9-049f-4e66-9128-93fed57742d8"
+  "step_backend_id": "ce503fa7-3310-4e8d-9e67-32bc608096ce",
+  "workflow_job_run_backend_id": "32a62b67-44ad-51fe-9060-8d129854d2f3",
+  "workflow_run_backend_id": "0cf62442-2a6f-4246-afee-138ad494ca10"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa7.blob.core.windows.net/actions-results/480d2cc9-049f-4e66-9128-93fed57742d8/workflow-job-run-6fa32102-5d50-5dc0-a030-964bc88a770c/logs/steps/step-logs-958844f7-aa12-431c-b118-346351ebd10f.txt?se=2026-07-10T04%3A55%3A11Z&sig=j1JKez2e1%***REDACTED***%3D&ske=2026-07-10T07%3A43%3A13Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T03%3A43%3A13Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T03%3A55%3A06Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa17.blob.core.windows.net/actions-results/0cf62442-2a6f-4246-afee-138ad494ca10/workflow-job-run-32a62b67-44ad-51fe-9060-8d129854d2f3/logs/steps/step-logs-ce503fa7-3310-4e8d-9e67-32bc608096ce.txt?se=2026-07-10T05%3A19%3A52Z&sig=Q6YCx8J5kU2aXWSf9M%2BEm6H%2BNGgIUHpLlgGGKEW3Uqg%3D&ske=2026-07-10T07%3A24%3A34Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T03%3A24%3A34Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T04%3A19%3A47Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 33.6 / aksh 35.5 | p95: official 168.3 / aksh 302.3

### `POST /{n}/acquirejob`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "billingOwnerId": "O_kgDOEbddog",
-  "jobMessageId": "6fa32102-5d50-5dc0-a030-964bc88a770c",
+  "jobMessageId": "32a62b67-44ad-51fe-9060-8d129854d2f3",
   "runnerOS": "Linux"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -29,11 +29,11 @@
         },
         {
           "k": "run_id",
-          "v": "29067836016"
+          "v": "29068768717"
         },
         {
           "k": "run_number",
-          "v": "37"
+          "v": "38"
         },
         {
           "k": "retention_days",
@@ -712,7 +712,7 @@
       "d": [
         {
           "k": "check_run_id",
-          "v": 86283175431
+          "v": 86285845117
         },
         {
           "k": "workflow_ref",
@@ -747,7 +747,20 @@
               {
                 "k": "outputs",
                 "v": {
-                  "d": [],
+                  "d": [
+                    {
+                      "k": "cache-prefix",
+                      "v": ""
+                    },
+                    {
+                      "k": "matrix-json",
+                      "v": ""
+                    },
+                    {
+                      "k": "plan-token",
+                      "v": ""
+                    }
+                  ],
                   "t": 2
                 }
               }
@@ -951,7 +964,7 @@
   ],
   "jobContainer": null,
   "jobDisplayName": "final-gate",
-  "jobId": "6fa32102-5d50-5dc0-a030-964bc88a770c",
+  "jobId": "32a62b67-44ad-51fe-9060-8d129854d2f3",
   "jobName": "__default",
   "jobOutputs": null,
   "jobServiceContainers": null,
@@ -1031,30 +1044,30 @@
     },
     {
       "type": "regex",
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
-      "value": "***REDACTED***\\.SF4iZyBFTvkcj2l"
-    },
-    {
-      "type": "regex",
-      "value": "fjjnvQlMmqXR-m-py-***REDACTED***"
+      "value": "***REDACTED***\\.***REDACTED***"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***\\.IYEafsTB0gSvA85"
+    },
+    {
+      "type": "regex",
+      "value": "***REDACTED***-l8wEMEV9XZadwJjmw"
     }
   ],
   "messageType": "RunnerJobRequest",
   "plan": {
     "artifactLocation": "",
     "artifactUri": "",
-    "planId": "480d2cc9-049f-4e66-9128-93fed57742d8",
+    "planId": "0cf62442-2a6f-4246-afee-138ad494ca10",
     "planType": "actions",
     "version": 0
   },
@@ -1064,7 +1077,7 @@
       {
         "authorization": {
           "parameters": {
-            "AccessToken": "***REDACTED***"
+            "AccessToken": "***REDACTED***"
           },
           "scheme": "OAuth"
         },
@@ -1081,7 +1094,7 @@
         "isReady": true,
         "isShared": false,
         "name": "SystemVssConnection",
-        "url": "https://run-actions-1-azure-eastus.actions.githubusercontent.com/25/"
+        "url": "https://run-actions-2-azure-eastus.actions.githubusercontent.com/1/"
       }
     ]
   },
@@ -1098,7 +1111,7 @@
         "lit": "Check upstream job results",
         "type": 0
       },
-      "id": "46405566-a0e1-46a5-8e4f-6831bf7d9c25",
+      "id": "bec6535f-2d2e-4a57-8b6e-1e3e427c3b25",
       "inputs": {
         "map": [
           {
@@ -1148,7 +1161,7 @@
         "lit": "Check propagated outputs",
         "type": 0
       },
-      "id": "4f1d011b-752c-4ec3-9766-50af529b02f9",
+      "id": "5a30bb25-1db0-4b83-8377-1dc8567f957b",
       "inputs": {
         "map": [
           {
@@ -1198,7 +1211,7 @@
         "lit": "Write final summary",
         "type": 0
       },
-      "id": "450a5d2f-e451-49aa-8791-26c222ed085c",
+      "id": "c0c32706-cae5-4f59-8701-c475cfd5ec19",
       "inputs": {
         "map": [
           {
@@ -1240,7 +1253,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "480d2cc9-049f-4e66-9128-93fed57742d8",
+    "id": "0cf62442-2a6f-4246-afee-138ad494ca10",
     "location": null
   },
   "variables": {
@@ -1366,7 +1379,7 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "***REDACTED******REDACTED***"
+      "value": "***REDACTED******REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
@@ -1385,13 +1398,13 @@
     },
     "system.github.token": {
       "isSecret": true,
-      "value": "***REDACTED******REDACTED***"
+      "value": "***REDACTED******REDACTED***"
     },
     "system.github.token.permissions": {
       "value": "{\"Actions\":\"read\",\"Contents\":\"read\",\"Metadata\":\"read\"}"
     },
     "system.orchestrationId": {
-      "value": "480d2cc9-049f-4e66-9128-93fed57742d8.final-gate.__default"
+      "value": "0cf62442-2a6f-4246-afee-138ad494ca10.final-gate.__default"
     },
     "system.phaseDisplayName": {
       "value": "final-gate"
```

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 439.4 / aksh 401.6 | p95: official 451.0 / aksh 431.4

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,19 +2,19 @@
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
   "conclusion": "failed",
-  "jobId": "6fa32102-5d50-5dc0-a030-964bc88a770c",
+  "jobId": "32a62b67-44ad-51fe-9060-8d129854d2f3",
   "outputs": {},
-  "planId": "480d2cc9-049f-4e66-9128-93fed57742d8",
+  "planId": "0cf62442-2a6f-4246-afee-138ad494ca10",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-07-10T03:55:10.1666595Z",
+      "completed_at": "2026-07-10T04:19:53.938Z",
       "conclusion": "succeeded",
-      "external_id": "958844f7-aa12-431c-b118-346351ebd10f",
+      "external_id": "ce503fa7-3310-4e8d-9e67-32bc608096ce",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-07-10T03:55:10.1363022Z",
+      "started_at": "2026-07-10T04:19:53.938Z",
       "status": "completed",
       "type": "runner"
     },
@@ -22,67 +22,70 @@
       "action_name": "bash",
       "annotations": [
         {
-          "endLine": 33,
+          "endLine": 1,
           "level": "failure",
           "message": "Process completed with exit code 1.",
-          "startLine": 33,
+          "startLine": 1,
+          "stepNumber": 2
+        },
+        {
+          "endLine": 1,
+          "level": "failure",
+          "message": "process exit code 1",
+          "startLine": 1,
           "stepNumber": 2
         }
       ],
-      "completed_at": "2026-07-10T03:55:10.2143996Z",
+      "completed_at": "2026-07-10T04:19:53.938Z",
       "conclusion": "failed",
-      "external_id": "46405566-a0e1-46a5-8e4f-6831bf7d9c25",
+      "external_id": "bec6535f-2d2e-4a57-8b6e-1e3e427c3b25",
       "name": "Check upstream job results",
       "number": 2,
-      "started_at": "2026-07-10T03:55:10.1711205Z",
+      "started_at": "2026-07-10T04:19:53.938Z",
       "status": "completed",
       "type": "run"
     },
     {
+      "action_name": "bash",
       "annotations": [],
-      "completed_at": "2026-07-10T03:55:10.2155145Z",
+      "completed_at": "2026-07-10T04:19:53.938Z",
       "conclusion": "skipped",
-      "external_id": "4f1d011b-752c-4ec3-9766-50af529b02f9",
+      "external_id": "5a30bb25-1db0-4b83-8377-1dc8567f957b",
       "name": "Check propagated outputs",
       "number": 3,
-      "started_at": "2026-07-10T03:55:10.2152703Z",
-      "status": "completed"
+      "started_at": "2026-07-10T04:19:53.938Z",
+      "status": "completed",
+      "type": "run"
     },
     {
+      "action_name": "bash",
       "annotations": [],
-      "completed_at": "2026-07-10T03:55:10.2157817Z",
+      "completed_at": "2026-07-10T04:19:53.938Z",
       "conclusion": "skipped",
-      "external_id": "450a5d2f-e451-49aa-8791-26c222ed085c",
+      "external_id": "c0c32706-cae5-4f59-8701-c475cfd5ec19",
       "name": "Write final summary",
       "number": 4,
-      "started_at": "2026-07-10T03:55:10.2156132Z",
-      "status": "completed"
+      "started_at": "2026-07-10T04:19:53.938Z",
+      "status": "completed",
+      "type": "run"
     },
     {
       "action_name": "complete_job",
       "annotations": [],
-      "completed_at": "2026-07-10T03:55:10.5094982Z",
+      "completed_at": "2026-07-10T04:19:53.938Z",
       "conclusion": "succeeded",
-      "external_id": "e0647ccd-e0ab-4a7d-9428-25a8270cd753",
+      "external_id": "171fe9df-fd1c-40ec-bd53-8c67d88ae58f",
       "name": "Complete job",
       "number": 5,
-      "started_at": "2026-07-10T03:55:10.2190102Z",
+      "started_at": "2026-07-10T04:19:53.938Z",
       "status": "completed",
       "type": "runner"
     }
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
+      "message": "{\"ClassType\":\"StepsRunner\",\"FinishResult\":\"failed\"}",
+      "type": "task"
     }
   ]
 }
```

**Status codes:** official: [204, 204, 204, 204, 204] | aksh: [204, 204, 204, 204]

**Timing (ms):** p50: official 32.0 / aksh 44.9 | p95: official 68.3 / aksh 103.2

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "6fa32102-5d50-5dc0-a030-964bc88a770c",
-  "planId": "480d2cc9-049f-4e66-9128-93fed57742d8"
+  "jobId": "32a62b67-44ad-51fe-9060-8d129854d2f3",
+  "planId": "0cf62442-2a6f-4246-afee-138ad494ca10"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-07-10T04:05:09.918644849Z"
+  "lockedUntil": "2026-07-10T04:29:52.240475491Z"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 36.7 / aksh 44.2 | p95: official 54.2 / aksh 65.6
