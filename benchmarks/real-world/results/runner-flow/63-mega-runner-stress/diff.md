# MITM comparison: 63-mega-runner-stress

**official**: ok — 164 flows
**aksh**: ok — 113 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| GET | `/_apis/connectionData?connectOptions={n}` | 0 | 1 | - | 146.7 |  | 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 6 | 0 | 23.0 | - | 200, 200, 200, 200, 200, 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=direct-aksh-63-mega-runner-stress-1783634656&includeCapabilities=False` | 0 | 1 | - | 22.2 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=direct-official-63-mega-runner-stress-1783634529&includeCapabilities=False` | 1 | 0 | 25.3 | - | 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 1 | 1 | 23.9 | 24.6 | 200 | 200 |
| GET | `/_ws/ingest.sock` | 5 | 1 | 28.7 | 66.9 | 101, 101, 101, 101, 101 | 101 |
| GET | `/actions/checkout/tar.gz/***REDACTED***` | 2 | 2 | 186.6 | 134.1 | 200, 200 | 200, 200 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 1 | - | 1140.6 |  | 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 1 | - | 1802.1 |  | 200 |
| GET | `/health` | 8 | 6 | 57.7 | 38.2 | 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 5 | 13 | 0.0 | 3848.7 | None, None, None, None, None | 202, None, None, None, None, None, None, None, None, None, None, None, None |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 12 | 16 | 18942.7 | 21910.4 | 200, 200, 200, 200, 200, 202, 202, 202, 202, None, None, None | 200, 200, 200, 200, 202, 202, 202, 202, 202, 202, None, None, None, None, None, None |
| GET | `/ready` | 4 | 3 | 52.7 | 39.1 | 204, 204, 204, 204 | 204, 204, 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 1 | 1 | 179.1 | 67.2 | 200 | 200 |
| POST | `/_apis/oauth2/token` | 0 | 2 | - | 115.8 |  | 200, 200 |
| POST | `/_apis/oauth2/token/{guid}` | 8 | 3 | 108.6 | 72.7 | 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 5 | 4 | 52.5 | 77.9 | 200, 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 2 | 2 | 185.1 | 198.4 | 200, 200 | 200, 200 |
| POST | `/actions/runner-registration` | 1 | 1 | 188.8 | 221.0 | 200 | 200 |
| POST | `/session` | 1 | 1 | 45.5 | 33.3 | 201 | 201 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 12 | 9 | 53.0 | 40.3 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 5 | 1 | 61.2 | 41.0 | 200, 200, 200, 200, 200 | 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 20 | 4 | 129.7 | 193.2 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 5 | 4 | 37.0 | 34.8 | 200, 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 20 | 17 | 67.9 | 52.6 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/{n}/acquirejob` | 5 | 4 | 464.7 | 437.1 | 200, 200, 200, 200, 200 | 200, 200, 200, 200 |
| POST | `/{n}/completejob` | 5 | 4 | 122.1 | 57.3 | 204, 204, 204, 204, 204 | 204, 204, 204, 204 |
| POST | `/{n}/renewjob` | 5 | 5 | 44.9 | 45.7 | 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A02%3A16Z&sig=MGF%***REDACTED***%3D&ske=2026-07-10T01%3A45%3A50Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A50Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A11Z&sv=2025-11-05` | 1 | 0 | 24.9 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A03%3A13Z&sig=t9mzZcWp0I97a082%2BGsjGyR%2FVIYwOlLrDWatY0yXBEQ%3D&ske=2026-07-10T01%3A45%3A08Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A08Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A08Z&sv=2025-11-05` | 1 | 0 | 35.2 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A03%3A17Z&sig=d357fE5nio3nv7RI%2BbQJFv3vQh%2BngeqEl%2BIbmlBPeto%3D&ske=2026-07-10T01%3A46%3A10Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A46%3A10Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A12Z&sv=2025-11-05` | 1 | 0 | 170.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A03%3A53Z&sig=ciAWnESjW9K%***REDACTED***%3D&ske=2026-07-10T01%3A46%3A09Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A46%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A48Z&sv=2025-11-05` | 1 | 0 | 30.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A03%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T01%3A45%3A57Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A57Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A53Z&sv=2025-11-05` | 1 | 0 | 24.2 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A05%3A21Z&sig=***REDACTED***%3D&ske=2026-07-09T23%3A30%3A06Z&skoid={guid}&sks=b&skt=2026-07-09T19%3A30%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A05%3A16Z&sv=2025-11-05` | 0 | 1 | - | 31.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-__post_{guid}.txt?se=2026-07-09T23%3A05%3A20Z&sig=bVDMLJ7vP2uyT8L%2FwJKylv9WkSCj8RCKjO%2Fqz6dRgDA%3D&ske=2026-07-10T01%3A25%3A59Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A25%3A59Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A05%3A15Z&sv=2025-11-05` | 0 | 1 | - | 20.6 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A02%3A15Z&sig=7IDKkSUIC6U8tp%***REDACTED***%3D&ske=2026-07-10T01%3A44%3A57Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A44%3A57Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A10Z&sv=2025-11-05` | 1 | 0 | 25.4 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A02%3A16Z&sig=%2FoBm6dUQSHL27b3ffDo67u8NIZu2%2FiEVXmJrJs%2BSJbw%3D&ske=2026-07-10T01%3A45%3A02Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A02Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A11Z&sv=2025-11-05` | 1 | 0 | 27.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A02%3A16Z&sig=HoXbsVB9%2FXym%2BhGuNffHl%2FufECuPWzELQOJ%2FYe%2FSXl8%3D&ske=2026-07-10T01%3A46%3A13Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A46%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A11Z&sv=2025-11-05` | 1 | 0 | 146.2 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A02%3A45Z&sig=***REDACTED***%2FwCeGMstnk%3D&ske=2026-07-10T01%3A45%3A49Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A49Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A40Z&sv=2025-11-05` | 1 | 0 | 19.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T01%3A45%3A49Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A49Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A06Z&sv=2025-11-05` | 1 | 0 | 75.3 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A11Z&sig=ek2jszkKT%2FktYXwu6qklgt%2Bo5LWrpfArbcanD5QrM6o%3D&ske=2026-07-10T01%3A45%3A05Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A05Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A06Z&sv=2025-11-05` | 1 | 0 | 101.4 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A11Z&sig=ooYXiR%2F2nz%***REDACTED***%3D&ske=2026-07-10T01%3A45%3A09Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A06Z&sv=2025-11-05` | 1 | 0 | 73.2 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A12Z&sig=***REDACTED***%2FOmZxJyPe8%3D&ske=2026-07-10T01%3A44%3A58Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A44%3A58Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A07Z&sv=2025-11-05` | 1 | 0 | 74.3 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A12Z&sig=jWGTXCGzFijHA8jf1Io%2B7mrQv80CIWVTX%2FDHZtYJli4%3D&ske=2026-07-10T01%3A45%3A12Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A12Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A07Z&sv=2025-11-05` | 1 | 0 | 602.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A12Z&sig=wd7%2FeqBIBt4OQQY39dc7oEco0LH%2FZYCKs5nGUfL%2Fuf8%3D&ske=2026-07-10T01%3A46%3A02Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A46%3A02Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A07Z&sv=2025-11-05` | 1 | 0 | 75.5 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A13Z&sig=32QGepctJilwEfNd3%2F69NUHoZw3dwomSWWQ8Enb0pH4%3D&ske=2026-07-10T01%3A44%3A56Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A44%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A08Z&sv=2025-11-05` | 1 | 0 | 157.3 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A13Z&sig=***REDACTED***%3D&ske=2026-07-10T01%3A45%3A35Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A35Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A08Z&sv=2025-11-05` | 1 | 0 | 37.6 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A16Z&sig=e%2ByvSo6wWj7A9o2FzP92%2F4m1DTANGbmI7ZvSKhnC0g4%3D&ske=2026-07-10T01%3A46%3A21Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A46%3A21Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A11Z&sv=2025-11-05` | 1 | 0 | 30.6 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A20Z&sig=e6p258eIRXVXP8dP%2BEOG6j1%2BA3Ja0X%2BJB65%2FydizG1Y%3D&ske=2026-07-10T01%3A45%3A13Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A15Z&sv=2025-11-05` | 1 | 0 | 24.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A50Z&sig=***REDACTED***%2FY%3D&ske=2026-07-10T01%3A45%3A14Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A45Z&sv=2025-11-05` | 1 | 0 | 28.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A52Z&sig=***REDACTED***%3D&ske=2026-07-10T00%3A39%3A45Z&skoid={guid}&sks=b&skt=2026-07-09T20%3A39%3A45Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A47Z&sv=2025-11-05` | 1 | 0 | 24.2 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A52Z&sig=***REDACTED***%3D&ske=2026-07-10T01%3A45%3A30Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A47Z&sv=2025-11-05` | 1 | 0 | 79.4 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A57Z&sig=%2F%***REDACTED***%3D&ske=2026-07-10T01%3A45%3A56Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A52Z&sv=2025-11-05` | 1 | 0 | 24.3 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A57Z&sig=YGnyc8%***REDACTED***%3D&ske=2026-07-10T01%3A45%3A17Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A17Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A52Z&sv=2025-11-05` | 1 | 0 | 76.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A57Z&sig=pktdks2GqmWoSM58a%2BbXLBCrIxCCrAqQqOsPWa4g0zI%3D&ske=2026-07-10T01%3A45%3A19Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A19Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A52Z&sv=2025-11-05` | 1 | 0 | 22.4 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A04%3A52Z&sig=3Yk6eGqytrnuS%2Bm6fQl37dTIaOfkxUDV%2Bfj4WS6d1uk%3D&ske=2026-07-10T01%3A25%3A27Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A25%3A27Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A04%3A47Z&sv=2025-11-05` | 0 | 1 | - | 72.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A05%3A20Z&sig=***REDACTED***%3D&ske=2026-07-10T01%3A38%3A00Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A38%3A00Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A05%3A15Z&sv=2025-11-05` | 0 | 1 | - | 27.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A05%3A20Z&sig=S9kjrMuTL%2BFhAzJ%2BhO%2Bp07yidkQiGKWySIAKMYSQC8Y%3D&ske=2026-07-10T01%3A24%3A39Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A24%3A39Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A05%3A15Z&sv=2025-11-05` | 0 | 1 | - | 127.6 |  | 201 |

## Missing endpoints

### official only

- `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=direct-official-63-mega-runner-stress-1783634529&includeCapabilities=False`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A02%3A16Z&sig=MGF%***REDACTED***%3D&ske=2026-07-10T01%3A45%3A50Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A50Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A11Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A03%3A13Z&sig=t9mzZcWp0I97a082%2BGsjGyR%2FVIYwOlLrDWatY0yXBEQ%3D&ske=2026-07-10T01%3A45%3A08Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A08Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A08Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A03%3A17Z&sig=d357fE5nio3nv7RI%2BbQJFv3vQh%2BngeqEl%2BIbmlBPeto%3D&ske=2026-07-10T01%3A46%3A10Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A46%3A10Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A12Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A03%3A53Z&sig=ciAWnESjW9K%***REDACTED***%3D&ske=2026-07-10T01%3A46%3A09Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A46%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A48Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A03%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T01%3A45%3A57Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A57Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A53Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A02%3A15Z&sig=7IDKkSUIC6U8tp%***REDACTED***%3D&ske=2026-07-10T01%3A44%3A57Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A44%3A57Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A10Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A02%3A16Z&sig=%2FoBm6dUQSHL27b3ffDo67u8NIZu2%2FiEVXmJrJs%2BSJbw%3D&ske=2026-07-10T01%3A45%3A02Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A02Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A11Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A02%3A16Z&sig=HoXbsVB9%2FXym%2BhGuNffHl%2FufECuPWzELQOJ%2FYe%2FSXl8%3D&ske=2026-07-10T01%3A46%3A13Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A46%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A11Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A02%3A45Z&sig=***REDACTED***%2FwCeGMstnk%3D&ske=2026-07-10T01%3A45%3A49Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A49Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A40Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T01%3A45%3A49Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A49Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A11Z&sig=ek2jszkKT%2FktYXwu6qklgt%2Bo5LWrpfArbcanD5QrM6o%3D&ske=2026-07-10T01%3A45%3A05Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A05Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A11Z&sig=ooYXiR%2F2nz%***REDACTED***%3D&ske=2026-07-10T01%3A45%3A09Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A12Z&sig=***REDACTED***%2FOmZxJyPe8%3D&ske=2026-07-10T01%3A44%3A58Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A44%3A58Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A07Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A12Z&sig=jWGTXCGzFijHA8jf1Io%2B7mrQv80CIWVTX%2FDHZtYJli4%3D&ske=2026-07-10T01%3A45%3A12Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A12Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A07Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A12Z&sig=wd7%2FeqBIBt4OQQY39dc7oEco0LH%2FZYCKs5nGUfL%2Fuf8%3D&ske=2026-07-10T01%3A46%3A02Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A46%3A02Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A07Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A13Z&sig=32QGepctJilwEfNd3%2F69NUHoZw3dwomSWWQ8Enb0pH4%3D&ske=2026-07-10T01%3A44%3A56Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A44%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A08Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A13Z&sig=***REDACTED***%3D&ske=2026-07-10T01%3A45%3A35Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A35Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A08Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A16Z&sig=e%2ByvSo6wWj7A9o2FzP92%2F4m1DTANGbmI7ZvSKhnC0g4%3D&ske=2026-07-10T01%3A46%3A21Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A46%3A21Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A11Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A20Z&sig=e6p258eIRXVXP8dP%2BEOG6j1%2BA3Ja0X%2BJB65%2FydizG1Y%3D&ske=2026-07-10T01%3A45%3A13Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A15Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A50Z&sig=***REDACTED***%2FY%3D&ske=2026-07-10T01%3A45%3A14Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A45Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A52Z&sig=***REDACTED***%3D&ske=2026-07-10T00%3A39%3A45Z&skoid={guid}&sks=b&skt=2026-07-09T20%3A39%3A45Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A47Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A52Z&sig=***REDACTED***%3D&ske=2026-07-10T01%3A45%3A30Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A30Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A47Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A57Z&sig=%2F%***REDACTED***%3D&ske=2026-07-10T01%3A45%3A56Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A56Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A52Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A57Z&sig=YGnyc8%***REDACTED***%3D&ske=2026-07-10T01%3A45%3A17Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A17Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A52Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A03%3A57Z&sig=pktdks2GqmWoSM58a%2BbXLBCrIxCCrAqQqOsPWa4g0zI%3D&ske=2026-07-10T01%3A45%3A19Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A45%3A19Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A03%3A52Z&sv=2025-11-05`

### aksh only

- `GET /_apis/connectionData?connectOptions={n}`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=direct-aksh-63-mega-runner-stress-1783634656&includeCapabilities=False`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `POST /_apis/oauth2/token`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-09T23%3A05%3A21Z&sig=***REDACTED***%3D&ske=2026-07-09T23%3A30%3A06Z&skoid={guid}&sks=b&skt=2026-07-09T19%3A30%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A05%3A16Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-__post_{guid}.txt?se=2026-07-09T23%3A05%3A20Z&sig=bVDMLJ7vP2uyT8L%2FwJKylv9WkSCj8RCKjO%2Fqz6dRgDA%3D&ske=2026-07-10T01%3A25%3A59Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A25%3A59Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A05%3A15Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A04%3A52Z&sig=3Yk6eGqytrnuS%2Bm6fQl37dTIaOfkxUDV%2Bfj4WS6d1uk%3D&ske=2026-07-10T01%3A25%3A27Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A25%3A27Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A04%3A47Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A05%3A20Z&sig=***REDACTED***%3D&ske=2026-07-10T01%3A38%3A00Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A38%3A00Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A05%3A15Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-09T23%3A05%3A20Z&sig=S9kjrMuTL%2BFhAzJ%2BhO%2Bp07yidkQiGKWySIAKMYSQC8Y%3D&ske=2026-07-10T01%3A24%3A39Z&skoid={guid}&sks=b&skt=2026-07-09T21%3A24%3A39Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A05%3A15Z&sv=2025-11-05`

## Per-endpoint comparison

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{accept-encoding, x-tfs-fedauthredirect}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -10,7 +10,7 @@
       "isInternal": true,
       "name": "Default",
       "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 7,
+      "size": 8,
       "targetSize": null
     },
     {
```

**Response body schema diff:**

_identical_

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 23.9 / aksh 24.6 | p95: official 23.9 / aksh 24.6

### `GET /_ws/ingest.sock`

**Header key differences:**

- aksh only: `{accept}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [101, 101, 101, 101, 101] | aksh: [101]

**Timing (ms):** p50: official 28.5 / aksh 66.9 | p95: official 33.0 / aksh 66.9

### `GET /actions/checkout/tar.gz/***REDACTED***`

**Header key differences:**

- aksh only: `{accept}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 225.6 / aksh 167.7 | p95: official 225.6 / aksh 167.7

### `GET /health`

**Header key differences:**

- aksh only: `{accept}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 46.2 / aksh 34.2 | p95: official 127.1 / aksh 64.5

### `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`

**Header key differences:**

- aksh only: `{x-github-backend, x-github-request-id}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [None, None, None, None, None] | aksh: [202, None, None, None, None, None, None, None, None, None, None, None, None]

**Timing (ms):** p50: official 0.0 / aksh 0.0 | p95: official 0.0 / aksh 50032.5

### `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "body": "{\"runner_request_id\":\"8db209c1-dcb3-5500-abfd-bda3900d1994\",\"run_service_url\":\"https://run-actions-3-azure-eastus.actions.githubusercontent.com/124/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
-  "messageId": 3238708582906253506,
+  "body": "{\"runner_request_id\":\"353aed0b-bc9f-578f-882b-0663412c1233\",\"run_service_url\":\"https://run-actions-3-azure-eastus.actions.githubusercontent.com/141/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
+  "messageId": 2755148848611453517,
   "messageType": "RunnerJobRequest"
 }
\ No newline at end of file
```

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200, 202, 202, 202, 202, None, None, None] | aksh: [200, 200, 200, 200, 202, 202, 202, 202, 202, 202, None, None, None, None, None, None]

**Timing (ms):** p50: official 976.5 / aksh 22167.2 | p95: official 50111.4 / aksh 50043.6

### `GET /ready`

**Header key differences:**

- aksh only: `{accept}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [204, 204, 204, 204] | aksh: [204, 204, 204]

**Timing (ms):** p50: official 41.4 / aksh 26.9 | p95: official 114.7 / aksh 63.8

### `POST /_apis/distributedtask/pools/{n}/agents`

**Header key differences:**

- official only: `{accept-encoding, x-tfs-fedauthredirect}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,12 +2,12 @@
   "authorization": {
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "xjz+bDznhan4+***REDACTED***/30xTpBcyaLDDNufX/***REDACTED***/nct39HsSexbTh2ss4JnfhZB3Gt+AwLW55m0/g+AZJyRjHg+***REDACTED***+***REDACTED***+LnZryJtG2755bmhbkw=="
+      "modulus": "***REDACTED***/j//T+***REDACTED***/***REDACTED***/qCddHu6xzn6mZ4Zf9IIjG15wP9iji+I+92BmKh+/OvomK8YGh107AqRahU8PeJf+gWDEH4seqgQI/WMo8xnjFxY79ccTzwYflgV/8PbfSz4Ftm2qpp6/ef45cqWyJakWw5LtZ7Iw/5H8W5cJFiVYu2T3MP5I/iEQ=="
     }
   },
   "createdOn": "0001-01-01T00:00:00",
-  "disableUpdate": false,
-  "ephemeral": false,
+  "disableUpdate": true,
+  "ephemeral": true,
   "id": 0,
   "labels": [
     {
@@ -47,7 +47,7 @@
     }
   ],
   "maxParallelism": 1,
-  "name": "direct-official-63-mega-runner-stress-1783634529",
+  "name": "direct-aksh-63-mega-runner-stress-1783634656",
   "osDescription": "Ubuntu 24.04.4 LTS",
   "provisioningState": "Provisioned",
   "status": 0,
```

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,18 +1,18 @@
 {
   "authorization": {
-    "authorizationUrl": "https://tokenghub.actions.githubusercontent.com/_apis/oauth2/token/5e4d430c-d710-4b62-aed8-555ffd0f7592",
-    "clientId": "5f316ddc-80ec-4c32-b720-6215be964125",
+    "authorizationUrl": "https://pipelinesghubeus24.actions.githubusercontent.com/***REDACTED***/_apis/oauth2/token",
+    "clientId": "adf7d713-10ab-4e07-993a-555a1be33bb1",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "xjz+bDznhan4+***REDACTED***/30xTpBcyaLDDNufX/***REDACTED***/nct39HsSexbTh2ss4JnfhZB3Gt+AwLW55m0/g+AZJyRjHg+***REDACTED***+***REDACTED***+LnZryJtG2755bmhbkw=="
+      "modulus": "***REDACTED***/j//T+***REDACTED***/***REDACTED***/qCddHu6xzn6mZ4Zf9IIjG15wP9iji+I+92BmKh+/OvomK8YGh107AqRahU8PeJf+gWDEH4seqgQI/WMo8xnjFxY79ccTzwYflgV/8PbfSz4Ftm2qpp6/ef45cqWyJakWw5LtZ7Iw/5H8W5cJFiVYu2T3MP5I/iEQ=="
     }
   },
-  "createdOn": "2026-07-09T22:02:11.103Z",
+  "createdOn": "2026-07-09T22:04:17.757Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
-  "ephemeral": false,
-  "id": 785,
+  "ephemeral": true,
+  "id": 786,
   "isElastic": false,
   "isVirtual": false,
   "labels": [
@@ -43,7 +43,7 @@
     }
   ],
   "maxParallelism": 1,
-  "name": "direct-official-63-mega-runner-stress-1783634529",
+  "name": "direct-aksh-63-mega-runner-stress-1783634656",
   "osDescription": "Ubuntu 24.04.4 LTS",
   "owningTenant": null,
   "properties": {
@@ -65,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-785",
+  "queueName": "taskagent-786",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Response body schema diff:**

_identical_

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 179.1 / aksh 67.2 | p95: official 179.1 / aksh 67.2

### `POST /_apis/oauth2/token/{guid}`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

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
\ No newline at end of file
```

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 87.0 / aksh 81.2 | p95: official 183.0 / aksh 90.4

### `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "runnerRequestId": "8db209c1-dcb3-5500-abfd-bda3900d1994"
+  "runnerRequestId": "353aed0b-bc9f-578f-882b-0663412c1233"
 }
\ No newline at end of file
```

**Request body schema diff:**

_identical_

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 50.2 / aksh 61.2 | p95: official 59.6 / aksh 151.2

### `POST /actions/build/{guid}/jobs/{guid}/runnerresolve/actions`

**Request body diff:**

_identical_

**Request body schema diff:**

_identical_

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 258.9 / aksh 275.9 | p95: official 258.9 / aksh 275.9

### `POST /actions/runner-registration`

**Header key differences:**

- aksh only: `{accept}`

**Request body diff:**

_identical_

**Request body schema diff:**

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
\ No newline at end of file
```

**Response body schema diff:**

_identical_

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 188.8 / aksh 221.0 | p95: official 188.8 / aksh 221.0

### `POST /session`

**Header key differences:**

- official only: `{x-actions-session}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,14 +1,14 @@
 {
   "agent": {
     "ephemeral": null,
-    "id": 785,
-    "name": "direct-official-63-mega-runner-stress-1783634529",
+    "id": 786,
+    "name": "direct-aksh-63-mega-runner-stress-1783634656",
     "osDescription": "Ubuntu 24.04.4 LTS",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "container (PID: 7054)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 7720)",
+  "sessionId": "24a3dc45-4a44-4d37-a4d7-e32e473788fb",
   "useFipsEncryption": false
 }
\ No newline at end of file
```

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,6 +1,6 @@
 {
   "assignmentQueued": false,
   "orchestrationId": "",
-  "ownerName": "container (PID: 7054)",
-  "sessionId": "9bbbab63-bc22-4e1c-94c8-d418b0db05a8"
+  "ownerName": "container (PID: 7720)",
+  "sessionId": "73bcf103-17e1-4155-9ef5-54861fe65264"
 }
\ No newline at end of file
```

**Response body schema diff:**

_identical_

**Status codes:** official: [201] | aksh: [201]

**Timing (ms):** p50: official 45.5 / aksh 33.3 | p95: official 45.5 / aksh 33.3

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,51 +2,15 @@
   "change_order": 1,
   "steps": [
     {
-      "completed_at": "2026-07-09T22:02:14.412Z",
-      "conclusion": 2,
-      "external_id": "18d3ec29-4204-43b9-8d87-d6923241cfa1",
+      "completed_at": null,
+      "conclusion": 0,
+      "external_id": "1b992762-6f17-41f7-8645-f014a81641b4",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-07-09T22:02:14.373Z",
-      "status": 6
-    },
-    {
-      "completed_at": "2026-07-09T22:02:14.461Z",
-      "conclusion": 3,
-      "external_id": "79c27572-2b9c-4183-a98a-8b730f6e2d57",
-      "name": "Check upstream job results",
-      "number": 2,
-      "started_at": "2026-07-09T22:02:14.419Z",
-      "status": 6
-    },
-    {
-      "completed_at": "2026-07-09T22:02:14.463Z",
-      "conclusion": 7,
-      "external_id": "6df766bd-5993-4d83-b265-5d245a78547c",
-      "name": "Check propagated outputs",
-      "number": 3,
-      "started_at": "2026-07-09T22:02:14.463Z",
-      "status": 6
-    },
-    {
-      "completed_at": "2026-07-09T22:02:14.465Z",
-      "conclusion": 7,
-      "external_id": "c2c0453a-ba8c-49cf-a3d5-8ffe0f1a3a02",
-      "name": "Write final summary",
-      "number": 4,
-      "started_at": "2026-07-09T22:02:14.464Z",
-      "status": 6
-    },
-    {
-      "completed_at": "2026-07-09T22:02:14.575Z",
-      "conclusion": 2,
-      "external_id": "8cf5cc83-c0e1-4414-ab6e-341f77503502",
-      "name": "Complete job",
-      "number": 5,
-      "started_at": "2026-07-09T22:02:14.469Z",
-      "status": 6
+      "started_at": "2026-07-09T22:04:51.633Z",
+      "status": 3
     }
   ],
-  "workflow_job_run_backend_id": "8db209c1-dcb3-5500-abfd-bda3900d1994",
-  "workflow_run_backend_id": "07871186-9050-443e-b45a-596d83bfb7d1"
+  "workflow_job_run_backend_id": "353aed0b-bc9f-578f-882b-0663412c1233",
+  "workflow_run_backend_id": "ed611300-3f3f-4705-b09f-e51c748a9d25"
 }
\ No newline at end of file
```

**Request body schema diff:**

```diff
--- official
+++ aksh
@@ -2,7 +2,7 @@
   "change_order": "number",
   "steps": [
     {
-      "completed_at": "string",
+      "completed_at": "null",
       "conclusion": "number",
       "external_id": "string",
       "name": "string",
```

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 52.8 / aksh 41.6 | p95: official 101.8 / aksh 44.6

### `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`

**Header key differences:**

- official only: `{x-actions-session}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,6 +1,6 @@
 {
-  "line_count": 49,
-  "uploaded_at": "2026-07-09T22:02:16.548Z",
-  "workflow_job_run_backend_id": "8db209c1-dcb3-5500-abfd-bda3900d1994",
-  "workflow_run_backend_id": "07871186-9050-443e-b45a-596d83bfb7d1"
+  "line_count": 157,
+  "uploaded_at": "2026-07-09T22:05:21.300Z",
+  "workflow_job_run_backend_id": "95c2d27b-5951-5235-bcd4-31dd5c5fc982",
+  "workflow_run_backend_id": "ed611300-3f3f-4705-b09f-e51c748a9d25"
 }
\ No newline at end of file
```

**Request body schema diff:**

_identical_

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200]

**Timing (ms):** p50: official 58.7 / aksh 41.0 | p95: official 88.3 / aksh 41.0

### `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`

**Header key differences:**

- official only: `{x-actions-session}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,7 +1,7 @@
 {
-  "line_count": 15,
-  "step_backend_id": "18d3ec29-4204-43b9-8d87-d6923241cfa1",
-  "uploaded_at": "2026-07-09T22:02:15.710Z",
-  "workflow_job_run_backend_id": "8db209c1-dcb3-5500-abfd-bda3900d1994",
-  "workflow_run_backend_id": "07871186-9050-443e-b45a-596d83bfb7d1"
+  "line_count": 8,
+  "step_backend_id": "f9f335fe-4d3e-4c79-918c-93a6e3563a65",
+  "uploaded_at": "2026-07-09T22:04:52.332Z",
+  "workflow_job_run_backend_id": "95c2d27b-5951-5235-bcd4-31dd5c5fc982",
+  "workflow_run_backend_id": "ed611300-3f3f-4705-b09f-e51c748a9d25"
 }
\ No newline at end of file
```

**Request body schema diff:**

_identical_

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 80.4 / aksh 174.6 | p95: official 385.1 / aksh 257.8

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "8db209c1-dcb3-5500-abfd-bda3900d1994",
-  "workflow_run_backend_id": "07871186-9050-443e-b45a-596d83bfb7d1"
+  "workflow_job_run_backend_id": "95c2d27b-5951-5235-bcd4-31dd5c5fc982",
+  "workflow_run_backend_id": "ed611300-3f3f-4705-b09f-e51c748a9d25"
 }
\ No newline at end of file
```

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa19.blob.core.windows.net/actions-results/07871186-9050-443e-b45a-596d83bfb7d1/workflow-job-run-8db209c1-dcb3-5500-abfd-bda3900d1994/logs/job/job-logs.txt?se=2026-07-09T23%3A02%3A16Z&sig=MGF%***REDACTED***%3D&ske=2026-07-10T01%3A45%3A50Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-09T21%3A45%3A50Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A11Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa13.blob.core.windows.net/actions-results/ed611300-3f3f-4705-b09f-e51c748a9d25/workflow-job-run-95c2d27b-5951-5235-bcd4-31dd5c5fc982/logs/job/job-logs.txt?se=2026-07-09T23%3A05%3A21Z&sig=***REDACTED***%3D&ske=2026-07-09T23%3A30%3A06Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-09T19%3A30%3A06Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A05%3A16Z&sv=2025-11-05"
 }
\ No newline at end of file
```

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 36.3 / aksh 34.4 | p95: official 40.9 / aksh 38.8

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "18d3ec29-4204-43b9-8d87-d6923241cfa1",
-  "workflow_job_run_backend_id": "8db209c1-dcb3-5500-abfd-bda3900d1994",
-  "workflow_run_backend_id": "07871186-9050-443e-b45a-596d83bfb7d1"
+  "step_backend_id": "f9f335fe-4d3e-4c79-918c-93a6e3563a65",
+  "workflow_job_run_backend_id": "95c2d27b-5951-5235-bcd4-31dd5c5fc982",
+  "workflow_run_backend_id": "ed611300-3f3f-4705-b09f-e51c748a9d25"
 }
\ No newline at end of file
```

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa19.blob.core.windows.net/actions-results/07871186-9050-443e-b45a-596d83bfb7d1/workflow-job-run-8db209c1-dcb3-5500-abfd-bda3900d1994/logs/steps/step-logs-18d3ec29-4204-43b9-8d87-d6923241cfa1.txt?se=2026-07-09T23%3A02%3A15Z&sig=7IDKkSUIC6U8tp%***REDACTED***%3D&ske=2026-07-10T01%3A44%3A57Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-09T21%3A44%3A57Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A02%3A10Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa13.blob.core.windows.net/actions-results/ed611300-3f3f-4705-b09f-e51c748a9d25/workflow-job-run-95c2d27b-5951-5235-bcd4-31dd5c5fc982/logs/steps/step-logs-f9f335fe-4d3e-4c79-918c-93a6e3563a65.txt?se=2026-07-09T23%3A04%3A52Z&sig=3Yk6eGqytrnuS%2Bm6fQl37dTIaOfkxUDV%2Bfj4WS6d1uk%3D&ske=2026-07-10T01%3A25%3A27Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-09T21%3A25%3A27Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-09T22%3A04%3A47Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
\ No newline at end of file
```

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 40.3 / aksh 40.2 | p95: official 371.8 / aksh 142.6

### `POST /{n}/acquirejob`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "billingOwnerId": "O_kgDOEbddog",
-  "jobMessageId": "8db209c1-dcb3-5500-abfd-bda3900d1994",
+  "jobMessageId": "353aed0b-bc9f-578f-882b-0663412c1233",
   "runnerOS": "Linux"
 }
\ No newline at end of file
```

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -29,11 +29,11 @@
         },
         {
           "k": "run_id",
-          "v": "28998886858"
+          "v": "29053445817"
         },
         {
           "k": "run_number",
-          "v": "26"
+          "v": "28"
         },
         {
           "k": "retention_days",
@@ -712,7 +712,7 @@
       "d": [
         {
           "k": "check_run_id",
-          "v": 86179563353
+          "v": 86239390358
         },
         {
           "k": "workflow_ref",
@@ -735,141 +735,7 @@
     },
     "matrix": null,
     "needs": {
-      "d": [
-        {
-          "k": "plan",
-          "v": {
-            "d": [
-              {
-                "k": "result",
-                "v": "cancelled"
-              },
-              {
-                "k": "outputs",
-                "v": {
-                  "d": [],
-                  "t": 2
-                }
-              }
-            ],
-            "t": 2
-          }
-        },
-        {
-          "k": "matrix-build",
-          "v": {
-            "d": [
-              {
-                "k": "result",
-                "v": "cancelled"
-              },
-              {
-                "k": "outputs",
-                "v": {
-                  "d": [],
-                  "t": 2
-                }
-              }
-            ],
-            "t": 2
-          }
-        },
-        {
-          "k": "cache-restore-gate",
-          "v": {
-            "d": [
-              {
-                "k": "result",
-                "v": "cancelled"
-              },
-              {
-                "k": "outputs",
-                "v": {
-                  "d": [],
-                  "t": 2
-                }
-              }
-            ],
-            "t": 2
-          }
-        },
-        {
-          "k": "artifact-gate",
-          "v": {
-            "d": [
-              {
-                "k": "result",
-                "v": "cancelled"
-              },
-              {
-                "k": "outputs",
-                "v": {
-                  "d": [],
-                  "t": 2
-                }
-              }
-            ],
-            "t": 2
-          }
-        },
-        {
-          "k": "docker-action",
-          "v": {
-            "d": [
-              {
-                "k": "result",
-                "v": "cancelled"
-              },
-              {
-                "k": "outputs",
-                "v": {
-                  "d": [],
-                  "t": 2
-                }
-              }
-            ],
-            "t": 2
-          }
-        },
-        {
-          "k": "container-job",
-          "v": {
-            "d": [
-              {
-                "k": "result",
-                "v": "failure"
-              },
-              {
-                "k": "outputs",
-                "v": {
-                  "d": [],
-                  "t": 2
-                }
-              }
-            ],
-            "t": 2
-          }
-        },
-        {
-          "k": "command-and-error-handling",
-          "v": {
-            "d": [
-              {
-                "k": "result",
-                "v": "cancelled"
-              },
-              {
-                "k": "outputs",
-                "v": {
-                  "d": [],
-                  "t": 2
-                }
-              }
-            ],
-            "t": 2
-          }
-        }
-      ],
+      "d": [],
       "t": 2
     },
     "strategy": {
@@ -945,10 +811,33 @@
     ".github/workflows/63-mega-runner-stress.yml"
   ],
   "jobContainer": null,
-  "jobDisplayName": "final-gate",
-  "jobId": "8db209c1-dcb3-5500-abfd-bda3900d1994",
+  "jobDisplayName": "command-and-error-handling",
+  "jobId": "353aed0b-bc9f-578f-882b-0663412c1233",
   "jobName": "__default",
-  "jobOutputs": null,
+  "jobOutputs": {
+    "col": 7,
+    "file": 1,
+    "line": 391,
+    "map": [
+      {
+        "Key": {
+          "col": 7,
+          "file": 1,
+          "line": 391,
+          "lit": "recovered-status",
+          "type": 0
+        },
+        "Value": {
+          "col": 25,
+          "expr": "steps.recover.outputs.recovered-status",
+          "file": 1,
+          "line": 391,
+          "type": 3
+        }
+      }
+    ],
+    "type": 2
+  },
   "jobServiceContainers": null,
   "lockedUntil": "0001-01-01T00:00:00",
   "mask": [
@@ -1026,30 +915,30 @@
     },
     {
       "type": "regex",
-      "value": "***REDACTED***\\.***REDACTED***"
+      "value": "***REDACTED***\\.***REDACTED***"
     },
     {
       "type": "regex",
-      "value": "***REDACTED***"
+      "value": "***REDACTED***"
     },
     {
       "type": "regex",
-      "value": "***REDACTED***"
+      "value": "***REDACTED***"
     },
     {
       "type": "regex",
-      "value": "***REDACTED***\\._Gz8dz5eZXf0S1p"
+      "value": "***REDACTED***\\.Uq0Vc4tyKxfMKWD"
     },
     {
       "type": "regex",
-      "value": "***REDACTED***-***REDACTED***"
+      "value": "***REDACTED***-NBjq-1riqHXrQNh8v8XFdS48tNQIPsA"
     }
   ],
   "messageType": "RunnerJobRequest",
   "plan": {
     "artifactLocation": "",
     "artifactUri": "",
-    "planId": "07871186-9050-443e-b45a-596d83bfb7d1",
+    "planId": "ed611300-3f3f-4705-b09f-e51c748a9d25",
     "planType": "actions",
     "version": 0
   },
@@ -1059,7 +948,7 @@
       {
         "authorization": {
           "parameters": {
-            "AccessToken": "***REDACTED***"
+            "AccessToken": "***REDACTED***"
           },
           "scheme": "OAuth"
         },
@@ -1076,7 +965,7 @@
         "isReady": true,
         "isShared": false,
         "name": "SystemVssConnection",
-        "url": "https://run-actions-3-azure-eastus.actions.githubusercontent.com/124/"
+        "url": "https://run-actions-3-azure-eastus.actions.githubusercontent.com/141/"
       }
     ]
   },
@@ -1084,16 +973,38 @@
   "steps": [
     {
       "condition": "success()",
+      "contextName": "__actions_checkout",
+      "continueOnError": null,
+      "displayNameToken": {
+        "col": 15,
+        "file": 1,
+        "line": 393,
+        "lit": "Checkout",
+        "type": 0
+      },
+      "id": "8b33ce03-5f9b-4d08-82a6-a0bcb79f1362",
+      "name": "__actions_checkout",
+      "reference": {
+        "name": "actions/checkout",
+        "ref": "v4",
+        "repositoryType": "GitHub",
+        "type": "repository"
+      },
+      "timeoutInMinutes": null,
+      "type": "action"
+    },
+    {
+      "condition": "success()",
       "contextName": "__run",
       "continueOnError": null,
       "displayNameToken": {
         "col": 15,
         "file": 1,
-        "line": 482,
-        "lit": "Check upstream job results",
+        "line": 396,
+        "lit": "Add problem matcher",
         "type": 0
       },
-      "id": "79c27572-2b9c-4183-a98a-8b730f6e2d57",
+      "id": "fc1aaf4d-11af-4622-adc1-4699590d0c32",
       "inputs": {
         "map": [
           {
@@ -1103,10 +1014,10 @@
             },
             "Value": {
               "col": 14,
-              "expr": "format('set -euo pipefail\n\necho \"plan={0}\"\necho \"matrix-build={1}\"\necho \"cache-restore-gate={2}\"\necho \"artifact-gate={3}\"\necho \"docker-action={4}\"\necho \"container-job={5}\"\necho \"command-and-error-handling={6}\"\n\ntest \"{7}\" = \"success\"\ntest \"{8}\" = \"success\"\ntest \"{9}\" = \"success\"\ntest \"{10}\" = \"success\"\ntest \"{11}\" = \"success\"\ntest \"{12}\" = \"success\"\ntest \"{13}\" = \"success\"\n\necho \"PASS: all upstream jobs succeeded\"\n', needs.plan.result, needs.matrix-build.result, needs.cache-restore-gate.result, needs.artifact-gate.result, needs.docker-action.result, needs.container-job.result, needs.command-and-error-handling.result, needs.plan.result, needs.matrix-build.result, needs.cache-restore-gate.result, needs.artifact-gate.result, needs.docker-action.result, needs.container-job.result, needs.command-and-error-handling.result)",
               "file": 1,
-              "line": 484,
-              "type": 3
+              "line": 398,
+              "lit": "set -euo pipefail\n\necho \"::add-matcher::$GITHUB_WORKSPACE/.github/problem-matchers/mega-matcher.json\"\necho \"PASS: problem matcher added\"\n",
+              "type": 0
             }
           },
           {
@@ -1117,7 +1028,7 @@
             "Value": {
               "col": 16,
               "file": 1,
-              "line": 483,
+              "line": 397,
               "lit": "bash",
               "type": 0
             }
@@ -1135,15 +1046,21 @@
     {
       "condition": "success()",
       "contextName": "__run_2",
-      "continueOnError": null,
+      "continueOnError": {
+        "bool": true,
+        "col": 28,
+        "file": 1,
+        "line": 406,
+        "type": 5
+      },
       "displayNameToken": {
         "col": 15,
         "file": 1,
-        "line": 505,
-        "lit": "Check propagated outputs",
+        "line": 404,
+        "lit": "Emit matched error and warning",
         "type": 0
       },
-      "id": "6df766bd-5993-4d83-b265-5d245a78547c",
+      "id": "bdfe9d94-33c8-4c0d-acd5-373378c46a54",
       "inputs": {
         "map": [
           {
@@ -1153,10 +1070,10 @@
             },
             "Value": {
               "col": 14,
-              "expr": "format('set -euo pipefail\n\ntest \"{0}\" = \"ok\"\ntest -n \"{1}\"\n\necho \"PASS: needs outputs propagated\"\necho \"PASS: plan token propagated\"\n', needs.command-and-error-handling.outputs.recovered-status, needs.plan.outputs.plan-token)",
               "file": 1,
-              "line": 507,
-              "type": 3
+              "line": 407,
+              "lit": "set -euo pipefail\n\necho \"MEGA_ERROR sample.rs:12:34: synthetic matcher error\"\necho \"MEGA_WARN sample.rs:56:7: synthetic matcher warning\"\n\necho \"::error file=manual.rs,line=3,col=9::manual annotation error\"\necho \"::warning file=manual.rs,line=4,col=2::manual annotation warning\"\n\nexit 13\n",
+              "type": 0
             }
           },
           {
@@ -1167,7 +1084,7 @@
             "Value": {
               "col": 16,
               "file": 1,
-              "line": 506,
+              "line": 405,
               "lit": "bash",
               "type": 0
             }
@@ -1183,17 +1100,17 @@
       "type": "action"
     },
     {
-      "condition": "success()",
+      "condition": "failure()",
       "contextName": "__run_3",
       "continueOnError": null,
       "displayNameToken": {
         "col": 15,
         "file": 1,
-        "line": 516,
-        "lit": "Write final summary",
+        "line": 418,
+        "lit": "failure branch after continue-on-error",
         "type": 0
       },
-      "id": "c2c0453a-ba8c-49cf-a3d5-8ffe0f1a3a02",
+      "id": "2442e7d6-1163-4ca1-a5ac-a9c761a96449",
       "inputs": {
         "map": [
           {
@@ -1204,8 +1121,8 @@
             "Value": {
               "col": 14,
               "file": 1,
-              "line": 518,
-              "lit": "{\n  echo \"# Mega runner stress test\"\n  echo \"\"\n  echo \"PASS: matrix\"\n  echo \"PASS: cache v2\"\n  echo \"PASS: artifact v2\"\n  echo \"PASS: composite action\"\n  echo \"PASS: node action lifecycle\"\n  echo \"PASS: docker action\"\n  echo \"PASS: docker build/run\"\n  echo \"PASS: container job\"\n  echo \"PASS: service container\"\n  echo \"PASS: problem matcher\"\n  echo \"PASS: annotations\"\n  echo \"PASS: masking\"\n  echo \"PASS: needs/outputs\"\n  echo \"\"\n  echo \"Final result: PASS\"\n} >> \"$GITHUB_STEP_SUMMARY\"\n\necho \"PASS: MEGA WORKFLOW COMPLETE\"\n",
+              "line": 421,
+              "lit": "echo \"PASS: failure() branch observed continue-on-error failure\"\necho \"PASS: branch did not fail the job\"\n",
               "type": 0
             }
           },
@@ -1217,7 +1134,7 @@
             "Value": {
               "col": 16,
               "file": 1,
-              "line": 517,
+              "line": 420,
               "lit": "bash",
               "type": 0
             }
@@ -1231,11 +1148,261 @@
       },
       "timeoutInMinutes": null,
       "type": "action"
+    },
+    {
+      "condition": "success()",
+      "contextName": "__run_4",
+      "continueOnError": null,
+      "displayNameToken": {
+        "col": 15,
+        "file": 1,
+        "line": 425,
+        "lit": "success branch after continue-on-error",
+        "type": 0
+      },
+      "id": "329e73ce-932b-4b67-9f7c-e8bb7691fe46",
+      "inputs": {
+        "map": [
+          {
+            "Key": {
+              "lit": "script",
+              "type": 0
+            },
+            "Value": {
+              "col": 14,
+              "file": 1,
+              "line": 428,
+              "lit": "echo \"PASS: success() branch ran when no prior failure state is visible\"\n",
+              "type": 0
+            }
+          },
+          {
+            "Key": {
+              "lit": "shell",
+              "type": 0
+            },
+            "Value": {
+              "col": 16,
+              "file": 1,
+              "line": 427,
+              "lit": "bash",
+              "type": 0
+            }
+          }
+        ],
+        "type": 2
+      },
+      "name": "__run_4",
+      "reference": {
+        "type": "script"
+      },
+      "timeoutInMinutes": null,
+      "type": "action"
+    },
+    {
+      "condition": "always()",
+      "contextName": "__run_5",
+      "continueOnError": null,
+      "displayNameToken": {
+        "col": 15,
+        "file": 1,
+        "line": 431,
+        "lit": "Remove problem matcher",
+        "type": 0
+      },
+      "id": "78802709-8d12-4efb-bd2a-6455a2c0f524",
+      "inputs": {
+        "map": [
+          {
+            "Key": {
+              "lit": "script",
+              "type": 0
+            },
+            "Value": {
+              "col": 14,
+              "file": 1,
+              "line": 434,
+              "lit": "set -euo pipefail\n\necho \"::remove-matcher owner=mega-matcher::\"\necho \"PASS: problem matcher removed\"\n",
+              "type": 0
+            }
+          },
+          {
+            "Key": {
+              "lit": "shell",
+              "type": 0
+            },
+            "Value": {
+              "col": 16,
+              "file": 1,
+              "line": 433,
+              "lit": "bash",
+              "type": 0
+            }
+          }
+        ],
+        "type": 2
+      },
+      "name": "__run_5",
+      "reference": {
+        "type": "script"
+      },
+      "timeoutInMinutes": null,
+      "type": "action"
+    },
+    {
+      "condition": "always()",
+      "contextName": "__run_6",
+      "continueOnError": null,
+      "displayNameToken": {
+        "col": 15,
+        "file": 1,
+        "line": 440,
+        "lit": "Mask fake secret",
+        "type": 0
+      },
+      "id": "2543e6ba-b35b-4526-a73c-c7d536d7f984",
+      "inputs": {
+        "map": [
+          {
+            "Key": {
+              "lit": "script",
+              "type": 0
+            },
+            "Value": {
+              "col": 14,
+              "file": 1,
+              "line": 443,
+              "lit": "set -euo pipefail\n\nSECRET_VALUE=\"mega-secret-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}\"\necho \"::add-mask::$SECRET_VALUE\"\necho \"masked secret should appear redacted: $SECRET_VALUE\"\n\necho \"PASS: secret masking command emitted\"\n",
+              "type": 0
+            }
+          },
+          {
+            "Key": {
+              "lit": "shell",
+              "type": 0
+            },
+            "Value": {
+              "col": 16,
+              "file": 1,
+              "line": 442,
+              "lit": "bash",
+              "type": 0
+            }
+          }
+        ],
+        "type": 2
+      },
+      "name": "__run_6",
+      "reference": {
+        "type": "script"
+      },
+      "timeoutInMinutes": null,
+      "type": "action"
+    },
+    {
+      "condition": "always()",
+      "contextName": "recover",
+      "continueOnError": null,
+      "displayNameToken": {
+        "col": 15,
+        "file": 1,
+        "line": 452,
+        "lit": "Set recovery output",
+        "type": 0
+      },
+      "id": "f7c41341-b34c-40ad-85ab-9e18f24f8b32",
+      "inputs": {
+        "map": [
+          {
+            "Key": {
+              "lit": "script",
+              "type": 0
+            },
+            "Value": {
+              "col": 14,
+              "file": 1,
+              "line": 456,
+              "lit": "set -euo pipefail\n\necho \"recovered-status=ok\" >> \"$GITHUB_OUTPUT\"\necho \"PASS: recovery output set\"\n",
+              "type": 0
+            }
+          },
+          {
+            "Key": {
+              "lit": "shell",
+              "type": 0
+            },
+            "Value": {
+              "col": 16,
+              "file": 1,
+              "line": 455,
+              "lit": "bash",
+              "type": 0
+            }
+          }
+        ],
+        "type": 2
+      },
+      "name": "recover",
+      "reference": {
+        "type": "script"
+      },
+      "timeoutInMinutes": null,
+      "type": "action"
+    },
+    {
+      "condition": "always()",
+      "contextName": "__run_7",
+      "continueOnError": null,
+      "displayNameToken": {
+        "col": 15,
+        "file": 1,
+        "line": 462,
+        "lit": "always branch",
+        "type": 0
+      },
+      "id": "1eb0032c-c724-491f-ab6b-ee9507502b57",
+      "inputs": {
+        "map": [
+          {
+            "Key": {
+              "lit": "script",
+              "type": 0
+            },
+            "Value": {
+              "col": 14,
+              "file": 1,
+              "line": 465,
+              "lit": "echo \"PASS: always() branch ran\"\n",
+              "type": 0
+            }
+          },
+          {
+            "Key": {
+              "lit": "shell",
+              "type": 0
+            },
+            "Value": {
+              "col": 16,
+              "file": 1,
+              "line": 464,
+              "lit": "bash",
+              "type": 0
+            }
+          }
+        ],
+        "type": 2
+      },
+      "name": "__run_7",
+      "reference": {
+        "type": "script"
+      },
+      "timeoutInMinutes": null,
+      "type": "action"
     }
   ],
   "timeline": {
     "changeId": 0,
-    "id": "07871186-9050-443e-b45a-596d83bfb7d1",
+    "id": "ed611300-3f3f-4705-b09f-e51c748a9d25",
     "location": null
   },
   "variables": {
@@ -1361,13 +1528,13 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
     },
     "system.github.job": {
-      "value": "final-gate"
+      "value": "command-and-error-handling"
     },
     "system.github.launch_endpoint": {
       "value": "https://launch.actions.githubusercontent.com"
@@ -1380,16 +1547,16 @@
     },
     "system.github.token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.github.token.permissions": {
       "value": "{\"Actions\":\"read\",\"Contents\":\"read\",\"Metadata\":\"read\"}"
     },
     "system.orchestrationId": {
-      "value": "07871186-9050-443e-b45a-596d83bfb7d1.final-gate.__default"
+      "value": "ed611300-3f3f-4705-b09f-e51c748a9d25.command-and-error-handling.__default"
     },
     "system.phaseDisplayName": {
-      "value": "final-gate"
+      "value": "command-and-error-handling"
     },
     "system.runner.lowdiskspacethreshold": {
       "value": "100"
```

**Response body schema diff:**

```diff
--- official
+++ aksh
@@ -147,27 +147,7 @@
     },
     "matrix": "null",
     "needs": {
-      "d": [
-        {
-          "k": "string",
-          "v": {
-            "d": [
-              {
-                "k": "string",
-                "v": "string"
-              },
-              {
-                "k": "string",
-                "v": {
-                  "d": [],
-                  "t": "number"
-                }
-              }
-            ],
-            "t": "number"
-          }
-        }
-      ],
+      "d": [],
       "t": "number"
     },
     "strategy": {
@@ -222,7 +202,30 @@
   "jobDisplayName": "string",
   "jobId": "string",
   "jobName": "string",
-  "jobOutputs": "null",
+  "jobOutputs": {
+    "col": "number",
+    "file": "number",
+    "line": "number",
+    "map": [
+      {
+        "Key": {
+          "col": "number",
+          "file": "number",
+          "line": "number",
+          "lit": "string",
+          "type": "number"
+        },
+        "Value": {
+          "col": "number",
+          "expr": "string",
+          "file": "number",
+          "line": "number",
+          "type": "number"
+        }
+      }
+    ],
+    "type": "number"
+  },
   "jobServiceContainers": "null",
   "lockedUntil": "string",
   "mask": [
@@ -280,21 +283,30 @@
         "type": "number"
       },
       "id": "string",
+      "name": "string",
+      "reference": {
+        "name": "string",
+        "ref": "string",
+        "repositoryType": "string",
+        "type": "string"
+      },
+      "timeoutInMinutes": "null",
+      "type": "string"
+    },
+    {
+      "condition": "string",
+      "contextName": "string",
+      "continueOnError": "null",
+      "displayNameToken": {
+        "col": "number",
+        "file": "number",
+        "line": "number",
+        "lit": "string",
+        "type": "number"
+      },
+      "id": "string",
       "inputs": {
         "map": [
-          {
-            "Key": {
-              "lit": "string",
-              "type": "number"
-            },
-            "Value": {
-              "col": "number",
-              "expr": "string",
-              "file": "number",
-              "line": "number",
-              "type": "number"
-            }
-          },
           {
             "Key": {
               "lit": "string",
@@ -321,7 +333,13 @@
     {
       "condition": "string",
       "contextName": "string",
-      "continueOnError": "null",
+      "continueOnError": {
+        "bool": "boolean",
+        "col": "number",
+        "file": "number",
+        "line": "number",
+        "type": "number"
+      },
       "displayNameToken": {
         "col": "number",
         "file": "number",
```

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200]

**Timing (ms):** p50: official 480.0 / aksh 437.0 | p95: official 511.6 / aksh 518.6

### `POST /{n}/completejob`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,87 +2,105 @@
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
   "conclusion": "failed",
-  "jobId": "8db209c1-dcb3-5500-abfd-bda3900d1994",
-  "outputs": {},
-  "planId": "07871186-9050-443e-b45a-596d83bfb7d1",
+  "jobId": "95c2d27b-5951-5235-bcd4-31dd5c5fc982",
+  "outputs": {
+    "cache-prefix": {
+      "value": ""
+    },
+    "matrix-json": {
+      "value": ""
+    },
+    "plan-token": {
+      "value": ""
+    }
+  },
+  "planId": "ed611300-3f3f-4705-b09f-e51c748a9d25",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-07-09T22:02:14.4129596Z",
+      "completed_at": "2026-07-09T22:05:21.353Z",
       "conclusion": "succeeded",
-      "external_id": "18d3ec29-4204-43b9-8d87-d6923241cfa1",
+      "external_id": "f9f335fe-4d3e-4c79-918c-93a6e3563a65",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-07-09T22:02:14.3736355Z",
+      "started_at": "2026-07-09T22:05:21.353Z",
       "status": "completed",
       "type": "runner"
     },
     {
-      "action_name": "bash",
+      "action_name": "actions/checkout@v4",
       "annotations": [
         {
-          "endLine": 33,
+          "endLine": 1,
           "level": "failure",
-          "message": "Process completed with exit code 1.",
-          "startLine": 33,
+          "message": "node action exited with code 1",
+          "startLine": 1,
           "stepNumber": 2
         }
       ],
-      "completed_at": "2026-07-09T22:02:14.4613452Z",
+      "completed_at": "2026-07-09T22:05:21.353Z",
       "conclusion": "failed",
-      "external_id": "79c27572-2b9c-4183-a98a-8b730f6e2d57",
-      "name": "Check upstream job results",
+      "external_id": "cc17533b-4f70-4e53-be75-d7f60802640d",
+      "name": "Checkout",
       "number": 2,
-      "started_at": "2026-07-09T22:02:14.4190989Z",
+      "started_at": "2026-07-09T22:05:21.353Z",
       "status": "completed",
-      "type": "run"
+      "type": "action"
     },
     {
+      "action_name": "bash",
       "annotations": [],
-      "completed_at": "2026-07-09T22:02:14.4638401Z",
+      "completed_at": "2026-07-09T22:05:21.353Z",
       "conclusion": "skipped",
-      "external_id": "6df766bd-5993-4d83-b265-5d245a78547c",
-      "name": "Check propagated outputs",
+      "external_id": "fccef942-bff0-4985-ba57-8b706172f7dd",
+      "name": "Make dynamic matrix",
       "number": 3,
-      "started_at": "2026-07-09T22:02:14.4631173Z",
-      "status": "completed"
+      "started_at": "2026-07-09T22:05:21.353Z",
+      "status": "completed",
+      "type": "run"
     },
     {
+      "action_name": "bash",
       "annotations": [],
-      "completed_at": "2026-07-09T22:02:14.4650455Z",
+      "completed_at": "2026-07-09T22:05:21.353Z",
       "conclusion": "skipped",
-      "external_id": "c2c0453a-ba8c-49cf-a3d5-8ffe0f1a3a02",
-      "name": "Write final summary",
+      "external_id": "c14f1709-1dd1-4a1d-ae94-b0387dfa2ddb",
+      "name": "Verify expression helpers",
       "number": 4,
-      "started_at": "2026-07-09T22:02:14.4643709Z",
-      "status": "completed"
+      "started_at": "2026-07-09T22:05:21.353Z",
+      "status": "completed",
+      "type": "run"
+    },
+    {
+      "action_name": "actions/checkout@v4",
+      "annotations": [],
+      "completed_at": "2026-07-09T22:05:21.353Z",
+      "conclusion": "succeeded",
+      "external_id": "__post_cc17533b-4f70-4e53-be75-d7f60802640d",
+      "name": "Post Checkout",
+      "number": 5,
+      "started_at": "2026-07-09T22:05:21.353Z",
+      "status": "completed",
+      "type": "action"
     },
     {
       "action_name": "complete_job",
       "annotations": [],
-      "completed_at": "2026-07-09T22:02:14.5750086Z",
+      "completed_at": "2026-07-09T22:05:21.353Z",
       "conclusion": "succeeded",
-      "external_id": "8cf5cc83-c0e1-4414-ab6e-341f77503502",
+      "external_id": "1bb38497-2b37-4506-98b2-b0896582e806",
       "name": "Complete job",
-      "number": 5,
-      "started_at": "2026-07-09T22:02:14.4694959Z",
+      "number": 6,
+      "started_at": "2026-07-09T22:05:21.353Z",
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
\ No newline at end of file
```

**Request body schema diff:**

```diff
--- official
+++ aksh
@@ -3,7 +3,17 @@
   "billingOwnerId": "string",
   "conclusion": "string",
   "jobId": "string",
-  "outputs": {},
+  "outputs": {
+    "cache-prefix": {
+      "value": "string"
+    },
+    "matrix-json": {
+      "value": "string"
+    },
+    "plan-token": {
+      "value": "string"
+    }
+  },
   "planId": "string",
   "stepResults": [
     {
@@ -37,16 +47,6 @@
       "started_at": "string",
       "status": "string",
       "type": "string"
-    },
-    {
-      "annotations": [],
-      "completed_at": "string",
-      "conclusion": "string",
-      "external_id": "string",
-      "name": "string",
-      "number": "number",
-      "started_at": "string",
-      "status": "string"
     }
   ],
   "telemetry": [
```

**Response body diff:**

_identical_

**Response body schema diff:**

_identical_

**Status codes:** official: [204, 204, 204, 204, 204] | aksh: [204, 204, 204, 204]

**Timing (ms):** p50: official 61.7 / aksh 41.3 | p95: official 396.5 / aksh 117.6

### `POST /{n}/renewjob`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "8db209c1-dcb3-5500-abfd-bda3900d1994",
-  "planId": "07871186-9050-443e-b45a-596d83bfb7d1"
+  "jobId": "353aed0b-bc9f-578f-882b-0663412c1233",
+  "planId": "ed611300-3f3f-4705-b09f-e51c748a9d25"
 }
\ No newline at end of file
```

**Request body schema diff:**

_identical_

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-07-09T22:12:14.120451839Z"
+  "lockedUntil": "2026-07-09T22:14:51.388277054Z"
 }
\ No newline at end of file
```

**Response body schema diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200]

**Timing (ms):** p50: official 42.9 / aksh 45.9 | p95: official 50.0 / aksh 50.3
