# MITM comparison: 62-artifact-stress

**official**: ok — 66 flows
**aksh**: ok — 195 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/session` | 1 | 0 | 33.0 | - | 204 |  |
| GET | `/_apis/connectionData?connectOptions={n}` | 0 | 3 | - | 71.5 |  | 200, 200, 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 18 | 0 | 40.5 | - | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-62-artifact-stress-1-1783648338&includeCapabilities=False` | 0 | 1 | - | 24.5 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-62-artifact-stress-2-1783648338&includeCapabilities=False` | 0 | 1 | - | 30.3 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-62-artifact-stress-3-1783648338&includeCapabilities=False` | 0 | 1 | - | 25.5 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-1-1783648271&includeCapabilities=False` | 1 | 0 | 20.8 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-2-1783648271&includeCapabilities=False` | 1 | 0 | 22.0 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-3-1783648271&includeCapabilities=False` | 1 | 0 | 21.5 | - | 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 3 | 3 | 29.8 | 55.0 | 200, 200, 200 | 200, 200, 200 |
| GET | `/_ws/ingest.sock` | 1 | 3 | 96.4 | 116.4 | 101 | 101, 101, 101 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22checksums-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A46%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A46%3A06Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 28.0 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22nested-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=VBiiRCl2zlENE6ICnkM1i%2Bqb%2BxN9kWoIbruPb%2Fujn10%3D&ske=2026-07-10T04%3A36%3A32Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A32Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 19.1 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22nested-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A11Z&sig=%2Bj2JqZwqgLeJw8ApIlD%2FyUuctskcfOrgg0eYvApKuuo%3D&ske=2026-07-10T04%3A36%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A24Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A06Z&sv=2025-11-05` | 0 | 1 | - | 20.8 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22multi-files-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A15Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 111.7 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22multi-files-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A08Z&sig=qPEvvHY%2FNHjTXWSOP%2BnXNvV0dNOzUXvvDnRte9TF43w%3D&ske=2026-07-10T04%3A43%3A59Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A43%3A59Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05` | 0 | 1 | - | 23.1 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A36%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A30Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 22.2 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A16Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 25.6 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22binary-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=e8x4r%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A06Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 141.3 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22binary-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A10Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A05Z&sv=2025-11-05` | 0 | 1 | - | 55.1 |  | 200 |
| GET | `/actions/download-artifact/tar.gz/***REDACTED***` | 0 | 2 | - | 223.7 |  | 200, 200 |
| GET | `/actions/upload-artifact/tar.gz/***REDACTED***` | 1 | 1 | 366.6 | 179.3 | 200 | 200 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 3 | - | 4313.0 |  | 200, 200, 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 3 | - | 3531.3 |  | 200, 200, 200 |
| GET | `/health` | 2 | 6 | 114.1 | 28.4 | 200, 200 | 200, 200, 200, 200, 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 1 | 8 | 0 | 0 | None | None, None, None, None, None, None, None, None |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 3 | 6 | 9828.6 | 15912.3 | 200, None, None | 200, 200, 200, None, None, None |
| GET | `/ready` | 1 | 3 | 57.1 | 21.4 | 204 | 204, 204, 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 3 | 3 | 103.1 | 148.1 | 200, 200, 200 | 200, 200, 200 |
| POST | `/_apis/oauth2/token` | 4 | 6 | 292.4 | 102.0 | 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 1 | 3 | 41.6 | 67.9 | 200 | 200, 200, 200 |
| POST | `/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 1 | 3 | 180.8 | 147.5 | 200 | 200, 200, 200 |
| POST | `/actions/runner-registration` | 3 | 3 | 249.7 | 268.4 | 200, 200, 200 | 200, 200, 200 |
| POST | `/session` | 3 | 3 | 66.1 | 113.1 | 201, 201, 201 | 201, 201, 201 |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact` | 0 | 5 | - | 138.3 |  | 200, 200, 200, 200, 200 |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact` | 0 | 5 | - | 181.6 |  | 200, 200, 200, 200, 200 |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL` | 0 | 9 | - | 46.5 |  | 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts` | 0 | 14 | - | 110.9 |  | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 2 | 3 | 112.8 | 63.9 | 200, 200 | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 1 | 3 | 37.0 | 43.1 | 200 | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 3 | 19 | 182.8 | 69.9 | 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 1 | 3 | 33.0 | 35.5 | 200 | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 3 | 19 | 95.8 | 64.2 | 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/{n}/acquirejob` | 1 | 3 | 641.8 | 395.6 | 200 | 200, 200, 200 |
| POST | `/{n}/completejob` | 1 | 3 | 123.8 | 43.0 | 204 | 204, 204, 204 |
| POST | `/{n}/renewjob` | 1 | 3 | 34.7 | 56.0 | 200 | 200, 200, 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A03Z&sig=%***REDACTED***%3D&ske=2026-07-10T04%3A46%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A46%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A58Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 0 | 1 | - | 22.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A03Z&sig=%***REDACTED***%3D&ske=2026-07-10T04%3A46%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A46%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A58Z&sv=2025-11-05&comp=blocklist` | 0 | 1 | - | 28.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A01Z&sig=***REDACTED***%2F7uNzST8%3D&ske=2026-07-10T04%3A44%3A43Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A56Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 0 | 1 | - | 25.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A01Z&sig=***REDACTED***%2F7uNzST8%3D&ske=2026-07-10T04%3A44%3A43Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A56Z&sv=2025-11-05&comp=blocklist` | 0 | 1 | - | 40.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A59Z&sig=GxR6P%***REDACTED***%2Fyo%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A54Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 0 | 1 | - | 28.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A59Z&sig=GxR6P%***REDACTED***%2Fyo%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A54Z&sv=2025-11-05&comp=blocklist` | 0 | 1 | - | 26.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A24Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A53Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 0 | 1 | - | 32.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A24Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A53Z&sv=2025-11-05&comp=blocklist` | 0 | 1 | - | 26.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A00Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A24%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A24%3A16Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A55Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 0 | 1 | - | 66.8 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A00Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A24%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A24%3A16Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A55Z&sv=2025-11-05&comp=blocklist` | 0 | 1 | - | 25.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A51%3A59Z&sig=URJfnTSBqIFf4%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A14Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A51%3A54Z&sv=2025-11-05` | 1 | 0 | 106.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A04Z&sig=***REDACTED***%2FkJbOiH1Wg%3D&ske=2026-07-10T04%3A44%3A26Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A26Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A59Z&sv=2025-11-05` | 0 | 1 | - | 37.5 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A08Z&sig=3S2x%2FWbnmNtgpJwibySkELdnO%2B%2By4c1uJ7ACnkQD74Q%3D&ske=2026-07-10T04%3A45%3A22Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A22Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05` | 0 | 1 | - | 30.1 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A12Z&sig=iO6tfVSesZos6ERYZt2SRbQkS%2B8X2QmsQ1uVFg9CTtY%3D&ske=2026-07-10T04%3A44%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A07Z&sv=2025-11-05` | 0 | 1 | - | 29.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A51%3A52Z&sig=%2FhW7ZvE8Eoz19lmDOVlksxBJ%2FIBt4o2DRgvL7OcNT3I%3D&ske=2026-07-10T04%3A44%3A55Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A55Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A51%3A47Z&sv=2025-11-05` | 1 | 0 | 103.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A51%3A58Z&sig=tvX2tbRrcuEUsBGdZCM10jr19rdS%2BaL%2BrDa2CIj8T9E%3D&ske=2026-07-10T04%3A44%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A51%3A53Z&sv=2025-11-05` | 1 | 0 | 29.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A51%3A59Z&sig=zGjDKTQ3ODML%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A19Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A19Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A51%3A54Z&sv=2025-11-05` | 1 | 0 | 82.6 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A52%3A57Z&sig=%2FMZIbHNDfF2SWaQ%2B%2FMcCf6ZETVfR7eQQvrBWTJMjVTI%3D&ske=2026-07-10T04%3A44%3A38Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A38Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A52Z&sv=2025-11-05` | 0 | 1 | - | 29.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A52%3A57Z&sig=c%***REDACTED***%2BzzfOg%3D&ske=2026-07-10T04%3A44%3A38Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A38Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A52Z&sv=2025-11-05` | 0 | 1 | - | 72.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A52%3A58Z&sig=JPNKSA9O7Ew3znC%2FSIj1tnKiF08vL4fAGPw0thCnaDU%3D&ske=2026-07-10T04%3A44%3A11Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A11Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A53Z&sv=2025-11-05` | 0 | 1 | - | 21.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A00Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A16Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A55Z&sv=2025-11-05` | 0 | 1 | - | 25.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A01Z&sig=jqVw8iBtk5EB0g58rM2c%2FrA2%2Bp8VhXHeT2FGM9PZe34%3D&ske=2026-07-10T04%3A44%3A53Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A56Z&sv=2025-11-05` | 0 | 1 | - | 27.1 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A02Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A15Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A57Z&sv=2025-11-05` | 0 | 1 | - | 25.8 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A03Z&sig=2uZYBxRqA%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A50Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A50Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A58Z&sv=2025-11-05` | 0 | 1 | - | 22.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A03Z&sig=***REDACTED***%2BbK8%3D&ske=2026-07-10T04%3A44%3A11Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A11Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A58Z&sv=2025-11-05` | 0 | 1 | - | 76.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A06Z&sig=IDqKVWEZ%***REDACTED***%2B141HY%3D&ske=2026-07-10T04%3A45%3A05Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A05Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A01Z&sv=2025-11-05` | 0 | 1 | - | 31.6 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A06Z&sig=zz727E%2F78wkqHs%2B8QN8kqb7Rcd9mLNdM%2F%2Fz%2FVRqQarc%3D&ske=2026-07-10T04%3A37%3A01Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A37%3A01Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A01Z&sv=2025-11-05` | 0 | 1 | - | 32.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A08Z&sig=BA6IUsG%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A52Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A52Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05` | 0 | 1 | - | 68.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A08Z&sig=HGnNXWWK6%2FB2ZV522ZZ39TDPay8CRO%2BEfTKBwdTZRRY%3D&ske=2026-07-10T04%3A44%3A51Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A51Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05` | 0 | 1 | - | 35.6 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A08Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A43%3A59Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A43%3A59Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05` | 0 | 1 | - | 150.8 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A08Z&sig=dGdO0udNaCnsHq6Z29jMV0%2FJoFj4hS3k6tXe8nsrm2c%3D&ske=2026-07-10T04%3A44%3A27Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A27Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05` | 0 | 1 | - | 21.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A09Z&sig=sBwQVgYhRxmtee%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A05Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A05Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A04Z&sv=2025-11-05` | 0 | 1 | - | 23.2 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A10Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A48%3A21Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A48%3A21Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A05Z&sv=2025-11-05` | 0 | 1 | - | 26.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A11Z&sig=I9%2FyA8foN7aXhYwslFx%2BWSZcHs82NnY22eeomt%2BXt8o%3D&ske=2026-07-10T04%3A43%3A59Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A43%3A59Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A06Z&sv=2025-11-05` | 0 | 1 | - | 78.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A43%3A59Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A43%3A59Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A06Z&sv=2025-11-05` | 0 | 1 | - | 75.0 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A11Z&sig=vsaRMw4%2BkjJP%2FFVeNtOSVQyWjz1Gs7tJV6Vw4b%2FzYfA%3D&ske=2026-07-10T04%3A44%3A36Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A36Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A06Z&sv=2025-11-05` | 0 | 1 | - | 21.1 |  | 201 |

## Missing endpoints

### official only

- `DELETE /session`
- `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-1-1783648271&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-2-1783648271&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-3-1783648271&includeCapabilities=False`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A51%3A59Z&sig=URJfnTSBqIFf4%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A14Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A51%3A54Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A51%3A52Z&sig=%2FhW7ZvE8Eoz19lmDOVlksxBJ%2FIBt4o2DRgvL7OcNT3I%3D&ske=2026-07-10T04%3A44%3A55Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A55Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A51%3A47Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A51%3A58Z&sig=tvX2tbRrcuEUsBGdZCM10jr19rdS%2BaL%2BrDa2CIj8T9E%3D&ske=2026-07-10T04%3A44%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A51%3A53Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A51%3A59Z&sig=zGjDKTQ3ODML%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A19Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A19Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A51%3A54Z&sv=2025-11-05`

### aksh only

- `GET /_apis/connectionData?connectOptions={n}`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-62-artifact-stress-1-1783648338&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-62-artifact-stress-2-1783648338&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-62-artifact-stress-3-1783648338&includeCapabilities=False`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22checksums-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A46%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A46%3A06Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22nested-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=VBiiRCl2zlENE6ICnkM1i%2Bqb%2BxN9kWoIbruPb%2Fujn10%3D&ske=2026-07-10T04%3A36%3A32Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A32Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22nested-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A11Z&sig=%2Bj2JqZwqgLeJw8ApIlD%2FyUuctskcfOrgg0eYvApKuuo%3D&ske=2026-07-10T04%3A36%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A24Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A06Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22multi-files-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A15Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22multi-files-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A08Z&sig=qPEvvHY%2FNHjTXWSOP%2BnXNvV0dNOzUXvvDnRte9TF43w%3D&ske=2026-07-10T04%3A43%3A59Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A43%3A59Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A36%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A30Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A16Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22binary-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=e8x4r%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A06Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22binary-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A10Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A05Z&sv=2025-11-05`
- `GET /actions/download-artifact/tar.gz/***REDACTED***`
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
- `POST /twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact`
- `POST /twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact`
- `POST /twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL`
- `POST /twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A03Z&sig=%***REDACTED***%3D&ske=2026-07-10T04%3A46%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A46%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A58Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A03Z&sig=%***REDACTED***%3D&ske=2026-07-10T04%3A46%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A46%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A58Z&sv=2025-11-05&comp=blocklist`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A01Z&sig=***REDACTED***%2F7uNzST8%3D&ske=2026-07-10T04%3A44%3A43Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A56Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A01Z&sig=***REDACTED***%2F7uNzST8%3D&ske=2026-07-10T04%3A44%3A43Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A56Z&sv=2025-11-05&comp=blocklist`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A59Z&sig=GxR6P%***REDACTED***%2Fyo%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A54Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A59Z&sig=GxR6P%***REDACTED***%2Fyo%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A54Z&sv=2025-11-05&comp=blocklist`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A24Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A53Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A24Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A53Z&sv=2025-11-05&comp=blocklist`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A00Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A24%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A24%3A16Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A55Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A00Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A24%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A24%3A16Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A55Z&sv=2025-11-05&comp=blocklist`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A04Z&sig=***REDACTED***%2FkJbOiH1Wg%3D&ske=2026-07-10T04%3A44%3A26Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A26Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A59Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A08Z&sig=3S2x%2FWbnmNtgpJwibySkELdnO%2B%2By4c1uJ7ACnkQD74Q%3D&ske=2026-07-10T04%3A45%3A22Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A22Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A12Z&sig=iO6tfVSesZos6ERYZt2SRbQkS%2B8X2QmsQ1uVFg9CTtY%3D&ske=2026-07-10T04%3A44%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A07Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A52%3A57Z&sig=%2FMZIbHNDfF2SWaQ%2B%2FMcCf6ZETVfR7eQQvrBWTJMjVTI%3D&ske=2026-07-10T04%3A44%3A38Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A38Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A52Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A52%3A57Z&sig=c%***REDACTED***%2BzzfOg%3D&ske=2026-07-10T04%3A44%3A38Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A38Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A52Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A52%3A58Z&sig=JPNKSA9O7Ew3znC%2FSIj1tnKiF08vL4fAGPw0thCnaDU%3D&ske=2026-07-10T04%3A44%3A11Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A11Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A53Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A00Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A16Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A55Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A01Z&sig=jqVw8iBtk5EB0g58rM2c%2FrA2%2Bp8VhXHeT2FGM9PZe34%3D&ske=2026-07-10T04%3A44%3A53Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A53Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A56Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A02Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A15Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A57Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A03Z&sig=2uZYBxRqA%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A50Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A50Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A58Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A03Z&sig=***REDACTED***%2BbK8%3D&ske=2026-07-10T04%3A44%3A11Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A11Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A58Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A06Z&sig=IDqKVWEZ%***REDACTED***%2B141HY%3D&ske=2026-07-10T04%3A45%3A05Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A05Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A01Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A06Z&sig=zz727E%2F78wkqHs%2B8QN8kqb7Rcd9mLNdM%2F%2Fz%2FVRqQarc%3D&ske=2026-07-10T04%3A37%3A01Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A37%3A01Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A01Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A08Z&sig=BA6IUsG%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A52Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A52Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A08Z&sig=HGnNXWWK6%2FB2ZV522ZZ39TDPay8CRO%2BEfTKBwdTZRRY%3D&ske=2026-07-10T04%3A44%3A51Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A51Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A08Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A43%3A59Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A43%3A59Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A08Z&sig=dGdO0udNaCnsHq6Z29jMV0%2FJoFj4hS3k6tXe8nsrm2c%3D&ske=2026-07-10T04%3A44%3A27Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A27Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A09Z&sig=sBwQVgYhRxmtee%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A05Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A05Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A04Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A10Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A48%3A21Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A48%3A21Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A05Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A11Z&sig=I9%2FyA8foN7aXhYwslFx%2BWSZcHs82NnY22eeomt%2BXt8o%3D&ske=2026-07-10T04%3A43%3A59Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A43%3A59Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A11Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A43%3A59Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A43%3A59Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A06Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T02%3A53%3A11Z&sig=vsaRMw4%2BkjJP%2FFVeNtOSVQyWjz1Gs7tJV6Vw4b%2FzYfA%3D&ske=2026-07-10T04%3A44%3A36Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A36Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A06Z&sv=2025-11-05`

## Per-endpoint comparison

### `GET /_apis/distributedtask/pools?poolType=Automation`

**Header key differences:**

- official only: `{'x-tfs-fedauthredirect', 'accept-encoding'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -10,7 +10,7 @@
       "isInternal": true,
       "name": "Default",
       "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 9,
+      "size": 11,
       "targetSize": null
     },
     {
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 22.5 / aksh 57.3 | p95: official 46.0 / aksh 81.5

### `GET /_ws/ingest.sock`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [101] | aksh: [101, 101, 101]

**Timing (ms):** p50: official 96.4 / aksh 156.5 | p95: official 96.4 / aksh 169.3

### `GET /actions/upload-artifact/tar.gz/***REDACTED***`

**Header key differences:**

- official only: `{'authorization'}`
- aksh only: `{'accept'}`

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 366.6 / aksh 179.3 | p95: official 366.6 / aksh 179.3

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200] | aksh: [200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 191.8 / aksh 26.1 | p95: official 191.8 / aksh 42.2

### `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Status codes:** official: [None] | aksh: [None, None, None, None, None, None, None, None]

**Timing (ms):** p50: official 0.0 / aksh 0.0 | p95: official 0.0 / aksh 0.0

### `GET /message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "body": "{\"runner_request_id\":\"f8ea199f-570a-5fca-b537-c837394d42dd\",\"run_service_url\":\"https://run-actions-3-azure-eastus.actions.githubusercontent.com/172/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
-  "messageId": 1416402749619197803,
+  "body": "{\"runner_request_id\":\"87c7d739-b68c-5722-852e-58cca49db4ed\",\"run_service_url\":\"https://run-actions-2-azure-eastus.actions.githubusercontent.com/155/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
+  "messageId": 2542779028974654817,
   "messageType": "RunnerJobRequest"
 }
```

**Status codes:** official: [200, None, None] | aksh: [200, 200, 200, None, None, None]

**Timing (ms):** p50: official 0.0 / aksh 24257.1 | p95: official 29485.8 / aksh 37025.8

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204] | aksh: [204, 204, 204]

**Timing (ms):** p50: official 57.1 / aksh 23.1 | p95: official 57.1 / aksh 24.9

### `POST /_apis/distributedtask/pools/{n}/agents`

**Header key differences:**

- official only: `{'x-tfs-fedauthredirect', 'accept-encoding'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,11 +2,11 @@
   "authorization": {
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "2eI1snKv4GXlyJ+***REDACTED***/qWNitjUijTuRKkL4YmQHluDdo/jM7IeeLk8LtpqvudAS31Ucqy/***REDACTED***+4FjgSGAcW+N6Ku2rmx6yGlQDoJ11ia/3r+***REDACTED***+***REDACTED***+Jegr7NRjL3fT//***REDACTED***/tCKpFfqMdltNPdQ=="
+      "modulus": "pi5/XniEWwz5XDU/XmvwVy3vti5p7l+***REDACTED***/VWQeP9jcN7ecXgNdMDF/***REDACTED***/ixolSq1zPm5ODaUXZSeHPLLvZKmr/***REDACTED***+ecv84aopWr/EpS/G8MQOLre0g/***REDACTED***+***REDACTED***=="
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
-  "name": "ephemeral-official-62-artifact-stress-3-1783648271",
+  "name": "ephemeral-aksh-62-artifact-stress-2-1783648338",
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
-    "clientId": "2d9b118e-6828-46de-865c-6916fe5ad9a7",
+    "clientId": "df876ef1-a8e8-43b0-a378-de60f81077bf",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "2eI1snKv4GXlyJ+***REDACTED***/qWNitjUijTuRKkL4YmQHluDdo/jM7IeeLk8LtpqvudAS31Ucqy/***REDACTED***+4FjgSGAcW+N6Ku2rmx6yGlQDoJ11ia/3r+***REDACTED***+***REDACTED***+Jegr7NRjL3fT//***REDACTED***/tCKpFfqMdltNPdQ=="
+      "modulus": "pi5/XniEWwz5XDU/XmvwVy3vti5p7l+***REDACTED***/VWQeP9jcN7ecXgNdMDF/***REDACTED***/ixolSq1zPm5ODaUXZSeHPLLvZKmr/***REDACTED***+ecv84aopWr/EpS/G8MQOLre0g/***REDACTED***+***REDACTED***=="
     }
   },
-  "createdOn": "2026-07-10T01:51:18.46Z",
+  "createdOn": "2026-07-10T01:52:19.067Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
   "ephemeral": true,
-  "id": 795,
+  "id": 799,
   "isElastic": false,
   "isVirtual": false,
   "labels": [
@@ -43,7 +43,7 @@
     }
   ],
   "maxParallelism": 1,
-  "name": "ephemeral-official-62-artifact-stress-3-1783648271",
+  "name": "ephemeral-aksh-62-artifact-stress-2-1783648338",
   "osDescription": "Ubuntu 24.04.4 LTS",
   "owningTenant": null,
   "properties": {
@@ -65,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-795",
+  "queueName": "taskagent-799",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 71.9 / aksh 146.1 | p95: official 168.7 / aksh 153.1

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

**Status codes:** official: [200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 450.7 / aksh 90.5 | p95: official 455.8 / aksh 179.0

### `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "runnerRequestId": "f8ea199f-570a-5fca-b537-c837394d42dd"
+  "runnerRequestId": "87c7d739-b68c-5722-852e-58cca49db4ed"
 }
```

**Status codes:** official: [200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 41.6 / aksh 41.6 | p95: official 41.6 / aksh 121.3

### `POST /actions/build/{guid}/jobs/{guid}/runnerresolve/actions`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,21 @@
 {
   "actions": [
+    {
+      "action": "actions/upload-artifact",
+      "version": "v4"
+    },
+    {
+      "action": "actions/upload-artifact",
+      "version": "v4"
+    },
+    {
+      "action": "actions/upload-artifact",
+      "version": "v4"
+    },
+    {
+      "action": "actions/upload-artifact",
+      "version": "v4"
+    },
     {
       "action": "actions/upload-artifact",
       "version": "v4"
```

**Response body diff:**

_identical_

**Status codes:** official: [200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 180.8 / aksh 161.8 | p95: official 180.8 / aksh 192.1

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

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 252.9 / aksh 272.7 | p95: official 256.4 / aksh 277.5

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
-    "id": 796,
-    "name": "ephemeral-official-62-artifact-stress-2-1783648271",
+    "id": 800,
+    "name": "ephemeral-aksh-62-artifact-stress-3-1783648338",
     "osDescription": "Ubuntu 24.04.4 LTS",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "container (PID: 9832)",
-  "sessionId": "00000000-0000-0000-0000-000000000000",
+  "ownerName": "container (PID: 9998)",
+  "sessionId": "e324d367-2a60-4ecd-a675-6f312bab892b",
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
-  "ownerName": "container (PID: 9832)",
-  "sessionId": "fcd46a38-3812-475d-8172-e50e8765592d"
+  "ownerName": "container (PID: 9998)",
+  "sessionId": "bbd7a12c-e8f5-4bd4-89ef-2d535d5f1644"
 }
```

**Status codes:** official: [201, 201, 201] | aksh: [201, 201, 201]

**Timing (ms):** p50: official 43.3 / aksh 122.3 | p95: official 117.7 / aksh 174.9

### `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,15 +2,78 @@
   "change_order": 1,
   "steps": [
     {
-      "completed_at": null,
-      "conclusion": 0,
-      "external_id": "08fdd7bd-e298-42fb-ae3f-d1885dc3aa81",
+      "completed_at": "2026-07-10T01:52:57.229Z",
+      "conclusion": 2,
+      "external_id": "d5fa32e2-25ac-4add-a88e-c68d6799d7ba",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-07-10T01:51:51.061Z",
-      "status": 3
+      "started_at": "2026-07-10T01:52:57.229Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-10T01:52:57.704Z",
+      "conclusion": 2,
+      "external_id": "f82d7ed7-356b-476a-bb28-93dab504580e",
+      "name": "Create varied artifacts",
+      "number": 2,
+      "started_at": "2026-07-10T01:52:57.690Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-10T01:52:58.756Z",
+      "conclusion": 2,
+      "external_id": "418268f1-4b35-4efd-a75f-6418912f550f",
+      "name": "Upload single file artifact",
+      "number": 3,
+      "started_at": "2026-07-10T01:52:57.882Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-10T01:52:59.957Z",
+      "conclusion": 2,
+      "external_id": "fb55ae91-c197-48ff-9f9e-de709fc61fc2",
+      "name": "Upload multi-file artifact",
+      "number": 4,
+      "started_at": "2026-07-10T01:52:58.897Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-10T01:53:01.162Z",
+      "conclusion": 2,
+      "external_id": "fb619091-0a9d-4266-b6a1-8780c51db3c3",
+      "name": "Upload binary artifact",
+      "number": 5,
+      "started_at": "2026-07-10T01:53:00.087Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-10T01:53:02.469Z",
+      "conclusion": 2,
+      "external_id": "cf2c7202-7cf6-4167-aa2e-6c513f658e46",
+      "name": "Upload nested artifact",
+      "number": 6,
+      "started_at": "2026-07-10T01:53:01.542Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-10T01:53:03.540Z",
+      "conclusion": 2,
+      "external_id": "79a1794f-0142-46e8-bed7-3849520469dc",
+      "name": "Upload checksums",
+      "number": 7,
+      "started_at": "2026-07-10T01:53:02.620Z",
+      "status": 6
+    },
+    {
+      "completed_at": "2026-07-10T01:53:03.670Z",
+      "conclusion": 2,
+      "external_id": "43bb8978-e1f8-47e1-91ce-7fa58b9842fa",
+      "name": "Complete job",
+      "number": 8,
+      "started_at": "2026-07-10T01:53:03.670Z",
+      "status": 6
     }
   ],
-  "workflow_job_run_backend_id": "f8ea199f-570a-5fca-b537-c837394d42dd",
-  "workflow_run_backend_id": "f093a791-891a-45d4-81de-92ba0c9bc40c"
+  "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+  "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 191.6 / aksh 39.1 | p95: official 191.6 / aksh 114.3

### `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,6 +1,6 @@
 {
-  "line_count": 44,
-  "uploaded_at": "2026-07-10T01:51:59.719Z",
-  "workflow_job_run_backend_id": "f8ea199f-570a-5fca-b537-c837394d42dd",
-  "workflow_run_backend_id": "f093a791-891a-45d4-81de-92ba0c9bc40c"
+  "line_count": 246,
+  "uploaded_at": "2026-07-10T01:53:04.075Z",
+  "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+  "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 37.0 / aksh 42.7 | p95: official 37.0 / aksh 47.2

### `POST /twirp/results.services.receiver.Receiver/CreateStepLogsMetadata`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,7 +1,7 @@
 {
-  "line_count": 17,
-  "step_backend_id": "08fdd7bd-e298-42fb-ae3f-d1885dc3aa81",
-  "uploaded_at": "2026-07-10T01:51:57.633Z",
-  "workflow_job_run_backend_id": "f8ea199f-570a-5fca-b537-c837394d42dd",
-  "workflow_run_backend_id": "f093a791-891a-45d4-81de-92ba0c9bc40c"
+  "line_count": 8,
+  "step_backend_id": "d5fa32e2-25ac-4add-a88e-c68d6799d7ba",
+  "uploaded_at": "2026-07-10T01:52:57.549Z",
+  "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+  "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 173.4 / aksh 53.8 | p95: official 206.7 / aksh 201.0

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "f8ea199f-570a-5fca-b537-c837394d42dd",
-  "workflow_run_backend_id": "f093a791-891a-45d4-81de-92ba0c9bc40c"
+  "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+  "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa5.blob.core.windows.net/actions-results/f093a791-891a-45d4-81de-92ba0c9bc40c/workflow-job-run-f8ea199f-570a-5fca-b537-c837394d42dd/logs/job/job-logs.txt?se=2026-07-10T02%3A51%3A59Z&sig=URJfnTSBqIFf4%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A14Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A44%3A14Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A51%3A54Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa11.blob.core.windows.net/actions-results/bc12a29c-b053-4e7d-9952-ffb52eb4272d/workflow-job-run-87c7d739-b68c-5722-852e-58cca49db4ed/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A04Z&sig=***REDACTED***%2FkJbOiH1Wg%3D&ske=2026-07-10T04%3A44%3A26Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A44%3A26Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A59Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 33.0 / aksh 34.9 | p95: official 33.0 / aksh 37.9

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "08fdd7bd-e298-42fb-ae3f-d1885dc3aa81",
-  "workflow_job_run_backend_id": "f8ea199f-570a-5fca-b537-c837394d42dd",
-  "workflow_run_backend_id": "f093a791-891a-45d4-81de-92ba0c9bc40c"
+  "step_backend_id": "d5fa32e2-25ac-4add-a88e-c68d6799d7ba",
+  "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+  "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
   "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
-  "logs_url": "https://productionresultssa5.blob.core.windows.net/actions-results/f093a791-891a-45d4-81de-92ba0c9bc40c/workflow-job-run-f8ea199f-570a-5fca-b537-c837394d42dd/logs/steps/step-logs-08fdd7bd-e298-42fb-ae3f-d1885dc3aa81.txt?se=2026-07-10T02%3A51%3A52Z&sig=%2FhW7ZvE8Eoz19lmDOVlksxBJ%2FIBt4o2DRgvL7OcNT3I%3D&ske=2026-07-10T04%3A44%3A55Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A44%3A55Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A51%3A47Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa11.blob.core.windows.net/actions-results/bc12a29c-b053-4e7d-9952-ffb52eb4272d/workflow-job-run-87c7d739-b68c-5722-852e-58cca49db4ed/logs/steps/step-logs-d5fa32e2-25ac-4add-a88e-c68d6799d7ba.txt?se=2026-07-10T02%3A52%3A57Z&sig=%2FMZIbHNDfF2SWaQ%2B%2FMcCf6ZETVfR7eQQvrBWTJMjVTI%3D&ske=2026-07-10T04%3A44%3A38Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A44%3A38Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A52Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 124.2 / aksh 35.6 | p95: official 126.7 / aksh 263.9

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
-  "jobMessageId": "f8ea199f-570a-5fca-b537-c837394d42dd",
+  "jobMessageId": "87c7d739-b68c-5722-852e-58cca49db4ed",
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
-          "v": "29063339943"
+          "v": "29063382057"
         },
         {
           "k": "run_number",
-          "v": "7"
+          "v": "8"
         },
         {
           "k": "retention_days",
@@ -712,7 +712,7 @@
       "d": [
         {
           "k": "check_run_id",
-          "v": 86269676864
+          "v": 86269798885
         },
         {
           "k": "workflow_ref",
@@ -771,7 +771,7 @@
   ],
   "jobContainer": null,
   "jobDisplayName": "upload-artifacts",
-  "jobId": "f8ea199f-570a-5fca-b537-c837394d42dd",
+  "jobId": "87c7d739-b68c-5722-852e-58cca49db4ed",
   "jobName": "__default",
   "jobOutputs": null,
   "jobServiceContainers": null,
@@ -851,30 +851,30 @@
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
-      "value": "***REDACTED***\\.H6-Jrx2lqrU7Ksa"
-    },
-    {
-      "type": "regex",
-      "value": "iXeX4WkY-***REDACTED***-NBByngEgSvg5MJLDhyd8BA"
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
+      "value": "***REDACTED***\\.rW0KadfcQMqYunb"
+    },
+    {
+      "type": "regex",
+      "value": "ztsrLtEKz7a2C3sJkc-***REDACTED***-nA"
     }
   ],
   "messageType": "RunnerJobRequest",
   "plan": {
     "artifactLocation": "",
     "artifactUri": "",
-    "planId": "f093a791-891a-45d4-81de-92ba0c9bc40c",
+    "planId": "bc12a29c-b053-4e7d-9952-ffb52eb4272d",
     "planType": "actions",
     "version": 0
   },
@@ -884,7 +884,7 @@
       {
         "authorization": {
           "parameters": {
-            "AccessToken": "***REDACTED***"
+            "AccessToken": "***REDACTED***"
           },
           "scheme": "OAuth"
         },
@@ -901,7 +901,7 @@
         "isReady": true,
         "isShared": false,
         "name": "SystemVssConnection",
-        "url": "https://run-actions-3-azure-eastus.actions.githubusercontent.com/172/"
+        "url": "https://run-actions-2-azure-eastus.actions.githubusercontent.com/155/"
       }
     ]
   },
@@ -918,7 +918,7 @@
         "lit": "Create varied artifacts",
         "type": 0
       },
-      "id": "d1b444ad-d585-4607-a91e-c2f6202476a8",
+      "id": "f82d7ed7-356b-476a-bb28-93dab504580e",
       "inputs": {
         "map": [
           {
@@ -955,7 +955,7 @@
         "lit": "Upload single file artifact",
         "type": 0
       },
-      "id": "f1953004-2c62-4cb7-805e-e7301e6c7037",
+      "id": "418268f1-4b35-4efd-a75f-6418912f550f",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1033,7 +1033,7 @@
         "lit": "Upload multi-file artifact",
         "type": 0
       },
-      "id": "3afa42a2-a7de-4f2c-b995-6e1d3b734500",
+      "id": "fb55ae91-c197-48ff-9f9e-de709fc61fc2",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1111,7 +1111,7 @@
         "lit": "Upload binary artifact",
         "type": 0
       },
-      "id": "116a0b4d-1cde-47d8-8951-82f62203d882",
+      "id": "fb619091-0a9d-4266-b6a1-8780c51db3c3",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1189,7 +1189,7 @@
         "lit": "Upload nested artifact",
         "type": 0
       },
-      "id": "065da5b4-b0b2-496e-b7c2-6a55935bf89b",
+      "id": "cf2c7202-7cf6-4167-aa2e-6c513f658e46",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1267,7 +1267,7 @@
         "lit": "Upload checksums",
         "type": 0
       },
-      "id": "8e15483f-278a-41dd-81b2-db1f52185763",
+      "id": "79a1794f-0142-46e8-bed7-3849520469dc",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1337,7 +1337,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "f093a791-891a-45d4-81de-92ba0c9bc40c",
+    "id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d",
     "location": null
   },
   "variables": {
@@ -1463,7 +1463,7 @@
     },
     "github_token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.from_run_service": {
       "value": "true"
@@ -1482,13 +1482,13 @@
     },
     "system.github.token": {
       "isSecret": true,
-      "value": "ghs_15368_***REDACTED***"
+      "value": "ghs_15368_***REDACTED***"
     },
     "system.github.token.permissions": {
       "value": "{\"Contents\":\"read\",\"Metadata\":\"read\",\"Packages\":\"read\"}"
     },
     "system.orchestrationId": {
-      "value": "f093a791-891a-45d4-81de-92ba0c9bc40c.upload-artifacts.__default"
+      "value": "bc12a29c-b053-4e7d-9952-ffb52eb4272d.upload-artifacts.__default"
     },
     "system.phaseDisplayName": {
       "value": "upload-artifacts"
```

**Status codes:** official: [200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 641.8 / aksh 437.6 | p95: official 641.8 / aksh 443.5

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,122 +1,112 @@
 {
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
-  "conclusion": "failed",
-  "jobId": "f8ea199f-570a-5fca-b537-c837394d42dd",
+  "conclusion": "succeeded",
+  "jobId": "87c7d739-b68c-5722-852e-58cca49db4ed",
   "outputs": {},
-  "planId": "f093a791-891a-45d4-81de-92ba0c9bc40c",
+  "planId": "bc12a29c-b053-4e7d-9952-ffb52eb4272d",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-07-10T01:51:51.9875772Z",
+      "completed_at": "2026-07-10T01:53:04.128Z",
       "conclusion": "succeeded",
-      "external_id": "08fdd7bd-e298-42fb-ae3f-d1885dc3aa81",
+      "external_id": "d5fa32e2-25ac-4add-a88e-c68d6799d7ba",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-07-10T01:51:51.0613678Z",
+      "started_at": "2026-07-10T01:53:04.128Z",
       "status": "completed",
       "type": "runner"
     },
     {
       "action_name": "sh",
-      "annotations": [
-        {
-          "endLine": 26,
-          "level": "failure",
-          "message": "Process completed with exit code 123.",
-          "startLine": 26,
-          "stepNumber": 2
-        }
-      ],
-      "completed_at": "2026-07-10T01:51:52.0170653Z",
-      "conclusion": "failed",
-      "external_id": "d1b444ad-d585-4607-a91e-c2f6202476a8",
+      "annotations": [],
+      "completed_at": "2026-07-10T01:53:04.128Z",
+      "conclusion": "succeeded",
+      "external_id": "f82d7ed7-356b-476a-bb28-93dab504580e",
       "name": "Create varied artifacts",
       "number": 2,
-      "started_at": "2026-07-10T01:51:51.9918831Z",
+      "started_at": "2026-07-10T01:53:04.128Z",
       "status": "completed",
       "type": "run"
     },
     {
+      "action_name": "actions/upload-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-07-10T01:51:52.0181263Z",
-      "conclusion": "skipped",
-      "external_id": "f1953004-2c62-4cb7-805e-e7301e6c7037",
+      "completed_at": "2026-07-10T01:53:04.128Z",
+      "conclusion": "succeeded",
+      "external_id": "418268f1-4b35-4efd-a75f-6418912f550f",
       "name": "Upload single file artifact",
       "number": 3,
-      "started_at": "2026-07-10T01:51:52.017879Z",
-      "status": "completed"
+      "started_at": "2026-07-10T01:53:04.128Z",
+      "status": "completed",
+      "type": "action"
     },
     {
+      "action_name": "actions/upload-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-07-10T01:51:52.0183882Z",
-      "conclusion": "skipped",
-      "external_id": "3afa42a2-a7de-4f2c-b995-6e1d3b734500",
+      "completed_at": "2026-07-10T01:53:04.128Z",
+      "conclusion": "succeeded",
+      "external_id": "fb55ae91-c197-48ff-9f9e-de709fc61fc2",
       "name": "Upload multi-file artifact",
       "number": 4,
-      "started_at": "2026-07-10T01:51:52.0182225Z",
-      "status": "completed"
+      "started_at": "2026-07-10T01:53:04.128Z",
+      "status": "completed",
+      "type": "action"
     },
     {
+      "action_name": "actions/upload-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-07-10T01:51:52.0186356Z",
-      "conclusion": "skipped",
-      "external_id": "116a0b4d-1cde-47d8-8951-82f62203d882",
+      "completed_at": "2026-07-10T01:53:04.128Z",
+      "conclusion": "succeeded",
+      "external_id": "fb619091-0a9d-4266-b6a1-8780c51db3c3",
       "name": "Upload binary artifact",
       "number": 5,
-      "started_at": "2026-07-10T01:51:52.0184781Z",
-      "status": "completed"
+      "started_at": "2026-07-10T01:53:04.128Z",
+      "status": "completed",
+      "type": "action"
     },
     {
+      "action_name": "actions/upload-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-07-10T01:51:52.0188752Z",
-      "conclusion": "skipped",
-      "external_id": "065da5b4-b0b2-496e-b7c2-6a55935bf89b",
+      "completed_at": "2026-07-10T01:53:04.128Z",
+      "conclusion": "succeeded",
+      "external_id": "cf2c7202-7cf6-4167-aa2e-6c513f658e46",
       "name": "Upload nested artifact",
       "number": 6,
-      "started_at": "2026-07-10T01:51:52.0187242Z",
-      "status": "completed"
+      "started_at": "2026-07-10T01:53:04.128Z",
+      "status": "completed",
+      "type": "action"
     },
     {
+      "action_name": "actions/upload-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-07-10T01:51:52.0191132Z",
-      "conclusion": "skipped",
-      "external_id": "8e15483f-278a-41dd-81b2-db1f52185763",
+      "completed_at": "2026-07-10T01:53:04.128Z",
+      "conclusion": "succeeded",
+      "external_id": "79a1794f-0142-46e8-bed7-3849520469dc",
       "name": "Upload checksums",
       "number": 7,
-      "started_at": "2026-07-10T01:51:52.0189626Z",
-      "status": "completed"
+      "started_at": "2026-07-10T01:53:04.128Z",
+      "status": "completed",
+      "type": "action"
     },
     {
       "action_name": "complete_job",
       "annotations": [],
-      "completed_at": "2026-07-10T01:51:52.3806427Z",
+      "completed_at": "2026-07-10T01:53:04.128Z",
       "conclusion": "succeeded",
-      "external_id": "9184c874-840e-437b-947c-024146273374",
+      "external_id": "43bb8978-e1f8-47e1-91ce-7fa58b9842fa",
       "name": "Complete job",
       "number": 8,
-      "started_at": "2026-07-10T01:51:52.0217973Z",
+      "started_at": "2026-07-10T01:53:04.128Z",
       "status": "completed",
       "type": "runner"
     }
   ],
   "telemetry": [
     {
-      "message": "Action archive cache usage: actions/upload-artifact@***REDACTED*** use cache False has cache False",
-      "type": "General"
-    },
-    {
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

**Status codes:** official: [204] | aksh: [204, 204, 204]

**Timing (ms):** p50: official 123.8 / aksh 43.4 | p95: official 123.8 / aksh 44.9

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "f8ea199f-570a-5fca-b537-c837394d42dd",
-  "planId": "f093a791-891a-45d4-81de-92ba0c9bc40c"
+  "jobId": "87c7d739-b68c-5722-852e-58cca49db4ed",
+  "planId": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "lockedUntil": "2026-07-10T02:01:50.923643188Z"
+  "lockedUntil": "2026-07-10T02:02:57.330446946Z"
 }
```

**Status codes:** official: [200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 34.7 / aksh 55.9 | p95: official 34.7 / aksh 60.8
