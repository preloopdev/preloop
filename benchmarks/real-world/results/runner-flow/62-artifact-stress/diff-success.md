# MITM comparison: 62-artifact-stress

**official**: ok — 212 flows
**aksh**: ok — 195 flows

## Endpoint matrix

| method | normalized path | offi # | aksh # | offi mean ms | aksh mean ms | offi statuses | aksh statuses |
|---|---|---|---|---|---|---|---|
| DELETE | `/session` | 3 | 0 | 34.4 | - | 204, 204, 204 |  |
| GET | `/_apis/connectionData?connectOptions={n}` | 0 | 3 | - | 71.5 |  | 200, 200, 200 |
| GET | `/_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}` | 18 | 0 | 27.0 | - | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-62-artifact-stress-1-1783648338&includeCapabilities=False` | 0 | 1 | - | 24.5 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-62-artifact-stress-2-1783648338&includeCapabilities=False` | 0 | 1 | - | 30.3 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-aksh-62-artifact-stress-3-1783648338&includeCapabilities=False` | 0 | 1 | - | 25.5 |  | 200 |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-1-1783648872&includeCapabilities=False` | 1 | 0 | 24.9 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-2-1783648872&includeCapabilities=False` | 1 | 0 | 20.4 | - | 200 |  |
| GET | `/_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-3-1783648872&includeCapabilities=False` | 1 | 0 | 23.9 | - | 200 |  |
| GET | `/_apis/distributedtask/pools?poolType=Automation` | 3 | 3 | 40.8 | 55.0 | 200, 200, 200 | 200, 200, 200 |
| GET | `/_ws/ingest.sock` | 3 | 3 | 27.7 | 116.4 | 101, 101, 101 | 101, 101, 101 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22checksums-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A36%3A34Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A34Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05` | 1 | 0 | 27.8 | - | 200 |  |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22nested-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=C%2Bk945A8tjtO7VnH61MkWDSDU%2F%2BmRyHC%2BmS4EBrG2VY%3D&ske=2026-07-10T04%3A44%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A18Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05` | 1 | 0 | 24.2 | - | 200 |  |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22nested-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A04Z&sig=%2ByJ4qpg3I83Z%***REDACTED***%3D&ske=2026-07-10T04%3A24%3A56Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A24%3A56Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05` | 1 | 0 | 27.7 | - | 200 |  |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22binary-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=j%***REDACTED***%3D&ske=2026-07-10T04%3A23%3A21Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A21Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05` | 1 | 0 | 62.4 | - | 200 |  |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22binary-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A03Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A24%3A56Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A24%3A56Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A58Z&sv=2025-11-05` | 1 | 0 | 145.8 | - | 200 |  |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22checksums-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A46%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A46%3A06Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 28.0 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A13Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A13Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05` | 1 | 0 | 23.8 | - | 200 |  |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A56Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A56Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05` | 1 | 0 | 27.3 | - | 200 |  |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22nested-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=VBiiRCl2zlENE6ICnkM1i%2Bqb%2BxN9kWoIbruPb%2Fujn10%3D&ske=2026-07-10T04%3A36%3A32Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A32Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 19.1 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22nested-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A11Z&sig=%2Bj2JqZwqgLeJw8ApIlD%2FyUuctskcfOrgg0eYvApKuuo%3D&ske=2026-07-10T04%3A36%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A24Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A06Z&sv=2025-11-05` | 0 | 1 | - | 20.8 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22multi-files-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A15Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 111.7 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22multi-files-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A08Z&sig=qPEvvHY%2FNHjTXWSOP%2BnXNvV0dNOzUXvvDnRte9TF43w%3D&ske=2026-07-10T04%3A43%3A59Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A43%3A59Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05` | 0 | 1 | - | 23.1 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22multi-files-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=IyzQvG4h%2FwV55Zglc%2Ff0AseUDmez4lX9F7e4CZu1r58%3D&ske=2026-07-10T05%3A26%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T01%3A26%3A18Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05` | 1 | 0 | 27.9 | - | 200 |  |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22multi-files-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A03Z&sig=OCz%2FdiYutfCHoyjCs5kMkgoKcmbAd7a%2F%2BaxcFzf%2F7m4%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A58Z&sv=2025-11-05` | 1 | 0 | 31.5 | - | 200 |  |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A36%3A30Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A30Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 22.2 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A16Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 25.6 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22binary-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=e8x4r%***REDACTED***%3D&ske=2026-07-10T04%3A44%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A06Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05` | 0 | 1 | - | 141.3 |  | 200 |
| GET | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22binary-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A10Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A05Z&sv=2025-11-05` | 0 | 1 | - | 55.1 |  | 200 |
| GET | `/actions/download-artifact/tar.gz/***REDACTED***` | 2 | 2 | 283.8 | 223.7 | 200, 200 | 200, 200 |
| GET | `/actions/upload-artifact/tar.gz/***REDACTED***` | 1 | 1 | 321.6 | 179.3 | 200 | 200 |
| GET | `/dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz` | 0 | 3 | - | 4313.0 |  | 200, 200, 200 |
| GET | `/dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz` | 0 | 3 | - | 3531.3 |  | 200, 200, 200 |
| GET | `/health` | 6 | 6 | 37.9 | 28.4 | 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200 |
| GET | `/message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 3 | 8 | 0 | 0 | None, None, None | None, None, None, None, None, None, None, None |
| GET | `/message?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false` | 3 | 6 | 35868.5 | 15912.3 | 200, 200, 200 | 200, 200, 200, None, None, None |
| GET | `/ready` | 3 | 3 | 31.4 | 21.4 | 204, 204, 204 | 204, 204, 204 |
| POST | `/_apis/distributedtask/pools/{n}/agents` | 3 | 3 | 67.8 | 148.1 | 200, 200, 200 | 200, 200, 200 |
| POST | `/_apis/oauth2/token` | 6 | 6 | 105.9 | 102.0 | 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200 |
| POST | `/acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64` | 3 | 3 | 57.4 | 67.9 | 200, 200, 200 | 200, 200, 200 |
| POST | `/actions/build/{guid}/jobs/{guid}/runnerresolve/actions` | 3 | 3 | 115.7 | 147.5 | 200, 200, 200 | 200, 200, 200 |
| POST | `/actions/runner-registration` | 3 | 3 | 206.1 | 268.4 | 200, 200, 200 | 200, 200, 200 |
| POST | `/session` | 3 | 3 | 42.3 | 113.1 | 201, 201, 201 | 201, 201, 201 |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact` | 5 | 5 | 152.8 | 138.3 | 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact` | 5 | 5 | 205.1 | 181.6 | 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200 |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL` | 9 | 9 | 74.7 | 46.5 | 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts` | 14 | 14 | 80.5 | 110.9 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | 16 | 3 | 50.6 | 63.9 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata` | 3 | 3 | 38.1 | 43.1 | 200, 200, 200 | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata` | 19 | 19 | 89.6 | 69.9 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | 3 | 3 | 34.7 | 35.5 | 200, 200, 200 | 200, 200, 200 |
| POST | `/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | 19 | 19 | 44.6 | 64.2 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 | 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200 |
| POST | `/{n}/acquirejob` | 3 | 3 | 406.0 | 395.6 | 200, 200, 200 | 200, 200, 200 |
| POST | `/{n}/completejob` | 3 | 3 | 74.7 | 43.0 | 204, 204, 204 | 204, 204, 204 |
| POST | `/{n}/renewjob` | 3 | 3 | 42.8 | 56.0 | 200, 200, 200 | 200, 200, 200 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A56Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A23%3A31Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A31Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A51Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 1 | 0 | 22.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A56Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A23%3A31Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A31Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A51Z&sv=2025-11-05&comp=blocklist` | 1 | 0 | 32.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A55Z&sig=st2nRcaraFfmB7xZ8Jyj%2F%2BfM48hfxE5brkaDFBDzIR0%3D&ske=2026-07-10T04%3A44%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A50Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 1 | 0 | 34.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A55Z&sig=st2nRcaraFfmB7xZ8Jyj%2F%2BfM48hfxE5brkaDFBDzIR0%3D&ske=2026-07-10T04%3A44%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A50Z&sv=2025-11-05&comp=blocklist` | 1 | 0 | 102.2 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A54Z&sig=Rd92oL25s9t08NlyVO%2Fm5jRoGt9JGxLqQBmKIVSKp1k%3D&ske=2026-07-10T04%3A45%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A49Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 1 | 0 | 155.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A54Z&sig=Rd92oL25s9t08NlyVO%2Fm5jRoGt9JGxLqQBmKIVSKp1k%3D&ske=2026-07-10T04%3A45%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A49Z&sv=2025-11-05&comp=blocklist` | 1 | 0 | 29.4 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A03Z&sig=%***REDACTED***%3D&ske=2026-07-10T04%3A46%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A46%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A58Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 0 | 1 | - | 22.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A03Z&sig=%***REDACTED***%3D&ske=2026-07-10T04%3A46%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A46%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A58Z&sv=2025-11-05&comp=blocklist` | 0 | 1 | - | 28.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A53Z&sig=nB%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A18Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A48Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 1 | 0 | 28.4 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A53Z&sig=nB%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A18Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A48Z&sv=2025-11-05&comp=blocklist` | 1 | 0 | 29.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A01Z&sig=***REDACTED***%2F7uNzST8%3D&ske=2026-07-10T04%3A44%3A43Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A56Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 0 | 1 | - | 25.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A01Z&sig=***REDACTED***%2F7uNzST8%3D&ske=2026-07-10T04%3A44%3A43Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A43Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A56Z&sv=2025-11-05&comp=blocklist` | 0 | 1 | - | 40.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A59Z&sig=GxR6P%***REDACTED***%2Fyo%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A54Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 0 | 1 | - | 28.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A59Z&sig=GxR6P%***REDACTED***%2Fyo%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A54Z&sv=2025-11-05&comp=blocklist` | 0 | 1 | - | 26.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A54Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A14Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A49Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 1 | 0 | 20.6 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A54Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A14Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A49Z&sv=2025-11-05&comp=blocklist` | 1 | 0 | 33.6 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A24Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A53Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 0 | 1 | - | 32.4 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A24Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A24Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A53Z&sv=2025-11-05&comp=blocklist` | 0 | 1 | - | 26.3 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A00Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A24%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A24%3A16Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A55Z&sv=2025-11-05&comp=block&blockid=***REDACTED***` | 0 | 1 | - | 66.8 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A53%3A00Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A24%3A16Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A24%3A16Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A55Z&sv=2025-11-05&comp=blocklist` | 0 | 1 | - | 25.9 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A04Z&sig=***REDACTED***%2FkJbOiH1Wg%3D&ske=2026-07-10T04%3A44%3A26Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A26Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A59Z&sv=2025-11-05` | 0 | 1 | - | 37.5 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A08Z&sig=3S2x%2FWbnmNtgpJwibySkELdnO%2B%2By4c1uJ7ACnkQD74Q%3D&ske=2026-07-10T04%3A45%3A22Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A22Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A03Z&sv=2025-11-05` | 0 | 1 | - | 30.1 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A12Z&sig=iO6tfVSesZos6ERYZt2SRbQkS%2B8X2QmsQ1uVFg9CTtY%3D&ske=2026-07-10T04%3A44%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A53%3A07Z&sv=2025-11-05` | 0 | 1 | - | 29.7 |  | 201 |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T03%3A01%3A58Z&sig=***REDACTED***%2B9DyORA2F4%3D&ske=2026-07-10T04%3A44%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A15Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A53Z&sv=2025-11-05` | 1 | 0 | 32.4 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T03%3A02%3A04Z&sig=0IM9DUn%2B%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A01Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A01Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05` | 1 | 0 | 28.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T03%3A02%3A06Z&sig=***REDACTED***%2FwvmFNu1v4%3D&ske=2026-07-10T04%3A36%3A25Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A25Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A02%3A01Z&sv=2025-11-05` | 1 | 0 | 22.9 | - | 201 |  |
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
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A53Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A48%3A14Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A48%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A48Z&sv=2025-11-05` | 1 | 0 | 21.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A53Z&sig=tw6LxxnsPaElASDhtxOsrD%2Fk4o9SicJNNS2cJJ4Q3xk%3D&ske=2026-07-10T04%3A44%3A27Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A27Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A48Z&sv=2025-11-05` | 1 | 0 | 82.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A54Z&sig=VxCCv3rcQzqaVQh8J7mDP9%2F2bVK4W3PoSEHGkfIcKTg%3D&ske=2026-07-10T04%3A44%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A49Z&sv=2025-11-05` | 1 | 0 | 22.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A55Z&sig=***REDACTED***%2FEdhAHg1k%3D&ske=2026-07-10T04%3A44%3A51Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A51Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A50Z&sv=2025-11-05` | 1 | 0 | 24.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A55Z&sig=q5Gn%***REDACTED***%3D&ske=2026-07-10T04%3A48%3A14Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A48%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A50Z&sv=2025-11-05` | 1 | 0 | 20.9 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A57Z&sig=Q5XKip8DugSplp4krJifVK%2B%2Bafk%2FZio0pYuE3DxeRKo%3D&ske=2026-07-10T04%3A44%3A08Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A08Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A52Z&sv=2025-11-05` | 1 | 0 | 28.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A12Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A12Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A53Z&sv=2025-11-05` | 1 | 0 | 75.8 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A13Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A53Z&sv=2025-11-05` | 1 | 0 | 109.4 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A01Z&sig=JpDgMyzbFzwr1i%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A00Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A00Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A56Z&sv=2025-11-05` | 1 | 0 | 32.1 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A01Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A36%3A58Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A58Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A56Z&sv=2025-11-05` | 1 | 0 | 29.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A03Z&sig=***REDACTED***%2Fw3Shte4%3D&ske=2026-07-10T04%3A44%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A58Z&sv=2025-11-05` | 1 | 0 | 21.2 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A03Z&sig=ev9UQrtlhEvqEkHLk4aq%2B2Xrk3HihM9VHvDvTX6B6zg%3D&ske=2026-07-10T04%3A23%3A49Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A49Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A58Z&sv=2025-11-05` | 1 | 0 | 22.8 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A04Z&sig=Q%2Fofcma0o%2FG91hIES%2FAAGT7ITa1Zq9OjvR0bMoHMWaY%3D&ske=2026-07-10T04%3A23%3A21Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A21Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05` | 1 | 0 | 98.0 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A04Z&sig=dn%2B3qzlKKI1IYHm%2F3h9iriXuoWLDyULcGA0rM31eB5k%3D&ske=2026-07-10T04%3A44%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05` | 1 | 0 | 21.9 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A04Z&sig=fK0cj%2BsZKP%***REDACTED***%3D&ske=2026-07-10T04%3A37%3A13Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A37%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05` | 1 | 0 | 20.2 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A04Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A45%3A03Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A03Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05` | 1 | 0 | 71.4 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A05Z&sig=SNE4r%***REDACTED***%2Fk6GmfZW0%3D&ske=2026-07-10T04%3A36%3A37Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A37Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A02%3A00Z&sv=2025-11-05` | 1 | 0 | 28.7 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A06Z&sig=0p966lp777%***REDACTED***%3D&ske=2026-07-10T04%3A23%3A25Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A25Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A02%3A01Z&sv=2025-11-05` | 1 | 0 | 19.2 | - | 201 |  |
| PUT | `/actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A06Z&sig=lkqMQpcCpER4N9q%2BZyHVHqf%2B%2BmEuRxvYwWf9EXCvi6s%3D&ske=2026-07-10T04%3A36%3A36Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A36Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A02%3A01Z&sv=2025-11-05` | 1 | 0 | 73.6 | - | 201 |  |

## Missing endpoints

### official only

- `DELETE /session`
- `GET /_apis/connectionData?connectOptions={n}&lastChangeId={n}&lastChangeId64={n}`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-1-1783648872&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-2-1783648872&includeCapabilities=False`
- `GET /_apis/distributedtask/pools/{n}/agents?agentName=ephemeral-official-62-artifact-stress-3-1783648872&includeCapabilities=False`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22checksums-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A36%3A34Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A34Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22nested-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=C%2Bk945A8tjtO7VnH61MkWDSDU%2F%2BmRyHC%2BmS4EBrG2VY%3D&ske=2026-07-10T04%3A44%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A18Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22nested-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A04Z&sig=%2ByJ4qpg3I83Z%***REDACTED***%3D&ske=2026-07-10T04%3A24%3A56Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A24%3A56Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22binary-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=j%***REDACTED***%3D&ske=2026-07-10T04%3A23%3A21Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A21Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22binary-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A03Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A24%3A56Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A24%3A56Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A58Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A13Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A13Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A56Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A56Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22multi-files-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=IyzQvG4h%2FwV55Zglc%2Ff0AseUDmez4lX9F7e4CZu1r58%3D&ske=2026-07-10T05%3A26%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T01%3A26%3A18Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05`
- `GET /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22multi-files-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A03Z&sig=OCz%2FdiYutfCHoyjCs5kMkgoKcmbAd7a%2F%2BaxcFzf%2F7m4%3D&ske=2026-07-10T04%3A44%3A07Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A07Z&sktid={guid}&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A58Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A56Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A23%3A31Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A31Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A51Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A56Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A23%3A31Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A31Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A51Z&sv=2025-11-05&comp=blocklist`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A55Z&sig=st2nRcaraFfmB7xZ8Jyj%2F%2BfM48hfxE5brkaDFBDzIR0%3D&ske=2026-07-10T04%3A44%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A50Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A55Z&sig=st2nRcaraFfmB7xZ8Jyj%2F%2BfM48hfxE5brkaDFBDzIR0%3D&ske=2026-07-10T04%3A44%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A50Z&sv=2025-11-05&comp=blocklist`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A54Z&sig=Rd92oL25s9t08NlyVO%2Fm5jRoGt9JGxLqQBmKIVSKp1k%3D&ske=2026-07-10T04%3A45%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A49Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A54Z&sig=Rd92oL25s9t08NlyVO%2Fm5jRoGt9JGxLqQBmKIVSKp1k%3D&ske=2026-07-10T04%3A45%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A49Z&sv=2025-11-05&comp=blocklist`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A53Z&sig=nB%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A18Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A48Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A53Z&sig=nB%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A18Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A18Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A48Z&sv=2025-11-05&comp=blocklist`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A54Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A14Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A49Z&sv=2025-11-05&comp=block&blockid=***REDACTED***`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A54Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A14Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A49Z&sv=2025-11-05&comp=blocklist`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T03%3A01%3A58Z&sig=***REDACTED***%2B9DyORA2F4%3D&ske=2026-07-10T04%3A44%3A15Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A15Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A53Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T03%3A02%3A04Z&sig=0IM9DUn%2B%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A01Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A01Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/job/job-logs.txt?se=2026-07-10T03%3A02%3A06Z&sig=***REDACTED***%2FwvmFNu1v4%3D&ske=2026-07-10T04%3A36%3A25Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A25Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A02%3A01Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A53Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A48%3A14Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A48%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A48Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A53Z&sig=tw6LxxnsPaElASDhtxOsrD%2Fk4o9SicJNNS2cJJ4Q3xk%3D&ske=2026-07-10T04%3A44%3A27Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A27Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A48Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A54Z&sig=VxCCv3rcQzqaVQh8J7mDP9%2F2bVK4W3PoSEHGkfIcKTg%3D&ske=2026-07-10T04%3A44%3A33Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A33Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A49Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A55Z&sig=***REDACTED***%2FEdhAHg1k%3D&ske=2026-07-10T04%3A44%3A51Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A51Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A50Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A55Z&sig=q5Gn%***REDACTED***%3D&ske=2026-07-10T04%3A48%3A14Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A48%3A14Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A50Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A57Z&sig=Q5XKip8DugSplp4krJifVK%2B%2Bafk%2FZio0pYuE3DxeRKo%3D&ske=2026-07-10T04%3A44%3A08Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A08Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A52Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A12Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A12Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A53Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A01%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A13Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A53Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A01Z&sig=JpDgMyzbFzwr1i%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A00Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A00Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A56Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A01Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A36%3A58Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A58Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A56Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A03Z&sig=***REDACTED***%2Fw3Shte4%3D&ske=2026-07-10T04%3A44%3A06Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A06Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A58Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A03Z&sig=ev9UQrtlhEvqEkHLk4aq%2B2Xrk3HihM9VHvDvTX6B6zg%3D&ske=2026-07-10T04%3A23%3A49Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A49Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A58Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A04Z&sig=Q%2Fofcma0o%2FG91hIES%2FAAGT7ITa1Zq9OjvR0bMoHMWaY%3D&ske=2026-07-10T04%3A23%3A21Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A21Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A04Z&sig=dn%2B3qzlKKI1IYHm%2F3h9iriXuoWLDyULcGA0rM31eB5k%3D&ske=2026-07-10T04%3A44%3A09Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A44%3A09Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A04Z&sig=fK0cj%2BsZKP%***REDACTED***%3D&ske=2026-07-10T04%3A37%3A13Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A37%3A13Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A04Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A45%3A03Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A45%3A03Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A59Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A05Z&sig=SNE4r%***REDACTED***%2Fk6GmfZW0%3D&ske=2026-07-10T04%3A36%3A37Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A37Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A02%3A00Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A06Z&sig=0p966lp777%***REDACTED***%3D&ske=2026-07-10T04%3A23%3A25Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A23%3A25Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A02%3A01Z&sv=2025-11-05`
- `PUT /actions-results/{guid}/workflow-job-run-{guid}/logs/steps/step-logs-{guid}.txt?se=2026-07-10T03%3A02%3A06Z&sig=lkqMQpcCpER4N9q%2BZyHVHqf%2B%2BmEuRxvYwWf9EXCvi6s%3D&ske=2026-07-10T04%3A36%3A36Z&skoid={guid}&sks=b&skt=2026-07-10T00%3A36%3A36Z&sktid={guid}&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A02%3A01Z&sv=2025-11-05`

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
- `GET /dist/v20.19.0/node-v20.19.0-linux-arm64.tar.gz`
- `GET /dist/v24.3.0/node-v24.3.0-linux-arm64.tar.gz`
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

- official only: `{'accept-encoding', 'x-tfs-fedauthredirect'}`

**Response body diff:**

```diff
--- official
+++ aksh
@@ -10,7 +10,7 @@
       "isInternal": true,
       "name": "Default",
       "scope": "5e4d430c-d710-4b62-aed8-555ffd0f7592",
-      "size": 13,
+      "size": 11,
       "targetSize": null
     },
     {
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 24.6 / aksh 57.3 | p95: official 75.5 / aksh 81.5

### `GET /_ws/ingest.sock`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [101, 101, 101] | aksh: [101, 101, 101]

**Timing (ms):** p50: official 26.1 / aksh 156.5 | p95: official 31.6 / aksh 169.3

### `GET /actions/download-artifact/tar.gz/***REDACTED***`

**Header key differences:**

- official only: `{'authorization'}`
- aksh only: `{'accept'}`

**Status codes:** official: [200, 200] | aksh: [200, 200]

**Timing (ms):** p50: official 298.5 / aksh 231.1 | p95: official 298.5 / aksh 231.1

### `GET /actions/upload-artifact/tar.gz/***REDACTED***`

**Header key differences:**

- official only: `{'authorization'}`
- aksh only: `{'accept'}`

**Status codes:** official: [200] | aksh: [200]

**Timing (ms):** p50: official 321.6 / aksh 179.3 | p95: official 321.6 / aksh 179.3

### `GET /health`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 35.1 / aksh 26.1 | p95: official 56.1 / aksh 42.2

### `GET /message?sessionId={guid}&status=Busy&runnerVersion=2.335.1&os=Linux&architecture=ARM64&disableUpdate=false`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Status codes:** official: [None, None, None] | aksh: [None, None, None, None, None, None, None, None]

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
-  "body": "{\"runner_request_id\":\"9e3559d2-25e8-56e5-b20e-69ce905f9e65\",\"run_service_url\":\"https://run-actions-1-azure-eastus.actions.githubusercontent.com/126/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
-  "messageId": 2699623362490510091,
+  "body": "{\"runner_request_id\":\"87c7d739-b68c-5722-852e-58cca49db4ed\",\"run_service_url\":\"https://run-actions-2-azure-eastus.actions.githubusercontent.com/155/\",\"billing_owner_id\":\"O_kgDOEbddog\",\"should_acknowledge\":true}",
+  "messageId": 2542779028974654817,
   "messageType": "RunnerJobRequest"
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200, None, None, None]

**Timing (ms):** p50: official 38783.2 / aksh 24257.1 | p95: official 38863.5 / aksh 37025.8

### `GET /ready`

**Header key differences:**

- aksh only: `{'accept'}`

**Status codes:** official: [204, 204, 204] | aksh: [204, 204, 204]

**Timing (ms):** p50: official 26.1 / aksh 23.1 | p95: official 49.1 / aksh 24.9

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
-      "modulus": "vqLGaGrOahUXNELd+WPRZl4QLQUCV6VWZHK+AwCll9zqvlWG+***REDACTED***+woSE1EW8aqNvbC+Bd+GL0k1yZN/I0lCOXC+46u8JZxIY37yPcNhHxY2+***REDACTED***+***REDACTED***/01n+***REDACTED***=="
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
-  "name": "ephemeral-official-62-artifact-stress-1-1783648872",
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
-    "clientId": "3192bf88-3460-4bf3-8525-f6f812954309",
+    "clientId": "df876ef1-a8e8-43b0-a378-de60f81077bf",
     "publicKey": {
       "exponent": "AQAB",
-      "modulus": "vqLGaGrOahUXNELd+WPRZl4QLQUCV6VWZHK+AwCll9zqvlWG+***REDACTED***+woSE1EW8aqNvbC+Bd+GL0k1yZN/I0lCOXC+46u8JZxIY37yPcNhHxY2+***REDACTED***+***REDACTED***/01n+***REDACTED***=="
+      "modulus": "pi5/XniEWwz5XDU/XmvwVy3vti5p7l+***REDACTED***/VWQeP9jcN7ecXgNdMDF/***REDACTED***/ixolSq1zPm5ODaUXZSeHPLLvZKmr/***REDACTED***+ecv84aopWr/EpS/G8MQOLre0g/***REDACTED***+***REDACTED***=="
     }
   },
-  "createdOn": "2026-07-10T02:01:19.633Z",
+  "createdOn": "2026-07-10T01:52:19.067Z",
   "currentParallelism": 0,
-  "disableUpdate": false,
+  "disableUpdate": true,
   "enabled": true,
   "ephemeral": true,
-  "id": 804,
+  "id": 799,
   "isElastic": false,
   "isVirtual": false,
   "labels": [
@@ -43,7 +43,7 @@
     }
   ],
   "maxParallelism": 1,
-  "name": "ephemeral-official-62-artifact-stress-1-1783648872",
+  "name": "ephemeral-aksh-62-artifact-stress-2-1783648338",
   "osDescription": "Ubuntu 24.04.4 LTS",
   "owningTenant": null,
   "properties": {
@@ -65,7 +65,7 @@
     }
   },
   "provisioningState": "Provisioned",
-  "queueName": "taskagent-804",
+  "queueName": "taskagent-799",
   "runnerGroupId": 1,
   "runnerGroupName": null,
   "status": "offline",
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 68.7 / aksh 146.1 | p95: official 70.4 / aksh 153.1

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

**Status codes:** official: [200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 86.9 / aksh 90.5 | p95: official 166.3 / aksh 179.0

### `POST /acknowledge?sessionId={guid}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=ARM64`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "runnerRequestId": "9e3559d2-25e8-56e5-b20e-69ce905f9e65"
+  "runnerRequestId": "87c7d739-b68c-5722-852e-58cca49db4ed"
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 48.2 / aksh 41.6 | p95: official 82.2 / aksh 121.3

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

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 117.8 / aksh 161.8 | p95: official 122.4 / aksh 192.1

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

**Timing (ms):** p50: official 206.2 / aksh 272.7 | p95: official 212.0 / aksh 277.5

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
-    "id": 804,
-    "name": "ephemeral-official-62-artifact-stress-1-1783648872",
+    "id": 800,
+    "name": "ephemeral-aksh-62-artifact-stress-3-1783648338",
     "osDescription": "Ubuntu 24.04.4 LTS",
     "provisioningState": null,
     "status": 0,
     "version": "2.335.1"
   },
-  "ownerName": "container (PID: 10791)",
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
-  "ownerName": "container (PID: 10791)",
-  "sessionId": "c072509f-c27e-4d79-af00-d0ca4a603791"
+  "ownerName": "container (PID: 9998)",
+  "sessionId": "bbd7a12c-e8f5-4bd4-89ef-2d535d5f1644"
 }
```

**Status codes:** official: [201, 201, 201] | aksh: [201, 201, 201]

**Timing (ms):** p50: official 42.9 / aksh 122.3 | p95: official 47.5 / aksh 174.9

### `POST /twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,7 +1,7 @@
 {
-  "expires_at": "2026-07-11T02:01:52.825Z",
-  "name": "single-file-29063721609",
+  "expires_at": "2026-07-11T01:52:58.016Z",
+  "name": "single-file-29063382057",
   "version": 4,
-  "workflow_job_run_backend_id": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
-  "workflow_run_backend_id": "f4181416-0894-40ac-a92b-39fa46faed2a"
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
   "ok": true,
-  "signed_upload_url": "https://productionresultssa14.blob.core.windows.net/actions-results/f4181416-0894-40ac-a92b-39fa46faed2a/workflow-job-run-9e3559d2-25e8-56e5-b20e-69ce905f9e65/artifacts/***REDACTED***.zip?se=2026-07-10T03%3A01%3A53Z&sig=nB%***REDACTED***%3D&ske=2026-07-10T04%3A45%3A18Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A45%3A18Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A48Z&sv=2025-11-05"
+  "signed_upload_url": "https://productionresultssa11.blob.core.windows.net/actions-results/bc12a29c-b053-4e7d-9952-ffb52eb4272d/workflow-job-run-87c7d739-b68c-5722-852e-58cca49db4ed/artifacts/***REDACTED***.zip?se=2026-07-10T02%3A52%3A58Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A24Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A44%3A24Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A53Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200]

**Timing (ms):** p50: official 151.6 / aksh 143.9 | p95: official 200.7 / aksh 156.9

### `POST /twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,7 +1,7 @@
 {
-  "hash": "sha256:***REDACTED***",
-  "name": "single-file-29063721609",
+  "hash": "sha256:***REDACTED***",
+  "name": "single-file-29063382057",
   "size": "188",
-  "workflow_job_run_backend_id": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
-  "workflow_run_backend_id": "f4181416-0894-40ac-a92b-39fa46faed2a"
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
-  "artifact_id": "8216370207",
+  "artifact_id": "8216250428",
   "ok": true
 }
```

**Status codes:** official: [200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200]

**Timing (ms):** p50: official 217.6 / aksh 167.3 | p95: official 234.2 / aksh 239.9

### `POST /twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "name": "single-file-29063721609",
-  "workflow_job_run_backend_id": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
-  "workflow_run_backend_id": "f4181416-0894-40ac-a92b-39fa46faed2a"
+  "name": "checksums-29063382057",
+  "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+  "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,3 +1,3 @@
 {
-  "signed_url": "https://productionresultssa14.blob.core.windows.net/actions-results/f4181416-0894-40ac-a92b-39fa46faed2a/workflow-job-run-9e3559d2-25e8-56e5-b20e-69ce905f9e65/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22single-file-29063721609.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A12%3A02Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A44%3A56Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A44%3A56Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T02%3A01%3A57Z&sv=2025-11-05"
+  "signed_url": "https://productionresultssa11.blob.core.windows.net/actions-results/bc12a29c-b053-4e7d-9952-ffb52eb4272d/workflow-job-run-87c7d739-b68c-5722-852e-58cca49db4ed/artifacts/***REDACTED***.zip?rscd=attachment%3B+filename%3D%22checksums-29063382057.zip%22&rsct=application%2Fzip&se=2026-07-10T02%3A03%3A07Z&sig=***REDACTED***%3D&ske=2026-07-10T04%3A46%3A06Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A46%3A06Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=r&spr=https&sr=b&st=2026-07-10T01%3A53%3A02Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 67.0 / aksh 46.6 | p95: official 137.4 / aksh 55.2

### `POST /twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,4 @@
 {
-  "name_filter": "single-file-29063721609",
-  "workflow_job_run_backend_id": "c5deb957-6936-5521-9cc6-f079b3e77517",
-  "workflow_run_backend_id": "f4181416-0894-40ac-a92b-39fa46faed2a"
+  "workflow_job_run_backend_id": "da551c6c-0632-5479-bfdc-57f01b9a95b4",
+  "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

```diff
--- official
+++ aksh
@@ -1,13 +1,49 @@
 {
   "artifacts": [
     {
-      "created_at": "2026-07-10T02:01:53Z",
-      "database_id": "8216370207",
-      "digest": "sha256:***REDACTED***",
-      "name": "single-file-29063721609",
+      "created_at": "2026-07-10T01:52:58Z",
+      "database_id": "8216250428",
+      "digest": "sha256:***REDACTED***",
+      "name": "single-file-29063382057",
       "size": "188",
-      "workflow_job_run_backend_id": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
-      "workflow_run_backend_id": "f4181416-0894-40ac-a92b-39fa46faed2a"
+      "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+      "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
+    },
+    {
+      "created_at": "2026-07-10T01:52:59Z",
+      "database_id": "8216250705",
+      "digest": "sha256:***REDACTED***",
+      "name": "multi-files-29063382057",
+      "size": "1525",
+      "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+      "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
+    },
+    {
+      "created_at": "2026-07-10T01:53:01Z",
+      "database_id": "8216250986",
+      "digest": "sha256:***REDACTED***",
+      "name": "binary-29063382057",
+      "size": "102569",
+      "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+      "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
+    },
+    {
+      "created_at": "2026-07-10T01:53:02Z",
+      "database_id": "8216251275",
+      "digest": "sha256:***REDACTED***",
+      "name": "nested-29063382057",
+      "size": "417",
+      "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+      "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
+    },
+    {
+      "created_at": "2026-07-10T01:53:03Z",
+      "database_id": "8216251530",
+      "digest": "sha256:***REDACTED***",
+      "name": "checksums-29063382057",
+      "size": "840",
+      "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+      "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
     }
   ]
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 73.4 / aksh 106.9 | p95: official 158.2 / aksh 386.8

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
-      "external_id": "76abf30b-50d6-4d32-998c-267f6a757308",
+      "completed_at": "2026-07-10T01:52:57.229Z",
+      "conclusion": 2,
+      "external_id": "d5fa32e2-25ac-4add-a88e-c68d6799d7ba",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-07-10T02:01:51.963Z",
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
-  "workflow_job_run_backend_id": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
-  "workflow_run_backend_id": "f4181416-0894-40ac-a92b-39fa46faed2a"
+  "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+  "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 46.0 / aksh 39.1 | p95: official 123.3 / aksh 114.3

### `POST /twirp/results.services.receiver.Receiver/CreateJobLogsMetadata`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,6 +1,6 @@
 {
-  "line_count": 184,
-  "uploaded_at": "2026-07-10T02:01:58.576Z",
-  "workflow_job_run_backend_id": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
-  "workflow_run_backend_id": "f4181416-0894-40ac-a92b-39fa46faed2a"
+  "line_count": 246,
+  "uploaded_at": "2026-07-10T01:53:04.075Z",
+  "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+  "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 37.8 / aksh 42.7 | p95: official 40.4 / aksh 47.2

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
-  "step_backend_id": "76abf30b-50d6-4d32-998c-267f6a757308",
-  "uploaded_at": "2026-07-10T02:01:53.251Z",
-  "workflow_job_run_backend_id": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
-  "workflow_run_backend_id": "f4181416-0894-40ac-a92b-39fa46faed2a"
+  "line_count": 8,
+  "step_backend_id": "d5fa32e2-25ac-4add-a88e-c68d6799d7ba",
+  "uploaded_at": "2026-07-10T01:52:57.549Z",
+  "workflow_job_run_backend_id": "87c7d739-b68c-5722-852e-58cca49db4ed",
+  "workflow_run_backend_id": "bc12a29c-b053-4e7d-9952-ffb52eb4272d"
 }
```

**Response body diff:**

_identical_

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 60.4 / aksh 53.8 | p95: official 228.0 / aksh 201.0

### `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "workflow_job_run_backend_id": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
-  "workflow_run_backend_id": "f4181416-0894-40ac-a92b-39fa46faed2a"
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
-  "logs_url": "https://productionresultssa14.blob.core.windows.net/actions-results/f4181416-0894-40ac-a92b-39fa46faed2a/workflow-job-run-9e3559d2-25e8-56e5-b20e-69ce905f9e65/logs/job/job-logs.txt?se=2026-07-10T03%3A01%3A58Z&sig=***REDACTED***%2B9DyORA2F4%3D&ske=2026-07-10T04%3A44%3A15Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A44%3A15Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A53Z&sv=2025-11-05"
+  "logs_url": "https://productionresultssa11.blob.core.windows.net/actions-results/bc12a29c-b053-4e7d-9952-ffb52eb4272d/workflow-job-run-87c7d739-b68c-5722-852e-58cca49db4ed/logs/job/job-logs.txt?se=2026-07-10T02%3A53%3A04Z&sig=***REDACTED***%2FkJbOiH1Wg%3D&ske=2026-07-10T04%3A44%3A26Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A44%3A26Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A59Z&sv=2025-11-05"
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 34.7 / aksh 34.9 | p95: official 36.7 / aksh 37.9

### `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,5 +1,5 @@
 {
-  "step_backend_id": "76abf30b-50d6-4d32-998c-267f6a757308",
-  "workflow_job_run_backend_id": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
-  "workflow_run_backend_id": "f4181416-0894-40ac-a92b-39fa46faed2a"
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
-  "logs_url": "https://productionresultssa14.blob.core.windows.net/actions-results/f4181416-0894-40ac-a92b-39fa46faed2a/workflow-job-run-9e3559d2-25e8-56e5-b20e-69ce905f9e65/logs/steps/step-logs-76abf30b-50d6-4d32-998c-267f6a757308.txt?se=2026-07-10T03%3A01%3A53Z&sig=tw6LxxnsPaElASDhtxOsrD%2Fk4o9SicJNNS2cJJ4Q3xk%3D&ske=2026-07-10T04%3A44%3A27Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A44%3A27Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T02%3A01%3A48Z&sv=2025-11-05",
+  "logs_url": "https://productionresultssa11.blob.core.windows.net/actions-results/bc12a29c-b053-4e7d-9952-ffb52eb4272d/workflow-job-run-87c7d739-b68c-5722-852e-58cca49db4ed/logs/steps/step-logs-d5fa32e2-25ac-4add-a88e-c68d6799d7ba.txt?se=2026-07-10T02%3A52%3A57Z&sig=%2FMZIbHNDfF2SWaQ%2B%2FMcCf6ZETVfR7eQQvrBWTJMjVTI%3D&ske=2026-07-10T04%3A44%3A38Z&skoid=ca7593d4-ee42-46cd-af88-8b886a2f84eb&sks=b&skt=2026-07-10T00%3A44%3A38Z&sktid=398a6654-997b-47e9-b12b-9515b896b4de&skv=2025-11-05&sp=cw&spr=https&sr=b&st=2026-07-10T01%3A52%3A52Z&sv=2025-11-05",
   "soft_size_limit": "1048576"
 }
```

**Status codes:** official: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200] | aksh: [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]

**Timing (ms):** p50: official 38.4 / aksh 35.6 | p95: official 111.9 / aksh 263.9

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
-  "jobMessageId": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
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
-          "v": "29063721609"
+          "v": "29063382057"
         },
         {
           "k": "run_number",
-          "v": "10"
+          "v": "8"
         },
         {
           "k": "retention_days",
@@ -712,7 +712,7 @@
       "d": [
         {
           "k": "check_run_id",
-          "v": 86270807373
+          "v": 86269798885
         },
         {
           "k": "workflow_ref",
@@ -771,7 +771,7 @@
   ],
   "jobContainer": null,
   "jobDisplayName": "upload-artifacts",
-  "jobId": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
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
-      "value": "***REDACTED***\\.wZlTzTH8hiOHGSR"
-    },
-    {
-      "type": "regex",
-      "value": "kJ9k9goltAhWQJmPO-***REDACTED***"
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
-    "planId": "f4181416-0894-40ac-a92b-39fa46faed2a",
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
-        "url": "https://run-actions-1-azure-eastus.actions.githubusercontent.com/126/"
+        "url": "https://run-actions-2-azure-eastus.actions.githubusercontent.com/155/"
       }
     ]
   },
@@ -918,7 +918,7 @@
         "lit": "Create varied artifacts",
         "type": 0
       },
-      "id": "8cd7d13d-6d4b-4b1a-8832-567d8cff4309",
+      "id": "f82d7ed7-356b-476a-bb28-93dab504580e",
       "inputs": {
         "map": [
           {
@@ -955,7 +955,7 @@
         "lit": "Upload single file artifact",
         "type": 0
       },
-      "id": "3fdf1c5d-e824-4f88-a748-e0f4389ab16d",
+      "id": "418268f1-4b35-4efd-a75f-6418912f550f",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1033,7 +1033,7 @@
         "lit": "Upload multi-file artifact",
         "type": 0
       },
-      "id": "13ef89e1-b6e2-4412-bca4-0fb0b42c3cd9",
+      "id": "fb55ae91-c197-48ff-9f9e-de709fc61fc2",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1111,7 +1111,7 @@
         "lit": "Upload binary artifact",
         "type": 0
       },
-      "id": "8c9298f4-7674-40cd-bbec-70f4d57aa452",
+      "id": "fb619091-0a9d-4266-b6a1-8780c51db3c3",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1189,7 +1189,7 @@
         "lit": "Upload nested artifact",
         "type": 0
       },
-      "id": "37c177dd-976d-4883-afb6-5236fbb6e198",
+      "id": "cf2c7202-7cf6-4167-aa2e-6c513f658e46",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1267,7 +1267,7 @@
         "lit": "Upload checksums",
         "type": 0
       },
-      "id": "5b210398-90c2-4893-9d4b-c05d2fe9a803",
+      "id": "79a1794f-0142-46e8-bed7-3849520469dc",
       "inputs": {
         "col": 11,
         "file": 1,
@@ -1337,7 +1337,7 @@
   ],
   "timeline": {
     "changeId": 0,
-    "id": "f4181416-0894-40ac-a92b-39fa46faed2a",
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
-      "value": "f4181416-0894-40ac-a92b-39fa46faed2a.upload-artifacts.__default"
+      "value": "bc12a29c-b053-4e7d-9952-ffb52eb4272d.upload-artifacts.__default"
     },
     "system.phaseDisplayName": {
       "value": "upload-artifacts"
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 401.0 / aksh 437.6 | p95: official 462.6 / aksh 443.5

### `POST /{n}/completejob`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -2,136 +2,111 @@
   "annotations": [],
   "billingOwnerId": "O_kgDOEbddog",
   "conclusion": "succeeded",
-  "jobId": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
+  "jobId": "87c7d739-b68c-5722-852e-58cca49db4ed",
   "outputs": {},
-  "planId": "f4181416-0894-40ac-a92b-39fa46faed2a",
+  "planId": "bc12a29c-b053-4e7d-9952-ffb52eb4272d",
   "stepResults": [
     {
       "action_name": "setup_job",
       "annotations": [],
-      "completed_at": "2026-07-10T02:01:52.7158999Z",
+      "completed_at": "2026-07-10T01:53:04.128Z",
       "conclusion": "succeeded",
-      "external_id": "76abf30b-50d6-4d32-998c-267f6a757308",
+      "external_id": "d5fa32e2-25ac-4add-a88e-c68d6799d7ba",
       "name": "Set up job",
       "number": 1,
-      "started_at": "2026-07-10T02:01:51.9638772Z",
+      "started_at": "2026-07-10T01:53:04.128Z",
       "status": "completed",
       "type": "runner"
     },
     {
       "action_name": "sh",
       "annotations": [],
-      "completed_at": "2026-07-10T02:01:52.7449693Z",
+      "completed_at": "2026-07-10T01:53:04.128Z",
       "conclusion": "succeeded",
-      "external_id": "8cd7d13d-6d4b-4b1a-8832-567d8cff4309",
+      "external_id": "f82d7ed7-356b-476a-bb28-93dab504580e",
       "name": "Create varied artifacts",
       "number": 2,
-      "started_at": "2026-07-10T02:01:52.7201121Z",
+      "started_at": "2026-07-10T01:53:04.128Z",
       "status": "completed",
       "type": "run"
     },
     {
-      "action_name": "actions/upload-artifact",
+      "action_name": "actions/upload-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-07-10T02:01:53.7708256Z",
+      "completed_at": "2026-07-10T01:53:04.128Z",
       "conclusion": "succeeded",
-      "external_id": "3fdf1c5d-e824-4f88-a748-e0f4389ab16d",
+      "external_id": "418268f1-4b35-4efd-a75f-6418912f550f",
       "name": "Upload single file artifact",
       "number": 3,
-      "ref": "v4",
-      "started_at": "2026-07-10T02:01:52.7454718Z",
+      "started_at": "2026-07-10T01:53:04.128Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
-      "action_name": "actions/upload-artifact",
+      "action_name": "actions/upload-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-07-10T02:01:54.5574918Z",
+      "completed_at": "2026-07-10T01:53:04.128Z",
       "conclusion": "succeeded",
-      "external_id": "13ef89e1-b6e2-4412-bca4-0fb0b42c3cd9",
+      "external_id": "fb55ae91-c197-48ff-9f9e-de709fc61fc2",
       "name": "Upload multi-file artifact",
       "number": 4,
-      "ref": "v4",
-      "started_at": "2026-07-10T02:01:53.7719021Z",
+      "started_at": "2026-07-10T01:53:04.128Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
-      "action_name": "actions/upload-artifact",
+      "action_name": "actions/upload-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-07-10T02:01:55.5022827Z",
+      "completed_at": "2026-07-10T01:53:04.128Z",
       "conclusion": "succeeded",
-      "external_id": "8c9298f4-7674-40cd-bbec-70f4d57aa452",
+      "external_id": "fb619091-0a9d-4266-b6a1-8780c51db3c3",
       "name": "Upload binary artifact",
       "number": 5,
-      "ref": "v4",
-      "started_at": "2026-07-10T02:01:54.5578145Z",
+      "started_at": "2026-07-10T01:53:04.128Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
-      "action_name": "actions/upload-artifact",
+      "action_name": "actions/upload-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-07-10T02:01:56.4274261Z",
+      "completed_at": "2026-07-10T01:53:04.128Z",
       "conclusion": "succeeded",
-      "external_id": "37c177dd-976d-4883-afb6-5236fbb6e198",
+      "external_id": "cf2c7202-7cf6-4167-aa2e-6c513f658e46",
       "name": "Upload nested artifact",
       "number": 6,
-      "ref": "v4",
-      "started_at": "2026-07-10T02:01:55.5028205Z",
+      "started_at": "2026-07-10T01:53:04.128Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
-      "action_name": "actions/upload-artifact",
+      "action_name": "actions/upload-artifact@v4",
       "annotations": [],
-      "completed_at": "2026-07-10T02:01:57.4318092Z",
+      "completed_at": "2026-07-10T01:53:04.128Z",
       "conclusion": "succeeded",
-      "external_id": "5b210398-90c2-4893-9d4b-c05d2fe9a803",
+      "external_id": "79a1794f-0142-46e8-bed7-3849520469dc",
       "name": "Upload checksums",
       "number": 7,
-      "ref": "v4",
-      "started_at": "2026-07-10T02:01:56.4289673Z",
+      "started_at": "2026-07-10T01:53:04.128Z",
       "status": "completed",
-      "type": "node24"
+      "type": "action"
     },
     {
       "action_name": "complete_job",
-      "annotations": [
-        {
-          "endLine": 2,
-          "level": "warning",
-          "message": "Node.js 20 is deprecated. The following actions target Node.js 20 but are being forced to run on Node.js 24: actions/upload-artifact@v4. For more information see: https://github.blog/changelog/2025-09-19-deprecation-of-node-20-on-github-actions-runners/",
-          "startLine": 2,
-          "stepNumber": 8
-        }
-      ],
-      "completed_at": "2026-07-10T02:01:57.4453835Z",
+      "annotations": [],
+      "completed_at": "2026-07-10T01:53:04.128Z",
       "conclusion": "succeeded",
-      "external_id": "a732a593-7ccd-481e-aa49-990abd544c53",
+      "external_id": "43bb8978-e1f8-47e1-91ce-7fa58b9842fa",
       "name": "Complete job",
       "number": 8,
-      "started_at": "2026-07-10T02:01:57.4395275Z",
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

**Status codes:** official: [204, 204, 204] | aksh: [204, 204, 204]

**Timing (ms):** p50: official 63.7 / aksh 43.4 | p95: official 122.8 / aksh 44.9

### `POST /{n}/renewjob`

**Header key differences:**

- official only: `{'x-actions-session'}`

**Request body diff:**

```diff
--- official
+++ aksh
@@ -1,4 +1,4 @@
 {
-  "jobId": "9e3559d2-25e8-56e5-b20e-69ce905f9e65",
-  "planId": "f4181416-0894-40ac-a92b-39fa46faed2a"
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
-  "lockedUntil": "2026-07-10T02:11:51.8196761Z"
+  "lockedUntil": "2026-07-10T02:02:57.330446946Z"
 }
```

**Status codes:** official: [200, 200, 200] | aksh: [200, 200, 200]

**Timing (ms):** p50: official 44.9 / aksh 55.9 | p95: official 45.0 / aksh 60.8
