# runner-watch conformance report

✅ All 10 scenario(s) matched recorded baseline responses (see replay caveats below).

- [06-multi-step](.runner-watch/conformance/v2.335.1/06-multi-step.md)
- [07-step-failure](.runner-watch/conformance/v2.335.1/07-step-failure.md)
- [08-job-outputs-needs](.runner-watch/conformance/v2.335.1/08-job-outputs-needs.md)
- [09-matrix-fan-out](.runner-watch/conformance/v2.335.1/09-matrix-fan-out.md)
- [10-uses-checkout](.runner-watch/conformance/v2.335.1/10-uses-checkout.md)
- [11-cache-roundtrip](.runner-watch/conformance/v2.335.1/11-cache-roundtrip.md)
- [12-artifact](.runner-watch/conformance/v2.335.1/12-artifact.md)
- [13-composite-action](.runner-watch/conformance/v2.335.1/13-composite-action.md)
- [14-annotations](.runner-watch/conformance/v2.335.1/14-annotations.md)
- [15-oidc-id-token](.runner-watch/conformance/v2.335.1/15-oidc-id-token.md)

## Replay methodology and known gaps

The conformance gate replays official golden flows through aksh and compares
HTTP status codes. Several categories of flow are intentionally excluded or
treated leniently; a ✅ gate result does **not** mean full protocol parity.

### Flows skipped from replay

Two skip layers are applied before any request is sent to aksh:

**Host/path skip list** (`should_skip_replay_path`) — flows to these
destinations are dropped entirely; aksh is never involved:

| Host / path | Why skipped |
|---|---|
| `*.blob.core.windows.net` | Azure Blob Storage — artifact/cache byte uploads and downloads |
| `objects.githubusercontent.com` | GitHub object storage |
| `token.actions.githubusercontent.com` | GitHub OIDC issuer (external) |
| `codeload.github.com` | GitHub source tarballs for action downloads |
| `launch.actions.githubusercontent.com` | GitHub batch action-resolution service |
| path `/health` or `/ready` | Health/readiness probes with no protocol content |

**No-status skip** (`should_skip_replay_flow`) — any captured flow whose
`status` field is null (i.e. the runner was killed mid-request and no
response was ever recorded) is also dropped. These are capture artifacts,
not protocol evidence.

### Status lines excluded from the gate

Even for flows that _are_ replayed, two endpoint families are excluded from
the status-mismatch check (`status_mismatch_in_report`):

| Endpoint pattern | Why excluded |
|---|---|
| `…/oauth2/token` | Official validates PSA256 client assertions and rejects job-scoped credentials; aksh is its own CA and accepts all. Unverifiable in replay. |
| `…/messages?…` | Broker proactively invalidates sessions via concurrent two-session pattern; timing-based and not reproducible from a static golden. |

### Mocked implementations

The following endpoints return **shape-correct 200 responses but are not
real implementations**. The gate passes because status codes match; body
content and actual data behaviour are not checked.

| Endpoint | What the mock returns | What is missing |
|---|---|---|
| `CacheService/GetCacheEntryDownloadURL` | `ok:true, signed_download_url:""` — always a cache **miss** | No real cache store; runner skips restore |
| `CacheService/CreateCacheEntry` | `ok:true, signed_upload_url:<fake-aksh-url>` | Upload URL points at a non-existent aksh route; the runner's PUT would 404 |
| `CacheService/FinalizeCacheEntryUpload` | `ok:true` | No entry is stored |
| `ArtifactService/CreateArtifact` | `ok:true, signed_upload_url:<fake-aksh-url>` | Same as above; upload silently fails |
| `ArtifactService/FinalizeArtifact` | `ok:true` | No artifact is stored |
| `ArtifactService/GetSignedArtifactURL` | `signed_url:<fake-aksh-url>` | Download would 404 |
| `ArtifactService/ListArtifacts` | `artifacts:[]` | Always empty |

The blob uploads/downloads that follow these calls go to `*.blob.core.windows.net`
(in official captures) or to non-existent aksh routes (during replay), so
they are never replayed and never appear in the comparison.