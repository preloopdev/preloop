# runner-watch conformance report

✅ All 24 scenario(s) matched recorded baseline responses (see replay caveats below).

- [01-register-and-idle](.runner-watch/conformance/v2.336.0/01-register-and-idle.md)
- [02-trivial-job](.runner-watch/conformance/v2.336.0/02-trivial-job.md)
- [03-cancellation](.runner-watch/conformance/v2.336.0/03-cancellation.md)
- [04-request-ack](.runner-watch/conformance/v2.336.0/04-request-ack.md)
- [05-multi-job](.runner-watch/conformance/v2.336.0/05-multi-job.md)
- [06-multi-step](.runner-watch/conformance/v2.336.0/06-multi-step.md)
- [07-step-failure](.runner-watch/conformance/v2.336.0/07-step-failure.md)
- [08-job-outputs-needs](.runner-watch/conformance/v2.336.0/08-job-outputs-needs.md)
- [09-matrix-fan-out](.runner-watch/conformance/v2.336.0/09-matrix-fan-out.md)
- [10-uses-checkout](.runner-watch/conformance/v2.336.0/10-uses-checkout.md)
- [11-cache-roundtrip](.runner-watch/conformance/v2.336.0/11-cache-roundtrip.md)
- [12-artifact](.runner-watch/conformance/v2.336.0/12-artifact.md)
- [13-composite-action](.runner-watch/conformance/v2.336.0/13-composite-action.md)
- [14-annotations](.runner-watch/conformance/v2.336.0/14-annotations.md)
- [15-oidc-id-token](.runner-watch/conformance/v2.336.0/15-oidc-id-token.md)
- [16-container-job](.runner-watch/conformance/v2.336.0/16-container-job.md)
- [17-service-container](.runner-watch/conformance/v2.336.0/17-service-container.md)
- [30-container-job-basic](.runner-watch/conformance/v2.336.0/30-container-job-basic.md)
- [31-container-with-services](.runner-watch/conformance/v2.336.0/31-container-with-services.md)
- [32-services-no-container](.runner-watch/conformance/v2.336.0/32-services-no-container.md)
- [33-container-env-options](.runner-watch/conformance/v2.336.0/33-container-env-options.md)
- [34-container-with-checkout](.runner-watch/conformance/v2.336.0/34-container-with-checkout.md)
- [35-container-lifecycle](.runner-watch/conformance/v2.336.0/35-container-lifecycle.md)
- [36-docker-action](.runner-watch/conformance/v2.336.0/36-docker-action.md)

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

### Cache and artifact replay

Cache v4 and artifact v4 control-plane endpoints are implemented and
replayed against the local cache and artifact stores. Their Twirp status
codes remain part of the gate; a missing or broken endpoint is reported as a
conformance failure rather than being suppressed.

Captured Azure blob URLs cannot be reused after their SAS signatures expire.
For cache/artifact PUTs, the replayer consumes the signed URL returned by the
preceding local Twirp create call and uploads the captured bytes there.

Additionally, the conformance pipeline will be expanded to verify stateful
side effects directly rather than relying solely on HTTP responses:
- **Cache validation**: Verify that actual cache archives are written to disk
  and are retrievable during subsequent restore calls.
- **OIDC token verification**: Validate that generated tokens carry the requested
  audience, correct claims, and valid signatures that the server accepts.

### How Wire Compliance is Checked

The conformance checker compares the local `aksh` server against the official
recorded golden baseline. For each non-skipped flow, it compares:

1. **HTTP Status Codes**: Verifies status codes match exactly (e.g. `200` vs `200`, `204` vs `204`). Any mismatch fails the scenario.
2. **Request & Response Bodies**: Compares JSON structure and values using unified diffs. Volatile segments (like session IDs, timestamps, and authentication tokens) are redacted or normalized beforehand.
3. **Header Keys**: Checks for differences in HTTP header names (e.g., verifying that expected content types or authentication headers are present).