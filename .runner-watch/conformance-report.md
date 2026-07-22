# runner-watch conformance report

❌ 1 of 1 scenario(s) diverged.

- [06-multi-step](.runner-watch/conformance/v2.335.1/06-multi-step.md)

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

### Unsupported protocol surfaces

Cache v4 and artifact v4 endpoints are intentionally **not mocked**.
If a golden capture exercises one of these endpoints before aksh has a real
implementation, replay must report a status mismatch instead of pretending
the scenario works.

| Endpoint family | Current truth | Expected replay signal |
|---|---|---|
| `CacheService/*` | Not implemented | 404/status mismatch until backed by the cache store |
| `ArtifactService/*` | Not implemented | 404/status mismatch until backed by the artifact store |

Blob uploads/downloads to `*.blob.core.windows.net` remain skipped because
they are external storage traffic, not aksh HTTP endpoints. Skipping those
flows does not waive the aksh Twirp control-plane endpoints above.

#### Roadmap: Removing Exclusions & Verifying Side Effects

Once local equivalents for storage (blob), cache, and OIDC are implemented
in their respective crates, we will remove them from these skip lists.
Because captured Azure SAS signatures expire and direct external connections
cannot authenticate during static playbacks, the replayer must be updated
to rewrite external hosts (e.g. `*.blob.core.windows.net`) to the local `aksh`
server's endpoints, allowing verification of the local storage implementation.

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