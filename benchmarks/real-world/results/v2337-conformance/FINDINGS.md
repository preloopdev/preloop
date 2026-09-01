# v2.337.0 Conformance Campaign & 5-Repo Campaign v2 — Consolidated Findings

## Preloop Server Fixes (Landed & Verified)
1. **Strategy Dispatch Inputs**: `jobs.<id>.strategy.*` expressions now receive `inputs` and `github.event.inputs` contexts at expansion time.
2. **Environment Variables TemplateToken Wire Shape**: Serialized `environmentVariables` as `{type: 2, map: [{Key: {type: 0, lit: k}, Value: {type: 0, lit: v}}]}` mapping objects. Resolves official runner `The template is not valid. Unexpected value ''` panics.
3. **Orchestration ID Token Sanitization**: Mapped `/` and illegal characters to `-` in `system.orchestrationId`. Eliminates `FormatException` on reusable workflow job execution.
4. **Agent Lookup Client ID GUID**: Populated valid GUID string for `authorization.clientId` in runner registration lookups.
5. **Artifact Scoping by Run ID**: Canonical run ID is resolved from recorded job requests across Twirp handlers (`CreateArtifact`, `ListArtifacts`, etc.), ensuring artifacts are discoverable across distinct jobs within a run.

## Preloop Runner Fixes
1. **Working Directory Pre-creation**: Step runner automatically ensures target directories exist before spawning scripts.

## 5-Repo Conformance Campaign v2 Summary
- **Targets Evaluated**: `cli/cli` (`lint.yml`), `serde-rs/serde` (`ci.yml`), `tokio-rs/tokio` (`ci.yml`), `valkey-io/valkey` (`ci.yml`), `pydantic/pydantic` (`ci.yml`).
- **Results**:
  - `cli/cli`, `serde-rs/serde`, `tokio-rs/tokio` pass cleanly across both official v2.337 and preloop runners against the local preloop server, matching GitHub baseline.
  - `valkey-io` and `pydantic` show identical environment-related behavior on both official and preloop runners (PEP 668 / integration test requirements).
