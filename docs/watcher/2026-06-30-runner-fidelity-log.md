# Runner fidelity work log — 2026-06-30

## Scope

Addressed the fidelity gaps called out from `docs/fidelity-gap.md` for:

- legacy AzDO runner session handshake RSA wrapping;
- current-runner broker job flow coverage for `AgentJobRequestMessage`;
- `connectionData` service-location richness for current runner paths;
- GitHub runner-registration and OAuth token response shape.

## Changes

### RSA-wrapped session keys

- Updated `create_session` in `crates/aksh-runner-server/src/lib.rs` to read the registered runner RSA public key from `InnerState::runner_rsa_public_keys` by `runner_id`.
- When a key is present, the generated AES-256 session key is RSA-OAEP wrapped and returned with `encryptionKey.encrypted = true`.
- Kept plaintext AES key fallback only for runners that registered without public-key material.
- Added extraction of nested TaskAgent `authorization.publicKey.{modulus,exponent}` registration payloads so current DistributedTask registration stores the runner public key.

### Service discovery and auth shape

- Expanded `connectionData` with deployment metadata, access mappings, resource-area definitions, and current-runner service locations for broker listener, created session, runner messages/config refresh, OAuth token, and pipelines job/log resources.
- Made `connectionData` query-aware: stale/location requests return the expanded service map; fresh-cache requests with `connectOptions=0` and a non-negative `lastChangeId` return a minimal `clientCacheFresh` response.
- Changed GitHub runner-registration response to return a local JWT-shaped `OAuthAccessToken` and an aksh service URL instead of echoing the GitHub repository URL.
- Changed OAuth token response to match runner-compatible shape: `token_type = JWT`, `expires_in = 2999`, and an HMAC-signed local JWT-shaped token.
- Updated DistributedTask agent registration response to preserve the runner public key, expose `UseV2Flow = true`, and point `ServerUrlV2` at the aksh root that serves session/message routes.

### Current-runner E2E coverage

- Added `current_runner_registration_to_broker_job_e2e`, covering:
  1. `/api/v3/actions/runner-registration`;
  2. `connectionData` service discovery;
  3. DistributedTask agent registration with nested RSA public key;
  4. OAuth token issue;
  5. DistributedTask session creation;
  6. workflow submission;
  7. current `/distributedtask/.../messages` broker-ref poll;
  8. `/broker/{runner}/acquirejob` returning full `AgentJobRequestMessage`;
  9. `/broker/{runner}/completejob` returning `204`.

## Focused verification during implementation

```sh
cargo test -p aksh-runner-server session_key
cargo test -p aksh-runner-server task_agent_registration_extracts_nested_public_key
cargo test -p aksh-runner-server connection_data_exposes_current_runner_service_locations
cargo test -p aksh-runner-server registration_and_oauth_return_runner_compatible_tokens
cargo test -p aksh-runner-server current_runner_registration_to_broker_job_e2e
```

All focused tests above passed before this log was written.

## Final verification

```sh
cargo test -p aksh-runner-server
cargo test -p aksh-gha-protocol crypto
cargo test -p runner-watch
cargo run -p runner-watch -- conform --runner v2.335.1 --aksh-url http://127.0.0.1:19090 --scenario 01-register-and-idle --skip-cargo-test
```

Observed results:

- `cargo test -p aksh-runner-server`: 30 passed.
- `cargo test -p aksh-gha-protocol crypto`: 8 passed.
- `cargo test -p runner-watch`: 7 passed.
- runner-watch conformance: `.runner-watch/conformance-report.md` reports `All 1 scenario(s) matched recorded baseline responses`; the refreshed `01-register-and-idle` report compares 52 official flows to 52 aksh responses with no missing endpoints and matching status sets.

## Follow-up boundary

This work improves runner-observed control-plane fidelity without attempting byte-for-byte GitHub-hosted parity. Cache v2/blob-Twirp, full Azure signed blob URL body parity, DAP, server-enforced runner settings, and Node migration warnings remain separate `docs/fidelity-gap.md` items.
