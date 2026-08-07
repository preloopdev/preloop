## What & why

<!-- What this change does and the problem it solves. Link the issue if there is one. -->

## Protocol surface

<!--
Check whether this change touches the core runner protocol interface:
  - registration / `/_apis/...` request and response shapes
  - broker messages (job request, complete, renew) and NDJSON event shapes
  - Twirp results-service / artifact-service payloads
  - check-run or OAuth wire behavior

If it does, the gates below are REQUIRED before merge — preloop's compatibility
contract is byte-for-byte fidelity with the official runner (v2.336.0).
-->

- [ ] I confirm this change touches the runner protocol interface: **YES / NO**

## Required gates

- [ ] `just test-ci` passes locally (fmt-check + clippy `-D` + full test suite + runner-watch conformance)
- [ ] If protocol/wire shapes changed: the change is validated against the official runner (golden replay via `runner-watch`, or a live capture showing the official bytes), not only unit tests
- [ ] If the touched subsystem has property tests (`concurrency_properties`, scheduling, matrix expansion, …): they are extended for the changed contract and pass (`PROPTEST_CASES=256 cargo test -p preloop-runner-server`)
- [ ] New wire fields/events are additive and serde-defaulted where the official runner would not send them
- [ ] No secrets, credentials, or internal/deployment-specific paths are introduced (captures with live tokens must be redacted or excluded)

## Verification performed

<!-- Concrete evidence: commands run, test names, capture outputs. -->

## Checklist

- [ ] Tests added/updated for any new observable contract
- [ ] Docs updated (`docs/`, `CONTRIBUTING.md`) where behavior changed
- [ ] Changelog-worthy user-facing change described in the PR body
