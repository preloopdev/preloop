# Plan 005: Close results-Twirp gaps (missing diagnostic-logs route + auth verification)

> **Executor instructions**: Follow step by step; run every verification; STOP conditions are
> binding. Update this plan's row in `plans/README.md` when done.
>
> **Drift check (run first)**:
> `git diff --stat 839791c..HEAD -- crates/aksh-runner-server/src/lib.rs crates/aksh-runner/src/client/results.rs`
> `lib.rs` had large uncommitted changes at planning time. Anchor on symbol names
> (`twirp_get_step_logs_signed_blob_url`, `require_bearer`, route registrations starting at
> `/twirp/results.services.receiver.Receiver/`). Mismatch in structure = STOP.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: bug + security-investigate
- **Planned at**: commit `839791c`, 2026-07-13

## Why this matters

Two independent issues:

**1. Missing diagnostic-logs route (confirmed bug).** The aksh runner unconditionally calls
`Receiver/CreateResultsDiagnosticLogsSignedBlobURL` after every job
(`crates/aksh-runner/src/client/results.rs:113-116`). The server's `/twirp/` route table
(`lib.rs:708-738`) does not register this path, so every job ends with a 404 on diagnostic
upload. The runner logs a warning and continues, but diagnostic data (step traces, environment
dumps) is never persisted — hurts debugging.

**2. Auth model on Twirp routes (must verify before remote deployment).** The server's
comment (`lib.rs:709-710`) says "the runner's job token (which uses a different signing key)
is accepted" — but the Twirp handlers were moved *outside* `require_bearer` without per-handler
JWT validation replacing it. The audit could not confirm that each handler validates the
`Authorization: Bearer <job-JWT>` header itself. This must be verified (and fixed if absent)
before the server is exposed to untrusted networks. With no auth, any client that can reach
the server can upload arbitrary log blobs or read signed URLs.

## Current state

- `crates/aksh-runner/src/client/results.rs:113-116` — the runner calls the missing route:

  ```rust
  let url = format!(
      "{}/twirp/results.services.receiver.Receiver/CreateResultsDiagnosticLogsSignedBlobURL",
      self.base_url
  );
  ```

- `crates/aksh-runner-server/src/lib.rs:708-738` — the registered Twirp routes. Registered:
  `WorkflowStepsUpdate`, `GetJobLogsSignedBlobURL`, `GetStepLogsSignedBlobURL`,
  `GetStepSummarySignedBlobURL`, `CreateStepSummaryMetadata`, `CreateStepLogsMetadata`,
  `CreateJobLogsMetadata`. **Not registered**: `CreateResultsDiagnosticLogsSignedBlobURL`.
- Existing signed-URL handler to copy: `twirp_get_step_logs_signed_blob_url` — find its
  implementation in `lib.rs` (search `fn twirp_get_step_logs_signed_blob_url`) for the
  exact pattern (reads workflow/job backend IDs, generates a blob PUT URL, returns JSON).
- `require_bearer` (`lib.rs:791-794`): routes starting with `/broker/` bypass it; the Twirp
  routes sit below that middleware entirely (added to the router after the `route_layer`).
- Official reference for the route: `crates/aksh-runner/src/client/results.rs` is the
  authoritative client; the server must implement what the client calls.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Server tests | `cargo test -p aksh-runner-server --quiet` | all pass |
| Full gate | `cargo fmt --all --check && cargo clippy --workspace --all-targets && cargo test --workspace --quiet` | exit 0 |

## Scope

**In scope**:
- `crates/aksh-runner-server/src/lib.rs` (new route + handler; auth verification/fix)

**Out of scope** (do NOT touch):
- `crates/aksh-runner/src/client/results.rs` — the runner's call is correct; the server must
  match it.
- Cache/artifact Twirp handlers — separate section of the router; audited clean.
- `require_bearer` itself — do not change its bypass logic.

## Git workflow

- Branch: `advisor/005-results-twirp-gaps`; conventional commits; no push/PR unless told.

## Steps

### Step 1: Verify Twirp handler auth

For **each** handler function registered in the `/twirp/results.services.receiver.Receiver/`
block, read its body and answer: does it extract and validate the `Authorization: Bearer`
header against the job JWT (e.g. checking `scp` claim, plan_id, job_id)?

If YES for all: note "auth verified" in the commit message, skip to Step 2.

If NO (handler ignores auth): add a shared extractor. The pattern to follow is the existing
`require_bearer` function — adapt it into a function
`fn require_job_bearer(headers: &HeaderMap, expected_plan_id: &str, expected_job_id: &str) -> Result<(), ApiError>`
that at minimum checks the token is present and non-empty (a full JWT signature check requires
the signing key; if that key is not accessible to the handler, log a warning and document it
as a known gap rather than accepting any token — do not silently pass through unauthenticated
requests). Call it at the top of each affected handler.

**Verify**: `cargo check -p aksh-runner-server` exits 0.

### Step 2: Add the missing diagnostic-logs route

1. Register the route in the Twirp block (directly after the existing `CreateJobLogsMetadata`
   route):

   ```rust
   .route(
       "/twirp/results.services.receiver.Receiver/CreateResultsDiagnosticLogsSignedBlobURL",
       post(twirp_create_diagnostic_logs_signed_blob_url),
   )
   ```

2. Implement the handler. Copy the structure of the nearest signed-URL handler
   (`twirp_get_step_logs_signed_blob_url` or `twirp_get_job_logs_signed_blob_url`). The runner
   sends a JSON body with at minimum `workflow_run_backend_id` and `workflow_job_run_backend_id`
   (same pattern as all other receiver calls); the response must be a JSON object with a
   `signed_url` field pointing to a blob PUT URL the runner can write to. Reuse the existing
   blob-store path pattern (`/twirp-blob/diag/<token>` or similar) so the blob-PUT handler
   already registered for other log types covers it.

   If the blob-PUT router uses a path prefix, check whether it already covers a `/diag/`
   sub-path; if not, add a minimal PUT route for it alongside the new POST route.

**Verify**: `cargo check -p aksh-runner-server` exits 0.

### Step 3: Test

In the server's inline tests (model after the existing `current_service_broker_flow_uses_queued_job`
or `protected_apis_require_bearer_token` test style):

- `diagnostic_logs_route_returns_signed_url`: POST to the new route with a minimal JSON body,
  assert 200 and a non-empty `signed_url` field.
- If Step 1 added auth checking: `diagnostic_logs_route_rejects_missing_auth` — POST without
  `Authorization` → 401.

**Verify**: `cargo test -p aksh-runner-server --quiet` → all pass including new tests.

## Test plan

Step 3 above covers both sub-findings. No changes to the runner-side test suite needed.

## Done criteria

- [ ] Full gate exits 0
- [ ] `grep -n "CreateResultsDiagnosticLogs" crates/aksh-runner-server/src/lib.rs` → at least
  2 matches (route registration + handler)
- [ ] Auth posture documented in commit message (either "handlers validate JWT" or "gap
  documented, bearer presence checked only")
- [ ] No files outside scope modified (`git status`)
- [ ] `plans/README.md` row updated

## STOP conditions

- The Twirp route block has been reorganized and the missing route was already added in the
  uncommitted concurrency work — verify and mark DONE without duplicating.
- The blob-PUT handler path pattern is not clear from reading `lib.rs` — report the actual
  structure before adding a new sub-path.
- Full JWT signature verification requires a signing key not available to the handler — do NOT
  accept all tokens silently; document the gap explicitly and implement bearer-presence check
  only.

## Maintenance notes

- Reviewer: confirm the new signed-URL blob token is not guessable (use `uuid::Uuid::new_v4()`
  as the existing handlers do).
- If the production profile adds object storage, the blob-PUT handlers will be replaced; the
  signed-URL shape (the URL the runner receives) stays the same.
- Deferred: full JWT signature validation for Twirp routes (requires key management design
  decision beyond this plan's scope).
