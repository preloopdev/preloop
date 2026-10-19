# Debug sessions: design notes

This note records the shipped debug-session contract for contributors. The user-facing workflow is [Debugging failed jobs](../control-plane/guides/debugging.mdx); this document explains why the current boundaries exist and which state each component owns.

## One session, several controllers

A failed job can open one session in the same microVM. The worker owns the pause and waits for a verdict. The CLI, an HTTP agent, and a DAP client are controllers over that session; none of them owns a second state machine.

The session moves through `paused`, `attached`, and `retrying`, then ends as `resumed`, `aborted`, or `abandoned`. A single controller lease gates mutations. Observers can read the session, but only the lease holder can issue a retry, retry-from, continue, or abort operation. Every mutation carries an expected session version and a caller-generated request ID. A duplicate request ID returns the recorded response instead of executing the operation again.

The native controller surface is:

- `GET /api/v1/debug/sessions` and `GET .../sessions/:id` for discovery;
- `POST` and `DELETE .../sessions/:id/lease` for controller ownership;
- `GET .../sessions/:id/events?after=N&wait=S` for resumable structured events;
- `POST .../sessions/:id/operations` for typed mutations;
- `GET .../sessions/:id/audit` for the bounded audit trail.

Worker session routes use a job-scoped debug-worker credential. Controller routes use native operator authentication. Worker and controller credentials are intentionally different: a runner listen token identifies a machine, while a debug-worker token identifies one job request.

## Retry contract

Retry is in-place execution in the same guest. It does not rewind external effects. An API call already sent, a process started outside the workspace, or a service mutation remains after retry. The worker records each attempt and restores runner-managed logical state before the next attempt: step outcome, command-file contexts such as `$GITHUB_ENV`, `$GITHUB_PATH`, and `$GITHUB_OUTPUT`, expression contexts, and action state.

`retry_from` starts at an earlier resolved step. The target must be at or before the failed step. Preloop cannot infer producer/consumer edges in arbitrary shell commands, so the controller chooses how far back to run. A step has a bounded retry budget of 25 attempts.

The session records a per-attempt source revision (`original`, `repair-1`, and so on). A successful retry has the ordinary successful job conclusion. It is evidence from a warm, mutated guest, not an independent clean-run verification; a fresh submission supplies that verification.

## Workspace changes

The worker compares the workspace with the immutable checkout snapshot taken for the attempt. It classifies changes as:

| Category | Retry handling |
|---|---|
| Tracked files | Restored from the snapshot when the controller selects the `all` policy. |
| Untracked files | Removed with the `untracked` or `all` policy. |
| Ignored caches such as `target/` | Preserved. |
| Paths outside the workspace | Not reverted; the controller receives no false promise of rollback. |

The interactive CLI asks when tracked files changed because generated source and a hand-edited file are indistinguishable from the snapshot alone. The non-interactive API defaults to `none`; callers choose `untracked` or `all` explicitly.

Host-to-guest sync transfers changed files as one tar archive plus a deletion list. Gitignored output is excluded, so the transfer does not walk or replace build caches. Mode bits travel with the archive. A guest edit and host edit to the same path are a conflict; sync refuses it unless the controller requests `force`.

Guest-to-host export writes a patch and applies it by default. `--patch-only` leaves the patch for review without applying it. This is the persistence boundary for source edits: the VM remains disposable, while the host checkout receives a reviewable change.

## Lifetime and timeout boundaries

The worker must continue polling while a session is open. The server's worker-liveness window is 90 seconds. Paused duration is credited separately from ordinary job timeout accounting, with a four-hour maximum pause credit per job request. Once that credit is exhausted, normal timeout handling applies.

A preserved post-mortem VM has a ten-minute idle hold after a shell or debug attachment releases it. A controller heartbeat marks an active attachment; a state-preserving backend can suspend an unattached paused guest and resume it when a controller returns. A backend that cannot preserve the live guest keeps the machine in place instead. The host-side marker remains usable even when the guest is suspended.

## Why the implementation uses snapshots and whole-file sync

A mtime walk before and after every step costs repository-scale work on every run and misses writes that preserve timestamps. The checkout snapshot already provides content identity and an undo source, so change detection runs when a retry is requested rather than on every successful step.

A source diff against the server snapshot is not the local transfer primitive: the snapshot object graph lives in the control plane's snapshot store, not necessarily in the host repository. Fetching that graph can cost more than copying the changed files. A single archive minimizes guest round trips and keeps transfer cost proportional to the changed files.

The VM is never copied wholesale into a later run. Such a copy would include credentials, shell history, test databases, source edits, and unrelated mutations. A repaired retry keeps the current VM intentionally; clean verification starts from a new run's normal image and setup.

## Credential and path containment

Worker routes authenticate the job named by the token and return `404` for a session that belongs to another job, avoiding session-ID probing. The worker obtains its debug credential during job setup through the job runtime credential; the credential is not exposed as a workflow secret. Issuance is one-shot for a live job that requested failure preservation.

Controller output uses masked logs and structured diagnostics rather than an unrestricted environment dump. Guest shell paths are validated as absolute paths without shell-active characters before interpolation. Revert paths reject absolute escapes and `..`, then canonicalize the target parent so a symlink cannot redirect removal outside the workspace. The leaf itself is removed as a link rather than resolved.

The resulting boundary is deliberate: a controller can inspect and retry the selected job, but logs, repository files, and dependency diagnostics do not grant additional authority. The session audit records controller identity, operation, version, and result within bounded storage.
