# Guest Agent, Workspace, and Filesystem

## Goal

The guest agent and workspace layer should make each CI job look like a clean Linux runner while preserving fast local edit/retry behavior.

The model must support:

- read-only host source mounts for local mode,
- source snapshots for strict/self-hosted/managed mode,
- writable overlays with quotas,
- cache mounts,
- artifact/log egress,
- shell attach,
- step transaction hooks,
- and retry/fork workflows.

## Guest agent responsibilities

Preloop needs its own guest agent even if Aksh runs the job.

The agent should provide:

- boot health check,
- workspace setup,
- overlay mount orchestration,
- cache mount setup,
- process execution with env/cwd/stdout/stderr/exit,
- PTY shell over vsock/console,
- cancellation and process-tree kill,
- Docker/buildkit lifecycle hooks,
- service supervision,
- step transaction pre/post hooks,
- resource usage reporting,
- artifact/log export,
- and cleanup.

The guest agent should be a small static Rust binary where possible.

## Workspace topology

Local fast mode:

```text
/host_ro             read-only virtio-fs mount of repo
/workspace           overlay visible to Aksh/runner
/workspace_upper     tmpfs / sparse disk / hybrid upper
/workspace_work      overlay workdir
/preloop/cache       controlled cache mounts
/preloop/artifacts   artifact output
/preloop/logs        log output
/preloop/tmp         bounded scratch
```

Strict/self-hosted/managed mode:

```text
/source_ro           source snapshot at exact commit SHA
/workspace           overlay visible to job
/workspace_upper     sparse disk or hybrid upper
/preloop/cache       trust-tier-scoped cache mounts
/preloop/artifacts   output sink
```

The key difference is that strict mode should not rely on live host edits.

## Upper-layer modes

| Mode | Use | Risk |
|---|---|---|
| `tmpfs` | small tests, fastest local retry | RAM/inode exhaustion |
| `sparse-disk` | monorepos, package installs, large builds | slower than tmpfs |
| `hybrid` | default: hot small writes in tmpfs, large dirs on disk | implementation complexity |

Default to `hybrid`, not tmpfs-only.

## Quotas

Every writable area needs a quota:

```toml
[filesystem]
upper_limit = "8GiB"
scratch_limit = "32GiB"
log_limit = "2GiB"
artifact_limit = "10GiB"
cache_mount_limit = "50GiB"
inode_limit = "auto"
```

Disk exhaustion is both a reliability problem and a security problem.

## Host edit coherence

The hard problem: overlay copy-up can hide host edits.

Example:

1. `/host_ro/src/foo.rs` exists.
2. Build writes to `/workspace/src/foo.rs`, causing copy-up.
3. Agent edits `src/foo.rs` on host.
4. Retry step still sees the copied-up stale file unless Preloop handles it.

Solutions:

| Strategy | Use |
|---|---|
| Invalidate upper copies for changed paths | default local retry if changed paths are known |
| Recreate overlay | safe fallback |
| Rsync host tree into clean workspace | simpler but slower, good fallback |
| Strict source snapshot | final verification and managed CI |

For agent loops, the event API should let the agent pass changed paths:

```text
preloop retry --step failed --changed src/foo.rs tests/foo_test.rs
```

If changed paths are unknown, rebuild the overlay or run `retry-job`.

## Symlink and path safety

Preflight scanner requirements:

- reject symlinks escaping the workspace root,
- detect hardlinks where relevant,
- reject special files unless allowed,
- normalize paths before mounting,
- prevent `..` traversal in artifact/cache export,
- never mount developer home implicitly,
- never follow repo symlinks into `/Users`, `/home`, `/private`, `/var`, cloud credential paths, or SSH paths.

## macOS filesystem drift

Local macOS commonly uses case-insensitive APFS. Linux CI is case-sensitive.

Detect and report:

- case-insensitive source root,
- filenames differing only by case,
- executable-bit mismatches,
- symlink behavior differences,
- newline/permission edge cases if relevant.

Include this in fidelity metadata:

```json
{
  "host_diffs": ["case-insensitive-apfs"],
  "fidelity_score": 0.91
}
```

## Workspace setup sequence

```text
1. Boot VM.
2. Guest agent announces health.
3. Host attaches source mount or source disk.
4. Guest validates source mount.
5. Guest creates upper/work/scratch dirs.
6. Guest mounts overlay at /workspace.
7. Guest mounts cache/artifact/log dirs.
8. Guest starts Aksh runner.
9. Aksh executes the job.
10. Guest exports logs/artifacts and reports cleanup.
```

## Required tests

- Guest cannot write to source mount.
- Builds do not write into host tree.
- Host edit is visible after retry invalidation.
- Overlay rebuild fixes stale copy-up.
- Quotas produce classified failures.
- Symlink escape is rejected before job start.
- Case-insensitive APFS warning appears on macOS.
- `--strict` uses source snapshot, not live host mount.
- Artifact export cannot escape artifact root.
- Cache mount cannot write outside cache namespace.
