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

The guest agent is runtime-agnostic: the same static binary runs inside smolvm
(Local and smolvm-KVM tiers) and Firecracker (scale tier). Locally it rides
smolvm's vsock exec/shell/cp plane; nothing in it is libkrun-specific.

## smolvm I/O paths and the storage-disk rule

smolvm exposes two filesystems to the job (see
[doc 03](03_microvm_isolation_and_smolvm_runtime.md)):

- **ext4 storage disk (fast):** virtio-blk raw image; metadata ops stay in-guest;
  2.5–3.8× faster small-file I/O than macOS APFS. `/workspace` and the persistent
  overlay live here.
- **virtiofs bind mount (slow):** `-v host:/guest`; every file op round-trips to
  the host filesystem.

Rule: keep CI's hot I/O — deps, build output, test scratch — on the storage disk.
Use a bind mount only for the explicit live-edit mode below.

## Getting the working tree in (including uncommitted changes)

Local preflight must run the *working tree*, not just committed code: tracked
edits **plus** untracked-not-ignored files **plus** deletions. Transport options:

| Method | Sync cost | CI I/O speed | Edit visibility | Correctness notes |
|---|---|---|---|---|
| `tar` + `machine cp` full tree | one-time ~0.3–0.5s | fast (ext4) | stale after copy | blind re-tar misses deletions |
| **delta over warm snapshot** (default) | minimal (dirty files only) | fast (ext4) | re-sync per edit | must also send deletions |
| incremental sync on resume | ~0.05–0.1s (changed only) | fast (ext4) | re-sync per edit | mtime/content-hash diff + `--delete` |
| `-v ./:/workspace` bind mount | zero | slow (virtiofs) | instant/live | low fidelity, throughput tax |

Recommended default is **delta over warm snapshot**:

1. The warm `.smolmachine` already holds the repo at the last-committed SHA plus
   deps and build cache on ext4.
2. Compute the working-tree delta = `git diff` (tracked) ∪ untracked-not-ignored
   files ∪ deletions.
3. `machine cp` only that delta onto `/workspace` and apply deletions in-guest.
   Everything stays on ext4; transfer is proportional to what actually changed.

Rules:

- Honor `.gitignore` + a `.preloopignore`; never copy `.git/`, `node_modules/`,
  `target/`, `.venv/` — those come from the snapshot/cache, not the wire.
- Handle deletions explicitly (rsync `--delete` semantics). A tar-overwrite alone
  leaves removed files behind and produces false greens.
- Respect the `machine cp` 4 GiB per-transfer cap and ~35–42 MB/s upload; delta
  sync keeps you far under both.
- The `-v` live bind-mount mode is opt-in for interactive debugging; mark the run
  low-fidelity/low-throughput and record it in run metadata.
- For agent loops, pass changed paths so the delta is exact
  (`preloop retry --step failed --changed src/foo.rs`).

## Workspace topology

Local fast mode (default — source on the ext4 storage disk, no virtiofs):

```text
/workspace           ext4 working tree (source synced in via machine cp)
  + overlay          persistent overlay on the storage disk (writes)
/preloop/cache       controlled cache mounts (storage disk)
/preloop/artifacts   artifact output
/preloop/logs        log output
/preloop/tmp         bounded scratch
```

Live edit-visibility mode (opt-in, slow — virtiofs):

```text
/host_src (ro)       -v ./:/host_src:ro   host edits visible live
/workspace           overlay: lower=/host_src, upper=ext4 (writes stay fast)
```

Strict/self-hosted/managed mode:

```text
/workspace           source synced at the exact commit SHA onto ext4 (no live host mount)
  + overlay          overlay on the storage disk
/preloop/cache       trust-tier-scoped cache mounts
/preloop/artifacts   output sink
```

The key difference is that strict mode syncs a pinned commit, never the live
working tree.

## Upper-layer modes

| Mode | Use | Risk |
|---|---|---|
| `tmpfs` | small tests, fastest local retry | RAM/inode exhaustion |
| `sparse-disk` | monorepos, package installs, large builds | slower than tmpfs |
| `hybrid` | default: hot small writes in tmpfs, large dirs on disk | implementation complexity |

Default to `hybrid`, not tmpfs-only.

On smolvm these tiers map onto real disks: `tmpfs` = guest RAM, while
`sparse-disk`/`hybrid` = the ext4 storage disk + overlay disk smolvm already
provisions (`storage`/`overlay` sizes in the Smolfile). Preloop chooses the upper
policy; smolvm provides the disks.

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

The delta/incremental sync above (with `--delete` semantics) is the mechanism
behind changed-path invalidation; a clean snapshot rebuild
(`machine create --from warm.smolmachine`) is the safe fallback.

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
3. Host syncs the working-tree delta onto the storage-disk `/workspace` via `machine cp` (or, in edit-visibility mode, attaches the `-v` source mount).
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
