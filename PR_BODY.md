# Fix: stream cache blob writes + total cache quota with LRU eviction

Fixes round-3 report §11 (Cache flood → RAM spike + permanent disk).

## Problem

Two compounding defects in the Actions cache path:

1. **Full-blob-in-RAM finalize.** The Twirp v2 flow (`twirp_cache_v2_finalize`)
   staged upload chunks to disk correctly, then did `tokio::fs::read` of the
   whole staged blob and passed the `Vec<u8>` to `CacheStore::put`, which
   wrote it again. Reproduced in round 3: finalizing a 150 MB blob spiked
   server RSS **69 MB → 341 MB** (+265 MB ≈ 1.8× blob size). The legacy
   `/_apis/artifactcache` flow was worse: `cache_upload` appended every PATCH
   chunk to `PendingCache::bytes`, holding the entire blob in server RAM for
   the reservation's lifetime.
2. **No quota, no eviction.** `CacheStore::put` validated key format only. A
   finalized entry sat in `state/cache/` forever; disk filled for the lifetime
   of the state directory.

## Fix

**Streaming writes (never buffer the full blob in RAM):**
- `preloop-cache`: new `CacheStore::commit_file_scoped(namespace, key,
  version, src)` — claims the entry dir, writes metadata sidecars, then
  *moves* the staged file into place (atomic rename; bounded-buffer
  streaming copy fallback for cross-filesystem staging). The staging file is
  consumed, never read into RAM.
- Twirp v2 finalize now commits the staged `blobs/cache/<jti>/data` file via
  `commit_file_scoped`. Cleanup of the reservation + staging dir is
  unconditional (also runs on commit failure).
- Legacy v1 flow: `cache_reserve` creates `<state_dir>/cache-staging/<id>.part`;
  `cache_upload` appends each chunk to the file (open/append/close under the
  state lock, so the per-upload and per-job byte caps can't be raced);
  `cache_commit` hands the file to `commit_file_scoped`. `PendingCache::bytes:
  Vec<u8>` is gone — replaced by `staging_path` + `staging_len`. Abandoned
  reservations' staging files are deleted by the TTL sweeper
  (`sweep_pending_uploads` now returns the files; `reap_once` deletes them
  after releasing the lock); a crashed server's leftovers are reaped at boot.

**Total quota with LRU eviction:**
- New `[cache] total_quota_bytes`, default **10 GiB** (matches GitHub's 10 GB
  per-repository cache limit; this is the server-wide total across all
  namespaces). Env `PRELOOP_CACHE_TOTAL_QUOTA_BYTES` wins; unparseable
  values fail startup. Documented in `docs/self-hosting.md`.
- Every commit (and server boot) runs `CacheStore::enforce_quota`: entries are
  ordered by last use (archive mtime, refreshed on every read and commit) and
  the oldest are deleted until the total fits. The just-committed entry is
  never evicted by its own commit — a single entry larger than the quota is
  kept, then evicted by the next commit. Stale incomplete writes (crashed
  uploads: `.part` debris or metadata without an archive, older than 1h) are
  reaped so they can never wedge an entry or the quota.

**Read path survey:**
- `GET /twirp-blob/cache/<token>` (the v2 download) previously did
  `cache.get()` — loading the whole archive into RAM — then served it. It now
  resolves metadata only (`CacheStore::lookup`) and streams the file with
  `ReaderStream`. The staging-file fallback branch streams too.
- `twirp_cache_v2_get_dl_url` and both existence checks (`create`, finalize's
  already-exists path) use the metadata-only `lookup` instead of `get`: a
  restore lookup no longer pulls the archive into RAM just to throw the bytes
  away.
- `GET /api/v1/cache` (legacy base64-in-JSON) is unchanged by design: base64
  in a JSON body inherently requires the full content in memory. The v2
  streaming download is the large-blob path.

## Behavior changes

- Legacy `cache_reserve` now creates its staging file up front; a failed
  reserve (disk full, staging unwritable) surfaces at reserve time instead of
  at first chunk.
- `sweep_pending_uploads` signature: now returns `Vec<PathBuf>` (staging
  files to delete); `reap_once` deletes them.

## Tests

- `preloop-cache` (12 passed):
  - `commit_file_streams_large_blob_with_small_rss_delta`: finalizes a
    **150 MB** blob, asserts process RSS delta **< 64 MiB**.
  - `quota_evicts_least_recently_used`: 3 MiB quota, 4× 1 MiB entries; the
    oldest *untouched* entry is evicted while a recently-read older entry
    and the newest survive; total back under quota.
  - `oversized_single_entry_survives_its_own_commit`, plus
    `commit_file` round-trip/duplicate and metadata-only `lookup` tests.
- `cargo check` + `cargo clippy --all-targets`: clean. `cargo fmt`: applied.

## Repro verification (patched server, port 19109)

Setup: `preloop store migrate` then `preloop serve --listen 127.0.0.1:19109`
with `PRELOOP_HOME=<worktree>/.repro-19109`,
`PRELOOP_SYSTEM_TOKEN=repro-sys-token-19109`,
`PRELOOP_CACHE_TOTAL_QUOTA_BYTES=262144000` (250 MiB, to exercise eviction).
State dir under the worktree, never /tmp. Server PID 19041; RSS sampled from
`/proc/<pid>/status` (VmRSS).

**1. Streaming finalize — the +265 MB spike is gone.**
Drove the legacy chunked flow with the system bearer (same fixed commit
primitive as the Twirp v2 finalize, plus it exercises all the new staging
code):
- `POST /_apis/artifactcache/cache` `{"key":"repro-key-a2","version":"v1"}` → `cacheId`
- 75× `PATCH /_apis/artifactcache/cache/<id>` with 2 MiB chunks (150 MB blob)
- `POST /_apis/artifactcache/cache/<id>` `{"size":157286400}` → commit

| Phase | Server RSS |
|---|---|
| Baseline | 104.5 MiB |
| Peak during 150 MB chunked upload | 113.3 MiB (delta 8.8 MiB) |
| **Peak during commit (finalize)** | **107.8 MiB — delta 0.1 MiB** |

Round-3 unpatched: 69 → 341 MB (+265 MB) during finalize. The finalize is
now a file rename + directory scan; commit returned in <0.1s. Committed
archive sha256 matches the source blob; `cache_lookup` reports the entry.

**2. Quota eviction end-to-end.** With the 250 MiB quota:
- Committed `repro-key-a` (150 MB), then `repro-key-a2` (150 MB): the second
  commit pushed the total to 300 MB → `repro-key-a` evicted. Disk:
  `state/cache/` = 151M, 1 archive.
- LRU ordering: committed `lru-1..lru-3` (50 MB each), read `lru-1` via
  lookup (refreshes its LRU timestamp), committed `lru-4..lru-6`. Evicted:
  `repro-key-a2` (oldest) then `lru-2` (oldest *untouched* — `lru-1` survived
  because it was read). Final: `lru-1,lru-3,lru-4,lru-5,lru-6` on disk,
  total 251M ≈ quota.

**3. Read path.** `twirp_cache_v2_get_dl_url` and both existence checks now
use metadata-only `lookup` (no archive bytes loaded). The `/twirp-blob/cache`
download streams via `ReaderStream` (same idiom as the action-tarball path);
the signed-URL download leg needs a live job token so it was verified by code
inspection, not live — the legacy base64 endpoint (`GET /api/v1/cache`) is
unchanged by design (base64-in-JSON inherently buffers).

**4. Unit tests.** `cargo test -p preloop-cache`: 12/12 pass, including
`commit_file_streams_large_blob_with_small_rss_delta` (150 MB finalize, RSS
delta < 64 MiB bound) and `quota_evicts_least_recently_used`. `cargo check`
and `cargo clippy -p preloop-cache -p preloop-runner-server --all-targets`:
clean. `cargo fmt`: applied.

Not run: the full `just test-ci` gate (fmt+clippy done; the workspace test
suite needs hours under the current 4-way sibling build contention — the
server's lib test profile alone takes ~30 min to link here).
