# Plan 001: Caching performance strategy for local, self-hosted, and managed CI

> **Status**: Proposed architecture and implementation roadmap
>
> **Priority**: P1
> **Category**: performance, correctness, security, architecture
> **Planned at**: commit `1342346a`, 2026-07-27
> **Drift check before implementation**: `git diff --stat 1342346a..HEAD -- crates/aksh-cache crates/aksh-runner-server crates/aksh-runner crates/preloop-orchestrator docs`

## Executive decision

Preloop should not build one undifferentiated cache. It should build a layered cache system with three properties:

1. **Immutable content-addressed blobs** hold bytes once and are addressed by a verified digest.
2. **Scoped metadata** maps workflow-visible keys to those blobs under a server-derived tenant, repository, and trust domain.
3. **Guest-local materialization** makes hot content available to a runner without making a shared writable filesystem part of job correctness.

The deployment profiles use the same logical model but different storage:

| Profile | Durable metadata | Durable blobs | Hot tier | Primary goal |
|---|---|---|---|---|
| Local CI | SQLite under `~/.preloop/cache/v1` | Local content-addressed files | Golden/COW disk and host page cache | Lowest repeated-run latency with bounded disk use |
| Self-hosted CI | SQLite for one node; Postgres for multiple nodes | Persistent disk or S3-compatible storage such as MinIO | Per-runner-node NVMe/read-through cache | Reliable reuse across runners without requiring shared mounts |
| Managed CI | Regional Postgres | Versioned object storage with KMS encryption | Regional node-local NVMe/read-through cache | Multi-tenant isolation, scale, predictable cost, and regional throughput |

The first performance work should target measured losses, not speculative caching:

- Corrected Vite warm: Preloop `92.45 s`, Agent CI `38.71 s`.
- Actual build plus lint: Preloop `9.91 s`, Agent CI `10.40 s`.
- Preloop spent `34.24 s` in `setup-node`/cache handling, `16.50 s` in `pnpm install`, and `28.31 s` in the post-job cache save.
- The cache restore emitted `File name too long`, so dependency installation ran on a cache miss.
- Cold time before the first guest step was `48.54 s` for ripgrep, `545.84 s` for Vite, and `18.59 s` for testcontainers-go. That is VM/image preparation, not workload execution.

This evidence says the priority order is:

1. Make declared toolchains discoverable in the official toolcache layout.
2. Make Actions cache restore/save correct, indexed, and streaming.
3. Persist package download stores across disposable runners.
4. Resolve and cache immutable action/image/tool artifacts by digest.
5. Move VM preparation out of the run critical path.

## Goals

- Preserve official-runner and unmodified-workflow compatibility.
- Make a cache miss equivalent to normal uncached execution, except when a workflow explicitly requests `fail-on-cache-miss`.
- Eliminate repeated downloads and archive work that dominates warm jobs.
- Keep process memory usage proportional to a transfer chunk, not cache size.
- Prevent untrusted jobs, repositories, and tenants from poisoning caches consumed by trusted jobs.
- Bound disk/object-store growth with quotas, expiration, admission, and eviction.
- Give operators enough telemetry to explain every hit, miss, restore, save, and eviction.
- Use one architecture across local, self-hosted, and managed deployments without forcing managed-service complexity into local CI.

## Non-goals

- Making local CI pass because an undeclared tool happens to exist. Workflows must remain portable to GitHub Actions.
- Treating artifacts or logs as disposable caches. They are user-visible outputs with separate retention semantics.
- Reusing complete workspaces across jobs. Workspaces remain disposable unless a workflow explicitly saves paths through the cache protocol.
- Sharing mutable build caches across security boundaries.
- Replacing package-manager integrity checks or OCI digest verification.
- Guaranteeing that a cache survives eviction. A job must remain correct on a miss.

## Current state and evidence

### Actions cache store

`crates/aksh-cache/src/lib.rs` implements a local file-backed `CacheStore`:

- cache identity is a SHA-256-derived directory;
- original key, version, and creation time are stored as metadata;
- entries are immutable;
- restore order supports exact key and prefix matching;
- prefix lookup scans every cache directory;
- complete archives are read into `Vec<u8>` on restore;
- there is no quota, expiration, eviction policy, or indexed lookup.

`crates/aksh-runner-server/src/models.rs` stores v1 uploads in `PendingCache.bytes`.

`crates/aksh-runner-server/src/blob_store.rs` stages v2 blocks on disk, then:

- assembles all blocks in a single `Vec<u8>`;
- writes a second complete copy;
- reads complete blobs to return downloads.

`crates/aksh-runner-server/src/results_twirp.rs` reads the complete staged blob during finalize and passes complete bytes to `CacheStore::put`. This is acceptable for small local caches but cannot be the self-hosted or managed data path.

### Cache scoping and authorization

`results_twirp::scoped_cache_key` currently combines workflow-requested `repository` and `scope` fields. Managed isolation cannot trust these values. The server must derive tenant, repository, ref/trust class, plan, and job identity from authenticated server state.

The runtime token proves an `Actions.Results:<plan>:<job>` scope, but the cache API needs a stronger resolved authorization context before it becomes multi-tenant. Download tokens are stored in memory and should become expiring, operation-specific capabilities.

### Toolcache mismatch

The runner exports:

```text
RUNNER_TOOL_CACHE=/var/lib/preloop-runner/_work/_tool
```

The JavaScript action runtime is installed separately:

```text
/var/lib/preloop-runner/externals/node24/bin/node
```

The first path is where setup actions look for workflow-declared tools. The second is an internal runtime used to execute JavaScript actions. `actions/setup-node` therefore downloads Node even though the runner has a Node executable.

A declared Node installation must be materialized in the official toolcache shape:

```text
$RUNNER_TOOL_CACHE/node/<exact-version>/<arch>/bin/node
$RUNNER_TOOL_CACHE/node/<exact-version>/<arch>.complete
```

Do not merely put `/externals/node24/bin` on `PATH` and call the setup step satisfied. The setup action owns version selection and must observe the version it requested.

### Golden VMs and images

`crates/preloop-orchestrator/src/environment.rs` computes an environment fingerprint from a base-image string and declared toolchain layers. A mutable tag is not a durable compatibility identity; the fingerprint must ultimately include the resolved base-image digest.

Declared service/container images are preloaded into a golden and inherited by COW forks. Keeping preload images outside the environment compatibility fingerprint is reasonable because they are an optimization, not semantic environment state. Record them as a **coverage set** on the golden and select an existing compatible golden whose coverage is a superset; do not rebuild a golden solely because an optional preload set differs.

`docs/preloop-performance-engineering.md` records that an additional virtiofs cache mount caused 20–50 second fork stalls and instability. The cache design must not add one mount per ecosystem or per cache. Prefer COW image contents, one stable existing transport, or HTTP/cache-service transfer.

### Other useful caches

- `crates/aksh-runner-server/src/actions.rs` streams action tarballs into an atomic on-disk cache keyed by owner/repository/ref. Mutable tags are cached indefinitely; resolve tags to commit SHA and store immutable content by digest.
- `crates/aksh-runner-server/src/snapshots.rs` maintains a lock-protected Git object cache per local Git common-directory identity and persists stat data. Its documented `git add` result improves from `156 ms` to `16 ms` for a 6,000-file workspace. This is the right pattern: immutable objects, narrow identity, atomic refresh, and no workspace reuse.
- `crates/preloop-orchestrator/src/keys.rs` deliberately gives every runner a unique RSA key. Runner private keys, credentials, and secret-bearing state must never enter a shared cache or golden image.

## Cache taxonomy

Each class has different identity, mutability, trust, and placement rules.

| Class | Examples | Cache? | Scope | Preferred placement |
|---|---|---|---|---|
| Runner internals | Node 20/24 used to execute JS actions | Yes, immutable | Preloop release + OS + arch | Base/golden image under `externals/` |
| Declared toolchains | Node, Python, Go, Java, Rust, .NET selected by `setup-*` | Yes | Exact version + platform + toolcache format | Host tool CAS; materialize into golden or `$RUNNER_TOOL_CACHE` |
| Action source | `actions/checkout`, `setup-node`, third-party actions | Yes, immutable after resolution | Owner/repo/commit SHA | Shared verified action CAS |
| OCI images/layers | base VM input, job containers, services | Yes, immutable | Registry/repository/digest/platform | Registry mirror + node-local layer cache + golden coverage |
| Package download stores | pnpm store, npm cache, Cargo registry, Go modules, uv wheels | Yes | Ecosystem integrity identity + platform where needed | Persistent host/node store with guest-local materialization |
| Compiler/build cache | sccache objects, Go build cache, Vite transform cache | Yes, cautiously | Source/config/toolchain/target/features | Repository/trust-scoped cache |
| Actions cache archives | User paths saved through `actions/cache` or setup actions | Yes | Server-derived namespace + workflow key/version | Indexed metadata + blob CAS |
| Git snapshot objects | Immutable Git objects and stat index | Yes | Local Git common-dir identity or repository identity | Host-local persistent store |
| Workspace | Checked-out source and arbitrary generated files | No implicit reuse | One job/run | Disposable guest disk |
| Artifacts and logs | Build deliverables, test reports, logs | Store, but not as cache | Run/job retention policy | Artifact/log store |
| Secrets and identity | tokens, runner keys, `.credentials`, `.npmrc`, cloud config | Never shared-cache | One job or runner | Secret channel and ephemeral memory/disk only |

## The storage model

### Separate logical identity from physical bytes

A logical cache entry is metadata:

```text
CacheEntry {
  namespace,
  cache_class,
  user_key,
  version,
  blob_digest,
  compressed_size,
  uncompressed_size,
  compression,
  created_at,
  last_accessed_at,
  expires_at,
  producer_identity,
  trust_domain,
  state
}
```

The physical blob is immutable:

```text
blobs/sha256/ab/cd/<digest>
```

Benefits:

- user-controlled keys never become filesystem paths;
- identical bytes may be deduplicated without weakening logical isolation;
- atomic publication is simple;
- metadata lookup does not scan directories;
- eviction can remove logical references before garbage-collecting unreferenced blobs;
- blob integrity is verified independently from cache-key correctness.

Deduplication must not become a cross-tenant information oracle. A managed service may physically deduplicate encrypted/public content internally, but API behavior, timing, billing, and existence checks must remain tenant-scoped.

### Required operations

1. **Reserve** a logical key/version under an authenticated namespace.
2. **Upload** chunks or multipart blocks to a staging identity.
3. **Finalize** only after size and digest verification; atomically publish metadata.
4. **Lookup** exact key, then restore prefixes in official order.
5. **Download** by an expiring, read-only capability.
6. **Touch** access metadata asynchronously.
7. **Expire/evict** metadata, then garbage-collect unreferenced blobs after a grace period.

Readers must never observe pending uploads. Concurrent writers for the same immutable key/version should produce one winner; the loser gets the protocol-equivalent “already exists” result.

### Streaming requirements

- Upload memory use: `O(chunk_size)`.
- Block assembly: concatenate using streaming file/object-store operations, never a complete `Vec<u8>`.
- Finalization: compute digest and size while streaming; avoid staging-to-store-to-memory copies.
- Download: stream file/object-store body with backpressure and range support where the client uses it.
- Managed CI: issue short-lived direct object-store upload/download URLs after authorization; keep archive bytes off control-plane instances.
- Self-hosted single-node: `sendfile`/streamed file responses are sufficient.

## Cache key design

### General rule

A key must include every input that can change the cached output. If that set cannot be identified, do not cache the output.

Use canonical serialization followed by a hash for storage identity. Keep human-readable components in metadata and telemetry.

Logical namespace:

```text
schema_version
+ tenant_id
+ repository_id
+ trust_domain
+ cache_class
+ workflow-visible key/version
```

Compatibility inputs then vary by class.

### Trust domain

Recommended trust classes:

```text
trusted-default-branch
trusted-protected-branch:<ref-id>
untrusted-pr:<source-repository-id>:<pr-number>
trusted-manual:<actor-policy-id>
```

Derive this from the authenticated run, checked-out ref, source repository, permissions, and execution policy. Do not derive it only from event name or request fields; `pull_request_target`, reusable workflows, and manually elevated runs make that unsafe.

Default policy:

- trusted jobs may read and write trusted repository caches;
- untrusted pull-request jobs may read explicitly allowed base-branch dependency caches;
- untrusted jobs write only to their isolated PR/fork namespace;
- trusted jobs never restore from an untrusted namespace;
- public immutable tool/action/image content verified by digest may be shared globally;
- private repositories and mutable build products never cross tenant/repository boundaries.

### Key recipes

#### Toolchains

```text
tool/v1/<tool>/<exact-version>/<os>/<arch>/<libc>/<toolcache-format>/<upstream-sha256>
```

Include exact version, not only major version. A workflow may request `24`, but the resolved cache entry should identify `24.18.0` and maintain a separately expiring major-to-exact resolution record.

#### Action source

```text
action/v1/<owner>/<repo>/<resolved-commit-sha>/<archive-sha256>
```

Mutable refs such as `v4`, branches, and tags are resolution records with a short TTL. They point to immutable commit/archive entries. Pinning by commit should bypass mutable-ref TTL concerns.

#### OCI images

```text
oci/v1/<registry>/<repository>/<manifest-digest>/<platform-os>/<platform-arch>
```

Cache layers by their OCI digest. Golden compatibility must use the resolved base manifest/config digest, never `ubuntu:24.04` or another mutable tag alone.

#### Package download stores

Prefer the ecosystem’s verified content store over archiving an installed dependency tree:

```text
package/v1/<ecosystem>/<content-integrity>/<platform-if-native>
```

Examples:

- pnpm/npm: package integrity digest; share verified tarballs/store objects, not arbitrary `node_modules`.
- Cargo registry: crate checksum; Cargo Git DB by repository and commit.
- Go modules: module path/version/checksum; build cache remains platform/toolchain-specific.
- uv/pip: wheel/sdist hash, Python ABI, platform tag for native wheels.

A package store can safely benefit many lockfiles because the package manager verifies content. A hydrated dependency tree is more fragile and requires a stricter repository-specific key.

#### Actions cache archive for dependencies

The workflow supplies the primary key, restore keys, and cache version. The server prepends its private namespace. Recommended workflow key ingredients:

```text
<os>-<arch>-<package-manager-major>-<runtime-abi>-<lockfile-hash>
```

Do not silently rewrite the workflow’s visible key because restore behavior is user-facing. Namespace it server-side and expose diagnostics showing both the visible key digest and the server scope.

#### Build outputs

```text
build/v1/<repo>/<tool>/<tool-version>/<target>/<profile>/<feature-hash>/
         <relevant-env-hash>/<source-and-config-hash>
```

Examples of relevant inputs:

- Rust: rustc version, target triple, features, profile, `RUSTFLAGS`, build-script inputs, source hash.
- Go: Go version, `GOOS`, `GOARCH`, CGO/toolchain inputs, module graph, source hash.
- JavaScript transforms: Node version, package-manager version, lockfile, bundler version/config, source hash.

Prefer compiler-native remote caches such as sccache when they correctly encode compiler inputs. Avoid caching a whole `target/` or `node_modules/` directory under a weak branch-only key.

#### Golden VM

Compatibility identity:

```text
golden/v1/<base-image-digest>/<guest-arch>/<kernel-and-smolvm-version>/
          <runner-bundle-sha>/<provisioner-sha>/<declared-toolchain-set-hash>/
          <baseline-packages-hash>
```

Optimization metadata, not compatibility identity:

```text
preloaded OCI digest set
action digest set
last used
build cost
```

Select a compatible golden with a useful preload superset. Do not create a new 249-second golden merely to save a 4–9-second optional image pull.

#### Git snapshot objects

Local CI may continue keying by canonical Git common-directory identity because it identifies the developer’s repository object store. Self-hosted/managed systems should use repository identity plus immutable commit/object IDs and keep working-tree stat indexes node-local. Never share a mutable working-tree index across repositories or tenants.

## Placement and data flow

### L0: process-local coordination

Cache only small metadata:

- in-flight reservation deduplication;
- short negative cache for failed mutable-ref resolutions;
- recently resolved metadata entries.

Do not retain archive bytes in process memory. Every L0 entry needs a short TTL and bounded size.

### L1: guest-local ephemeral state

Use for active job extraction, package-manager working state, and compiler scratch data. It disappears with the runner. The guest may read immutable content inherited from a golden, but arbitrary job writes must stay in its COW layer.

### L2: node-local persistent CAS

Use for hot tools, actions, package objects, OCI layers, and recently restored cache archives. It provides high throughput without adding one VM mount per cache. Population is atomic and lock-protected.

A node-local entry is an optimization. Its loss must fall back to L3 or origin.

### L3: durable blob and metadata store

- Local: host filesystem plus SQLite.
- Self-hosted: persistent disk or S3-compatible object store; SQLite for one node, Postgres for multiple nodes.
- Managed: object storage plus Postgres, with regional placement and KMS encryption.

The runner accesses user `actions/cache` entries through the protocol, not a shared writable filesystem. BYO self-hosted runners cannot be assumed to share a mount with the control plane.

## Deployment profiles

### Local CI

Assumptions:

- one developer machine;
- one implicit tenant;
- loopback control plane;
- disposable SmolVM runners;
- latency matters more than cross-host durability.

Recommended layout:

```text
~/.preloop/cache/v1/
├── index.sqlite
├── blobs/sha256/
├── staging/
├── locks/
├── tools/
├── actions/
├── packages/
└── metrics/
```

Policy:

- write-through to local disk;
- one cache root, not one virtiofs mount per cache class;
- seed declared toolchains into an environment-specific golden/toolcache;
- keep runner internals in `externals/`, separate from declared workflow tools;
- retain the Git snapshot object cache;
- preload declared service/container images by digest;
- resolve action tags but store archives by commit and digest;
- default starting quota: 20 GiB soft target and configurable hard limit; expose `preloop cache status`, `preloop cache prune`, and per-class usage before enabling automatic destructive pruning;
- encryption at rest is optional because the local OS account is the trust boundary, but permissions must be user-only and secrets remain forbidden.

Local performance targets, to validate rather than assume:

- metadata lookup p95 below 10 ms;
- node/tool cache hit materialization below 1 s;
- Vite warm setup-node/cache phase below 2 s;
- Vite cache save below 3 s or durably spooled before asynchronous upload;
- corrected Vite warm total below 45 s on the benchmark host;
- memory stays bounded during multi-gigabyte cache transfers.

### Self-hosted CI

Assumptions:

- one organization or a small set of tenants;
- official runners may be remote/BYO;
- runners may be long-lived or ephemeral;
- service restart must not lose important cache metadata.

Single-node recommendation:

- SQLite metadata with WAL;
- persistent local blob filesystem;
- background index/metadata backup;
- HTTPS cache API with expiring capability URLs.

Multi-node recommendation:

- Postgres metadata;
- S3-compatible blob store such as MinIO;
- node-local read-through CAS on runner/control nodes;
- direct signed upload/download URLs;
- distributed reservation using a database uniqueness constraint, not an in-memory mutex.

Policy:

- namespaces are mandatory even for one organization because repositories and fork PRs differ in trust;
- configurable per-repository and per-tenant quotas;
- trusted-default-branch caches may seed trusted feature branches;
- untrusted PR writes remain isolated;
- node-local hot tiers are disposable and reconcile from durable metadata;
- action/tool/image mirrors may be organization-wide only when immutable and verified;
- audit cache administration and cross-scope policy decisions;
- support offline/air-gapped operation by pre-seeding verified action/tool/image CAS entries.

Suggested initial sizing—not a universal default:

- reserve node-local NVMe for the measured working set, typically 50–200 GiB per runner node;
- set repository logical quotas from observed package/build sizes rather than a fixed global number;
- alert at 70%, begin eviction at 80%, and reject new writes only at the hard limit after eviction cannot recover space.

### Managed CI

Assumptions:

- adversarial multi-tenancy;
- many repositories and concurrent runners;
- regional execution;
- customer-visible retention, deletion, and billing requirements.

Required architecture:

- regional Postgres metadata with explicit tenant/repository/trust columns and row-level authorization in the service layer;
- object storage with bucket/versioning/lifecycle rules and envelope encryption through KMS;
- short-lived operation-specific signed URLs, normally 5–15 minutes;
- multipart upload with maximum compressed and uncompressed size enforcement;
- regional node-local NVMe read-through caches;
- immutable, integrity-verified global cache only for public tools/actions/OCI layers;
- private and mutable content isolated by tenant and repository;
- deletion workflow that removes logical entries immediately and garbage-collects blobs after a grace period;
- per-tenant rate limits, concurrent-transfer limits, quotas, and cost attribution;
- complete audit logs for administrative reads/deletes and policy changes.

Managed cache tiers should be regional. Cross-region replication is useful for immutable global tools and customer-selected durable caches, but synchronous cross-region writes should not sit on every job’s completion path.

## Security design

### Never cache

- runner RSA private keys or shared runner credentials;
- OAuth, GitHub, cloud, package-registry, or signing tokens;
- `.runner`, `.credentials`, `.credentials_rsaparams`;
- `.npmrc`, `.pypirc`, `.netrc`, Docker auth config, cloud CLI credential directories;
- secret-bearing `$GITHUB_ENV`, `$GITHUB_OUTPUT`, process environments, or debug state;
- decrypted job messages;
- arbitrary home directories.

Path deny-lists are defense in depth, not the primary boundary. The primary controls are narrow cache paths, trust-scoped namespaces, ephemeral credentials, and no implicit whole-workspace caching.

### Poisoning prevention

- Derive namespace and trust from authenticated server state.
- Permit global sharing only for immutable bytes verified against an upstream digest/signature.
- Resolve mutable action and image references to immutable identities before storage.
- Never let an untrusted job write a key that a trusted job can restore.
- Treat restore-prefix matches as scoped reads; a prefix must not cross trust/repository boundaries.
- Record producer run, repository, commit, and trust domain in metadata for audit and incident response.

### Capability URLs

A signed upload/download capability must bind:

```text
tenant + repository + operation + blob/upload id + maximum size + expiry + nonce
```

Properties:

- short expiry;
- read or write, never both;
- no privilege escalation through path/query changes;
- maximum upload size enforced independently from client headers;
- safe retry/idempotency semantics;
- revocable through upload state or signing-key rotation;
- no permanent bearer tokens in logs.

### Archive safety

The cache service may treat archives as opaque, but runner extraction must still enforce:

- no path traversal outside requested roots;
- safe symlink/hardlink handling;
- maximum expanded size and file count in managed CI;
- compression-bomb limits;
- no setuid/device nodes or ownership restoration that crosses policy;
- integrity check before extraction.

### Encryption and deletion

- Local: rely on user-account filesystem permissions unless the operator selects encrypted storage.
- Self-hosted: support encrypted disks/object storage and organization-managed keys.
- Managed: envelope encryption with per-tenant or policy-group KMS context; TLS for every transfer.
- Do not use a customer-specific encryption key for globally deduplicated private blobs unless the encryption/deduplication design explicitly supports it.
- Deleting a tenant/repository must remove metadata immediately and enqueue physical garbage collection. Backups need documented retention and cryptographic erasure behavior.

## Correctness rules

1. **Misses are normal.** Backend failure should degrade to a miss/warning when official cache semantics allow it, not fail unrelated job work.
2. **Explicit failure remains explicit.** Preserve `fail-on-cache-miss` and action-defined failure behavior.
3. **Immutable publication.** Write staging, verify, atomically publish; never mutate a finalized key/version.
4. **No partial reads.** Readers see pending or complete, never both.
5. **Version archive semantics.** Compression method and cached path set contribute to the cache version so incompatible archives do not match.
6. **Safe restore keys.** Prefix matches follow official order and remain within the authenticated namespace.
7. **Reproducibility first.** Preload only declared toolchains/containers. A faster local pass that would fail on GitHub is incorrect.
8. **Resolved identities.** Mutable labels are lookup hints, not immutable storage identities.
9. **Crash recovery.** Finalization and metadata/blob reference updates are transactional or restart-reconcilable.
10. **Clock independence.** Ordering uses server timestamps or monotonic sequence IDs; clients do not choose creation time.
11. **Bounded retries.** Cache outages cannot add minutes of exponential backoff to a job. Set per-operation latency budgets and fall back where allowed.
12. **Isolation before deduplication.** Physical byte reuse must not change logical authorization.

## Admission, retention, and eviction

Caching everything is not optimal. Large one-hit entries consume bandwidth twice and evict useful hot data.

Track for each entry:

- compressed and uncompressed size;
- restore count;
- last access;
- save, restore, and origin-download cost;
- producer and trust scope;
- expiration and pin state.

Admission policy:

- always admit small verified tools/actions with high expected reuse;
- admit package content objects after integrity verification;
- reject or short-retain oversized one-off workflow caches unless explicitly allowed;
- coalesce concurrent uploads for the same immutable identity;
- use historical workflow manifests to prefetch only repeatedly used declared content.

Eviction policy:

1. Never evict active uploads or blobs with active readers.
2. Expire logical entries first.
3. Prefer evicting unreferenced, old, low-hit, cheap-to-recreate bytes.
4. Bias toward retaining high-download-cost tools/images and frequently restored dependency caches.
5. Garbage-collect a physical blob only after no metadata references it and a grace period has elapsed.
6. Keep soft and hard quotas separate so background eviction runs before writes must be rejected.

Starting retention classes:

| Class | Starting policy | Reason |
|---|---|---|
| Mutable tag/ref resolution | minutes to hours | Must observe upstream movement |
| Immutable action/tool/image blob | LRU under capacity; long TTL | Verified and expensive to redownload |
| Package content store | LRU under capacity | Content-addressed and broadly reusable |
| Branch dependency cache | 7–30 days since last access | Useful during active development |
| PR/fork writable cache | PR lifetime plus short grace | Limits untrusted accumulation |
| Build output cache | shorter TTL, cost-aware admission | Large and sensitive to weak keys |
| Staging upload | hours, never days | Incomplete and not restorable |
| Negative lookup | seconds to minutes | Avoids hiding newly published content |

These are policy starting points. Ship telemetry and operator controls before fixing service-wide constants.

## Performance techniques by layer

### Toolchains

- Maintain a host-side immutable tool distribution CAS.
- During golden preparation, materialize only toolchains explicitly declared by parsed setup actions/version files.
- Write official toolcache completion markers only after verification.
- Prefer reflink/hardlink/COW materialization where filesystem and ownership rules permit; otherwise copy from local CAS, never redownload.
- Keep the internal JavaScript action runtime separate from workflow-selected Node.

### Package managers

- Set stable package-store locations, not action-installation-relative paths.
- Cache package content, not arbitrary home directories.
- Let pnpm/npm/Cargo/Go/uv perform their integrity checks.
- Avoid archiving huge stores on every job when a node-local read-through store can serve immutable objects directly.
- For remote/BYO runners, use `actions/cache` archives or ecosystem-native remote caches; do not assume host mounts.

### Actions cache protocol

- Replace directory scans with indexed exact/prefix lookup.
- Stream archive bytes end to end.
- Allow local durable spool plus asynchronous remote replication only after documenting the durability tradeoff. A successful finalize must mean “durable enough for this profile.”
- Avoid recompressing an already compressed cache archive.
- Keep post-job cache work visible in step telemetry.

### VM and OCI preparation

- Resolve base images to digests before fingerprinting.
- Use a prepared, digest-pinned Ubuntu image to avoid base pulls and repeated apt installation on the run path.
- Mirror Docker Hub dependencies to avoid anonymous pull-rate limits.
- Build/refresh goldens in the background and maintain warm capacity.
- Store preload coverage separately from compatibility identity.
- Do not add per-cache virtiofs mounts to forked goldens.

### Metadata and control plane

- Keep cache metadata out of the global `Arc<Mutex<InnerState>>` in server profiles.
- Use database uniqueness for reservation and idempotent finalize.
- Update `last_accessed_at` asynchronously/batched.
- Paginate administration/list APIs.
- Separate cache data transfer from scheduling/control-plane request pools.

## Observability

Every cache operation should emit structured metrics and trace fields:

```text
cache.class
cache.namespace_hash
cache.visible_key_hash
cache.version_hash
cache.outcome = exact_hit | prefix_hit | miss | rejected | corrupt | error
cache.tier = guest | node | durable | origin
cache.bytes_compressed
cache.bytes_uncompressed
cache.lookup_ms
cache.first_byte_ms
cache.transfer_ms
cache.archive_ms
cache.extract_ms
cache.verify_ms
cache.save_ms
cache.eviction_reason
cache.trust_domain
```

Do not log secret-bearing raw keys or signed URLs. Hash keys for correlation and expose raw workflow keys only in appropriately authorized local diagnostics.

Job UI/logs should make the critical path obvious:

```text
setup-node: toolcache exact hit, Node 24.18.0, 180 ms
pnpm cache: prefix miss, reason=not_found, lookup 12 ms
pnpm install: 16.5 s
cache save: 1.2 GiB, archive 8.1 s, upload 3.4 s
```

Dashboards:

- hit ratio and byte-hit ratio by class/profile/repository;
- time saved versus save/restore cost;
- p50/p95 lookup, first-byte, transfer, archive, and extract latency;
- cache-service errors and fallback-to-miss rate;
- logical versus physical bytes and deduplication ratio;
- eviction churn and rejected writes;
- untrusted/trusted access denials;
- cold VM phase breakdown before first guest step.

A hit ratio alone is not enough. A 95% hit ratio can still lose time if archive/extraction costs exceed origin installation.

## Verification and benchmark plan

### Correctness matrix

Test with the official runner and `actions/cache@v4`:

- exact-key hit;
- ordered restore-prefix hit;
- version mismatch;
- immutable duplicate save;
- concurrent save of one key;
- interrupted multipart upload;
- restart between upload and finalize;
- corrupt block/digest;
- quota exhaustion;
- eviction while another entry downloads;
- untrusted PR cannot write/read trusted-only entries;
- tenant/repository scope cannot be forged through request fields;
- expired/replayed signed URL;
- multi-gigabyte transfer with bounded process RSS;
- backend outage degrades according to action semantics.

Run protocol changes against the official runner, not only unit tests. Final implementation gates remain:

```text
just test-ci
just dogfood
```

### Performance matrix

For each representative repository, record cold, warm, and forced-miss runs:

- command-to-run-created;
- run-created-to-runner-first-log;
- setup action durations;
- exact/prefix/miss status;
- archive, upload, download, and extraction time;
- origin bytes and cache bytes;
- host and guest CPU/RSS where observable;
- cache size before and after;
- second and fifth repetition.

Keep the existing five-repository slices and add controlled synthetic cases:

- 10 MiB, 1 GiB, and 10 GiB cache archives;
- many small files versus a few large files;
- concurrent readers of one blob;
- concurrent writers of one key;
- cold node-local tier with warm object storage;
- warm node-local tier;
- cross-tenant denial cases.

Success criteria for the first campaign:

- Vite build/lint remains within 10% of the existing measured `9.91 s`;
- Vite warm total falls below `45 s` on the same host;
- setup-node exact toolcache hit is below `2 s`;
- cache restore/save no longer reports `File name too long`;
- server RSS does not scale with archive size;
- cold first-step latency returns to the prepared-image target range documented in `docs/preloop-performance-engineering.md`, without Docker Hub rate-limit dependence.

## Phased roadmap

### Phase 0: Instrument before changing policy

Target areas:

- cache handlers in `aksh-runner-server`;
- `CacheStore` operations;
- orchestrator golden/tool/image phases;
- runner setup/post action timing.

Deliverables:

- structured phase metrics;
- cache hit/miss reasons;
- byte and latency accounting;
- benchmark command that produces a comparable JSON record.

Do not optimize a phase that cannot be separately measured.

### Phase 1: Fix local correctness and measured Vite losses

1. Reproduce and fix the cache backend `File name too long` failure at its source.
2. Add indexed metadata for exact and prefix lookup.
3. Stream v1/v2 upload, finalize, and download without complete in-memory buffers.
4. Materialize workflow-declared Node into `$RUNNER_TOOL_CACHE` in the official layout.
5. Configure a stable pnpm store and verify a real warm restore.
6. Add quota/status/prune controls before automatic eviction.

Acceptance: corrected Vite passes twice, the second run restores a cache, and warm total meets the first-campaign target.

### Phase 2: Immutable supply and VM caches

1. Resolve action tags/branches to commit SHA and cache immutable archives by digest.
2. Resolve base/container image tags to OCI digests.
3. Update golden compatibility identity to include resolved immutable inputs.
4. Maintain preload coverage as selection metadata.
5. Produce and consume a prepared digest-pinned Ubuntu base artifact.
6. Mirror upstream tool/image downloads used by normal jobs.

Acceptance: a cold run does not pull from Docker Hub or run baseline apt installation on the job path.

### Phase 3: Self-hosted durable backend

1. Introduce metadata/blob-store interfaces without weakening local behavior.
2. Implement single-node SQLite/filesystem and multi-node Postgres/S3-compatible backends.
3. Add node-local read-through CAS.
4. Derive cache namespace and trust server-side.
5. Add expiring capability URLs, quotas, and audit events.
6. Validate with an official runner on a separate machine/network namespace.

### Phase 4: Managed multi-tenancy

1. Enforce tenant/repository/trust policy on every cache operation.
2. Add KMS envelope encryption and deletion lifecycle.
3. Add regional object storage and NVMe hot tiers.
4. Add per-tenant quotas, rate limits, cost attribution, and abuse controls.
5. Add managed threat-model and cross-tenant penetration tests.
6. Add SLOs and capacity planning from production distributions.

## Implementation boundaries

The architecture should introduce narrow interfaces, not a generic cache framework that every crate must adopt.

Suggested seams:

```text
CacheMetadataStore
  reserve(namespace, key, version)
  lookup(namespace, key, version, restore_keys)
  finalize(reservation, blob, metadata)
  expire(entry)

BlobStore
  begin_upload(limits)
  put_part(upload, part, stream)
  finalize_upload(upload, expected_digest)
  open(digest, range)
  delete(digest)

HotCache
  get(digest)
  populate(digest, stream)
  evict(policy)
```

Toolchains, actions, OCI layers, and Git objects may use the same underlying blob CAS where useful, but retain class-specific resolvers and policy. Do not force Git object semantics or package-manager stores through the user-facing Actions cache API.

## Rejected approaches

### One shared writable tool/package directory mounted into every VM

Rejected because concurrent jobs can corrupt it, trust boundaries become unclear, and prior extra virtiofs mounts caused measurable fork stalls and instability. Use immutable host/node CAS plus guest-local materialization.

### Bake every common tool into one universal golden

Rejected because undeclared tools make workflows pass locally and fail on GitHub, images grow without bound, and any update invalidates a large artifact. Bake runner internals and baseline packages; layer only workflow-declared toolchains.

### Cache complete workspaces

Rejected because stale files create false passes, cross-branch contamination, and secret leakage. Cache immutable Git objects and explicit workflow-selected paths instead.

### Branch-only build keys

Rejected because toolchain, flags, lockfiles, source, and architecture can change without the branch name changing.

### Share mutable caches across tenants or trust domains

Rejected because cache poisoning can turn a normal trusted build into code execution supplied by another tenant or an untrusted pull request.

### Keep uploads/downloads in `Vec<u8>`

Rejected because server memory becomes proportional to cache size and concurrent transfers multiply the problem. Stream every data path.

### Cache mutable action tags forever

Rejected because `v4` and branches may move. Cache immutable commit/archive bytes indefinitely under capacity, but refresh the mutable resolution record.

### Add one virtiofs mount per ecosystem cache

Rejected by measured fork behavior. Every golden mount is inherited and paid by every clone.

### Cache secrets with encryption and call it safe

Rejected. Encryption reduces storage disclosure risk but does not make cross-job replay of credentials correct. Secret-bearing state remains job-scoped and ephemeral.

## STOP conditions for implementation

Stop and revisit this strategy rather than improvising if:

- official `actions/cache@v4` behavior requires a different exact/prefix/version order than `CacheStore` currently models;
- a proposed shared cache cannot prove its trust boundary and producer identity;
- a cache hit changes job output compared with a clean miss;
- implementing persistence requires exposing host paths directly to untrusted jobs;
- a performance improvement depends on undeclared tools or stale workspace files;
- storage deduplication exposes cross-tenant existence or timing information;
- cache save/restore costs more than the origin operation for the target workload;
- the SmolVM transport requires another inherited mount and the fork regression has not been re-benchmarked.

## Maintenance and review checklist

For every new cache class or key revision, reviewers must answer:

1. What exact computation/download is avoided?
2. What are all correctness inputs and where are they represented in the key?
3. Who may write it? Who may read it?
4. Can an untrusted producer affect a trusted consumer?
5. What verifies content integrity?
6. Where does it live in local, self-hosted, and managed profiles?
7. What happens on miss, corruption, backend outage, and partial upload?
8. How is it bounded, expired, evicted, and deleted?
9. What telemetry proves it saves time and bytes?
10. Does it preserve behavior against the official runner and GitHub-compatible workflow semantics?
