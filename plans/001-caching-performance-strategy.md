# Plan 001: Caching strategy for local and self-hosted CI

> **Status**: Proposed architecture and implementation roadmap
>
> **Scope**: Local CI and self-hosted CI, production grade. Managed multi-tenant is deferred.
> The architecture is designed to extend to managed deployments, but implementation targets
> `~/.preloop/cache/v1` (local) and `/var/lib/preloop/cache/v1` (self-hosted systemd deployment).
>
> **Priority**: P1
> **Category**: performance, correctness, architecture
> **Planned at**: commit `1342346a`, 2026-07-27
> **Revised at**: commit `d7f495d5`, 2026-08-02 — scope widened from local-only to
> local + self-hosted production grade; ecosystem download payload cache added as the
> primary toolchain acceleration mechanism (replacing resolver-gated golden baking).
> **Drift check before implementation**: `git diff --stat d7f495d5..HEAD -- crates/preloop-cache crates/preloop-runner-server crates/preloop-runner crates/preloop-orchestrator crates/preloop-vm docs`

## Executive decision

Preloop should not build one undifferentiated cache. It should build a layered cache system with four properties:

1. **Immutable content-addressed blobs** hold bytes once and are addressed by a verified digest.
2. **Scoped metadata** maps workflow-visible keys to those blobs.
3. **Guest-local materialization** makes hot content available to a runner without making a shared writable filesystem part of job correctness.
4. **A shared ecosystem download payload cache** caches the bytes the *tools themselves* fetch — registry metadata and version-stamped tarballs — keyed by origin URL, shared globally across all workspaces, jobs, and virtual machine images.

Property 4 is the deliberate alternative to GitHub's everything-image and to per-environment golden baking. Images cache inputs at the wrong granularity (you pay for all inputs for all users). Per-workspace goldens cache inputs in per-repo VM snapshots and depend on a resolver that can only see a narrow slice of workflows. Caching payloads by URL is universal (any workflow, unmodified — the drop-in compatibility promise) and amortizes one origin fetch across every job on the host.

**Storage layout:**

| Layer | Local profile | Self-hosted profile |
|---|---|---|
| Durable metadata | SQLite under `~/.preloop/cache/v1` | SQLite (WAL) under `/var/lib/preloop/cache/v1`; Postgres optional for multi-node |
| Durable blobs | Content-addressed files under `~/.preloop/cache/v1/blobs/` | Content-addressed files under `/var/lib/preloop/cache/v1/blobs/` |
| Payload cache | Same blob store, URL-keyed | Same blob store, URL-keyed; LAN-facing HTTPS listener with deployment-pinned CA |
| Hot tier | Golden/COW disk and host page cache | Golden/COW disk and host page cache; optional node-local read-through tier in multi-node |

The same logical model extends to managed deployments (object store + Postgres + KMS) in a future phase. Design interfaces for it; do not build it now.

### Evidence

Measured before this plan (2026-07-27, five-repository benchmark):

- Corrected Vite warm: Preloop `92.45 s`, Agent CI `38.71 s`.
- Actual build plus lint: Preloop `9.91 s`, Agent CI `10.40 s`.
- Preloop spent `34.24 s` in `setup-node`/cache handling, `16.50 s` in `pnpm install`, and `28.31 s` in the post-job cache save.
- The cache restore emitted `File name too long`, so dependency installation ran on a cache miss.
- Cold time before the first guest step was `48.54 s` for ripgrep, `545.84 s` for Vite, and `18.59 s` for testcontainers-go. That is VM/image preparation, not workload execution.

Measured after 2026-08-02 production changes (self-hosted host `main`):

- Packed golden fork pool is live: one 716 MB zstd-compressed artifact serves a forkable golden; two runners provision end-to-end in under a minute including configure, and the fork step itself is near-instant (CoW). No per-runner `apt`+`rustup` wait.
- Guest→host TCP now works (smolvm `1.7.2`, virtio-net fix) and is already the control-plane transport (`PRELOOP_CONTROL_UPSTREAM=http://10.0.0.161:9090`). **This is the payload cache's transport prerequisite**: guests can now reach LAN-addressed cache endpoints on the host.
- Toolchain baking (rustup into the golden) works — verified `cargo 1.97.1`/`rustc 1.97.1` on PATH in a CI run on 2026-08-02 — but only because `rust-toolchain.toml` was statically resolvable. It remains an opt-in accelerator, not the general mechanism.

Revised priority order:

1. Make Actions cache restore/save correct, indexed, and streaming (still the largest measured loss: 62 s of the 92 s Vite warm run).
2. Make declared toolchains discoverable in the official toolcache layout.
3. Build the shared download payload cache with config injection into the base image (Phase 2).
4. Resolve and cache immutable action/image/tool artifacts by digest.
5. Move VM preparation out of the run critical path (packed fork pool already shipped; keep refining).

## Goals

- Preserve official-runner and unmodified-workflow compatibility.
- Cover **all** workflows — not only statically resolvable ones — by caching tool payloads, not declaring environments.
- Make a cache miss equivalent to normal uncached execution, except when a workflow explicitly requests `fail-on-cache-miss`.
- Eliminate repeated downloads and archive work that dominates warm jobs.
- Keep process memory usage proportional to a transfer chunk, not cache size.
- Bound disk growth with quotas, expiration, admission, and eviction.
- Give operators enough telemetry to explain every hit, miss, restore, save, and eviction.
- Self-hosted production grade: authenticated cache access on non-loopback listeners, TLS for guest→cache traffic, durable-on-restart state, explicit ops controls (status/prune/purge), and documented backup posture.
- Design metadata/blob interfaces that extend to multi-node and managed deployments later.

## Non-goals

- Making local CI pass because an undeclared tool happens to exist. Workflows must remain portable to GitHub Actions. Toolchain baking into goldens is an opt-in accelerator, never a substitute for `setup-*` declarations.
- Treating the environment resolver as a correctness mechanism. It accelerates statically detectable declarations only; dynamic versions, matrices, forks, and `curl`-installed toolchains must all work without it.
- Treating artifacts or logs as disposable caches. They are user-visible outputs with separate retention semantics.
- Reusing complete workspaces across jobs. Workspaces remain disposable unless a workflow explicitly saves paths through the cache protocol.
- Sharing mutable build caches across security boundaries.
- Replacing package-manager integrity checks or OCI digest verification.
- Guaranteeing that a cache survives eviction. A job must remain correct on a miss.
- Running a transparent HTTPS-intercept (MITM) egress cache by default. Mirror insertion is per-tool configuration against an allowlisted origin set; there is no transparent interception of arbitrary guest egress.

## Current state and evidence

### Actions cache store

`crates/preloop-cache/src/lib.rs` implements a local file-backed `CacheStore`:

- cache identity is a SHA-256-derived directory;
- original key, version, and creation time are stored as metadata;
- entries are immutable;
- restore order supports exact key and prefix matching;
- prefix lookup scans every cache directory;
- complete archives are read into `Vec<u8>` on restore;
- there is no quota, expiration, eviction policy, or indexed lookup.

`crates/preloop-runner-server/src/models.rs` stores v1 uploads in `PendingCache.bytes`.

`crates/preloop-runner-server/src/blob_store.rs` stages v2 blocks on disk, then:

- assembles all blocks in a single `Vec<u8>`;
- writes a second complete copy;
- reads complete blobs to return downloads.

`crates/preloop-runner-server/src/results_twirp.rs` reads the complete staged blob during finalize and passes complete bytes to `CacheStore::put`. This is acceptable for small local caches but cannot be the self-hosted data path under multi-gigabyte transfers.

### Cache scoping and authorization

`results_twirp::scoped_cache_key` currently combines workflow-requested `repository` and `scope` fields. Self-hosted isolation cannot trust these values: forked-PR jobs could poison or read base-branch caches by forging request fields. The server must derive repository scope and ref trust class from authenticated server state (job record), not from client payloads. The runtime token proves an `Actions.Results:<plan>:<job>` scope; the cache API needs that resolved authorization context enforced in Phase 3. Download tokens are stored in memory and should become expiring, operation-specific capabilities.

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

PATH in guests: step shells run `bash --noprofile --norc`, so `/etc/profile.d/*` is never sourced. Guest PATH comes from `GITHUB_PATH` (which setup actions write) or from absolute install locations (`/usr/local/bin`). Bake-time PATH exports in shell profiles are silently inert — this already bit the rustup bake (symlinks to `/usr/local/bin` were the fix). Toolcache materialization is the correct mechanism for declared tools, because setup actions consume it directly.

### Golden VMs and images

`crates/preloop-orchestrator/src/environment.rs` computes an environment fingerprint from a base-image string and declared toolchain layers. A mutable tag is not a durable compatibility identity; the fingerprint must ultimately include the resolved base-image digest.

Declared service/container images are preloaded into a golden and inherited by COW forks. Keeping preload images outside the environment compatibility fingerprint is reasonable because they are an optimization, not semantic environment state. Record them as a **coverage set** on the golden and select an existing compatible golden whose coverage is a superset; do not rebuild a golden solely because an optional preload set differs.

`docs/preloop-performance-engineering.md` records that an additional virtiofs cache mount caused 20–50 second fork stalls and instability. The cache design must not add one mount per ecosystem or per cache. Prefer COW image contents, one stable existing transport, or HTTP/cache-service transfer.

Toolchain baking status (2026-08-02): `prepare_artifact` installs workspace-resolved toolchains into the packed artifact. This stays as an opt-in accelerator (`PRELOOP_BAKE_TOOLCHAINS`, to be added, default off once Phase 2 lands) because it only accelerates statically resolvable declarations. The payload cache is the default-on mechanism.

### Other useful caches

- `crates/preloop-runner-server/src/actions.rs` streams action tarballs into an atomic on-disk cache keyed by owner/repository/ref. Mutable tags are cached indefinitely; resolve tags to commit SHA and store immutable content by digest.
- `crates/preloop-runner-server/src/snapshots.rs` maintains a lock-protected Git object cache per local Git common-directory identity and persists stat data. Its documented `git add` result improves from `156 ms` to `16 ms` for a 6,000-file workspace. This is the right pattern: immutable objects, narrow identity, atomic refresh, and no workspace reuse.
- `crates/preloop-orchestrator/src/keys.rs` deliberately gives every runner a unique RSA key. Runner private keys, credentials, and secret-bearing state must never enter a shared cache or golden image.

### No payload cache today

There is currently no cache for the bytes `setup-*` actions and package managers download. Every ephemeral runner re-downloads rustup dists, npm tarballs, Go modules, and pip wheels from the internet. On a forked-runner pool this dominates job time and burns egress. The transport prerequisite (guest→host TCP) is now in place (smolvm `1.7.2`); the cache itself does not exist yet.

## Cache taxonomy

Each class has different identity, mutability, trust, and placement rules.

| Class | Examples | Cache? | Scope | Preferred placement |
|---|---|---|---|---|
| Runner internals | Node 20/24 used to execute JS actions | Yes, immutable | Preloop release + OS + arch | Base/golden image under `externals/` |
| Declared toolchains | Node, Python, Go, Java, Rust, .NET selected by `setup-*` | Yes, declared-only | Exact version + platform + toolcache format | Host tool CAS; materialize into `$RUNNER_TOOL_CACHE` (opt-in: golden bake) |
| Ecosystem download payloads | rustup dists, npm metadata/tarballs, cargo index/.crate, Go module zips, pip wheels, apt debs, Node runtime, GitHub release assets | Yes | Exact URL (immutable class); registry metadata (mutable class, TTL) | Host payload cache; guests fetch through it |
| Action source | `actions/checkout`, `setup-node`, third-party actions | Yes, immutable after resolution | Owner/repo/commit SHA | Shared verified action CAS |
| OCI images/layers | base VM input, job containers, services | Yes, immutable | Registry/repository/digest/platform | Golden preload (already shipped); registry mirror optional later |
| Package download stores | pnpm store, npm cache, Cargo registry/db, Go module cache, uv wheels | Yes | Ecosystem integrity identity + platform where needed | Persistent host store; served to guests through the payload cache |
| Compiler/build cache | sccache objects, Go build cache, Vite transform cache | Yes, cautiously | Source/config/toolchain/target/features | Repository/trust-scoped cache |
| Actions cache archives | User paths saved through `actions/cache` or setup actions | Yes | Server-derived namespace + workflow key/version | Indexed metadata + blob CAS |
| Git snapshot objects | Immutable Git objects and stat index | Yes | Local Git common-dir identity or repository identity | Host-local persistent store |
| Workspace | Checked-out source and arbitrary generated files | No implicit reuse | One job/run | Disposable guest disk |
| Artifacts and logs | Build deliverables, test reports, logs | Store, but not as cache | Run/job retention policy | Artifact/log store |
| Secrets and identity | tokens, runner keys, `.credentials`, `.npmrc`, cloud config | Never shared-cache | One job or runner | Secret channel and ephemeral memory/disk only |

## Ecosystem download payload cache

This is the mechanism that replaces per-environment toolchain baking as the default acceleration path. It is a read-only reverse proxy over a fixed allowlist of upstream origins, fronted by the shared blob CAS, serving every guest on the host — regardless of workspace, workflow shape, or how a toolchain was requested.

### Why URL-keyed bytes, not resolved environments

- Setup actions, matrices, expression inputs, forked or custom actions, and `curl`-installed toolchains all compute download URLs at runtime with full context. The resolver can never see many of these; the URL fetch is always correct.
- Version-stamped URLs (`/dist/v22.14.0/node-v22.14.0-linux-x64.tar.xz`, rustup dists, Go module zips, versioned wheels) are effectively immutable and content-verify independently, so they are ideal cache citizens.
- One origin fetch amortizes across every VM on the host: 100 rustup builds download Rust from `static.rust-lang.org` exactly once.
- Nothing in job correctness depends on the cache. Every miss is a normal download; every hit is the same bytes the origin would have sent.

### Architecture

```text
guest tool (rustup, npm, cargo, go, pip, apt, setup-* actions)
  │  GET https://<cache-host>:<cache-port>/<row-id>/<origin path>
  ▼
preloop-server payload cache listener (HTTPS, allowlisted rows only, read-only)
  ├─ blob present?  → stream from CAS (hit)
  └─ miss → singleflight coalesce → fetch origin once →
            tee stream(a) to guest, (b) to staging file →
            verify rules → atomic publish into CAS (tmp + rename)
```

The cache is a **dumb byte store**: it relays responses verbatim and never re-serializes payloads. All integrity stays where ecosystems already put it — npm/Cargo registry checksums and lockfile integrity fields, Go checksum database, rustup SHA256 manifests, pip hashes, apt `InRelease` signatures. A corrupt cache entry produces exactly the failure a corrupt origin would, and can be purged and refetched.

### Upstream policy table

One row per origin class, in one config file. Adding an ecosystem is one row + tests.

| Row | Origin | Allowlisted path classes | Mutability/TTL | Injected guest config |
|---|---|---|---|---|
| rustup | `static.rust-lang.org` | `/dist/**`, `/rustup/**` | immutable ∞ (versioned); channel manifests TTL 10 min | `RUSTUP_DIST_SERVER`, `RUSTUP_UPDATE_ROOT` |
| cargo | `index.crates.io`, `static.crates.io` | sparse index files; `/crates/**` | index TTL 5 min; `.crate` ∞ | `[source.crates-io]` replacement in `$CARGO_HOME/config.toml` |
| npm | `registry.npmjs.org` | `/{pkg}` metadata; `/{pkg}/-/*.tgz` | metadata TTL 5 min + ETag revalidate; tarballs ∞ | global `registry=` npmrc |
| Go modules | `proxy.golang.org` | `/{mod}/@v/{ver}.*` (immutable); `@v/list`, `@latest` (TTL 15 min) | module files ∞; list/latest TTL 15 min | `GOPROXY` (keep `,direct` fallback) |
| pip | `files.pythonhosted.org`, `pypi.org/simple` | wheel/sdist URLs are hash-laden ∞; simple index TTL 5 min + revalidate | wheels ∞; simple index TTL 5 min | `/etc/pip.conf` / `PIP_INDEX_URL` |
| apt | distro mirrors | `pool/**.deb` immutable; `dists/**` TTL 1 h | optional row; guests use golden-baked baselines first |
| Node runtime | `nodejs.org` | `/dist/v*/**` immutable | only when using payload downloads directly; `$RUNNER_TOOL_CACHE` materialization preferred |
| GitHub release assets | `github.com`, `objects.githubusercontent.com` | release download URLs | tag resolution records TTL 1 h; versioned archives ∞ | optional |

Rules for all rows:

- GET/HEAD only. No POST/PUT. No cookies forwarded. No client-controlled URL paths off the allowlist (hard anti-SSRF boundary).
- Response byte cap per class (default 1 GiB; apt/deb 4 GiB) to avoid disk bombs.
- HTTP error responses are cached negatively for 60 s, never positively.
- Range requests supported where the client sends them; cached under the object's own integrity only after a complete body is present.

### Transport and TLS

Guests reach the cache over the same NAT TCP path as the control plane (smolvm ≥ 1.7.2 required; virtio-net guest→host fix). Two listener rules:

- **Same daemon as the control plane.** Cache liveness must equal control-plane liveness, because some tools cannot fall back (npm, cargo, rustup hard-fail if their configured endpoint is down; Go falls back only via `GOPROXY=…,direct`).
- **Self-hosted profile: dedicated interface + TLS.** Bind `PRELOOP_CACHE_LISTEN` (default: control-upstream interface, port `9091`) and serve TLS with a per-deployment CA generated at first start under `$PRELOOP_HOME/cache/ca/`. The CA is injected into the golden's system trust store and per-tool stores (`NODE_EXTRA_CA_CERTS`, `SSL_CERT_FILE`, `CARGO_HTTP_CAINFO`). This is a *pinned, single-endpoint* CA — not general egress MITM. Plain-HTTP would require per-tool insecure flags (`GOINSECURE`, cargo registry HTTP rules) and gives no integrity for metadata rows, so it is rejected.
- **Local profile: plain HTTP to the host cache listener is acceptable**; the guest reaches it over the control-plane NAT path (e.g. the virtio-net gateway address), never the guest's own loopback — `127.0.0.1` inside the guest points at the guest, not the host. The same CA path is recommended to keep guest images identical across profiles.
- CA rotation requires golden rebuild; the fingerprint includes the deployment CA digest so goldens invalidate deliberately.
- **Auth**: on a non-loopback listener, `/cache/*` requires the runtime/job capability (Phase 3); loopback local profile requires none. The public tunnel address must never expose cache routes.

### Failure behavior by tool

| Tool | If cache is down | Design response |
|---|---|---|
| go | `GOPROXY=cache,direct` falls through automatically | keep `,direct`; log fallback |
| rustup / cargo / npm / pip | hard failure of the configured endpoint | cache runs **in the control-plane process** (same liveness); provision-time health gate refuses to start runners with an unhealthy cache; ops runbook entry |
| apt | hard failure of configured mirror | keep in-guest default sources as fallback or rely on golden-baked baseline |

Per-operation latency budget: origin fetch timeout 300 s; on timeout the guest sees the same failure an origin timeout would. No minutes-long backoff loops on the job path (correctness rule 11).

### Performance requirements

- **Singleflight**: concurrent cold fetches of one URL (8-runner pool, cold cache) trigger exactly one origin fetch; others attach to the stream.
- **Tee streaming**: response body streams to guest *while* written to a staging file; no full-file memory ever.
- **Atomic publication**: staging tmp file, size/length verification, `rename` into CAS; readers never observe partial files.
- **LRU eviction by bytes** with a soft (20 GiB default) and hard quota; in-flight or read-active entries are never evicted.
- **Reuse the seam**: payload blobs live in the same `BlobStore` CAS as cache archives; TTL/mutability classes live in `CacheMetadataStore`. Don't fork storage.
- Expected results: warm rustup toolchain install drops to LAN disk speed; `npm ci` on a warm registry mirror bounded by lockfile work, not origin bandwidth.

## Cache key design

### General rule

A key must include every input that can change the cached output. If that set cannot be identified, do not cache the output.

Use canonical serialization followed by a hash for storage identity. Keep human-readable components in metadata and telemetry.

Logical namespace:

```text
schema_version
+ tenant_id            (fixed local identity in local profile; deployment identity
                        + repository scope in self-hosted profile)
+ repository_id
+ trust_domain
+ cache_class
+ workflow-visible key/version
```

Compatibility inputs then vary by class.

### Trust domain

**Local profile**: one trust domain — the developer's OS account. Field default is a fixed local value.

**Self-hosted profile**: trust domains exist. Base branches/tags and fork pull requests are not mutually writable: fork-PR jobs may read base-ref caches (official GitHub behavior) but must never write into base-ref namespaces. Trust class is derived server-side from the job record (event type, ref, base ref), never from client-supplied request fields.

### Key recipes

#### Toolchains

```text
tool/v1/<tool>/<exact-version>/<os>/<arch>/<libc>/<toolcache-format>/<upstream-sha256>
```

Include exact version, not only major version. A workflow may request `24`, but the resolved cache entry should identify `24.18.0` and maintain a separately expiring major-to-exact resolution record.

#### Download payloads

```text
payload/v1/<origin-host>/<normalized-path>/<query-hash>
```

Two mutability classes ride on the same storage identity:

- **immutable** (version-stamped URLs): near-infinite TTL, LRU under capacity. Purge on caveat: operator `preloop cache purge <url>` after a verified-corrupt fetch.
- **mutable** (registry metadata, channel manifests, `@latest`/`@v/list`): short TTL (5–15 min) with ETag/`If-Modified-Since` revalidation; stale-while-revalidate for no more than one TTL period under origin outage, then fail-fresh.

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

Prefer the ecosystem's verified content store over archiving an installed dependency tree:

```text
package/v1/<ecosystem>/<content-integrity>/<platform-if-native>
```

Examples:

- pnpm/npm: package integrity digest; share verified tarballs/store objects, not arbitrary `node_modules`.
- Cargo registry: crate checksum; Cargo Git DB by repository and commit.
- Go modules: module path/version/checksum; build cache remains platform/toolchain-specific.
- uv/pip: wheel/sdist hash, Python ABI, platform tag for native wheels.

A package store can safely benefit many lockfiles because the package manager verifies content. A hydrated dependency tree is more fragile and requires a stricter repository-specific key. **When the payload cache covers the same registries, prefer the payload cache and set stable in-guest store paths; don't build a second store-hydration layer.**

#### Actions cache archive for dependencies

The workflow supplies the primary key, restore keys, and cache version. The server prepends its private namespace. Recommended workflow key ingredients:

```text
<os>-<arch>-<package-manager-major>-<runtime-abi>-<lockfile-hash>
```

Do not silently rewrite the workflow's visible key because restore behavior is user-facing. Namespace it server-side and expose diagnostics showing both the visible key digest and the server scope.

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
          <baseline-packages-hash>/<deployment-ca-digest>
```

Optimization metadata, not compatibility identity:

```text
preloaded OCI digest set
action digest set
last used
build cost
```

Select a compatible golden with a useful preload superset. Do not create a new 249-second golden merely to save a 4–9-second optional image pull. Golden chain depth: at most base → optional workspace-baked toolchains. Do not build a general N-deep fork-and-extend layering tree; the payload cache makes combinatorial environment layering unnecessary, and fork trees multiply golden build/maintenance cost.

#### Git snapshot objects

Local CI may continue keying by canonical Git common-directory identity because it identifies the developer’s repository object store. Self-hosted systems use repository identity plus immutable commit/object IDs and keep working-tree stat indexes node-local. Never share a mutable working-tree index across repositories.

## Placement and data flow

### L0: process-local coordination

Cache only small metadata:

- in-flight reservation deduplication;
- singleflight deduplication for payload origin fetches;
- short negative cache for failed mutable-ref resolutions;
- recently resolved metadata entries.

Do not retain archive bytes in process memory. Every L0 entry needs a short TTL and bounded size.

### L1: guest-local ephemeral state

Use for active job extraction, package-manager working state, and compiler scratch data. It disappears with the runner. The guest may read immutable content inherited from a golden, but arbitrary job writes must stay in its COW layer. In-the-guest package-manager stores (`~/.npm`, `~/.cargo/registry`) stay ephemeral — persistence comes from the payload cache the package managers fetch through, not from preserved guest disks.

### L2: node-local persistent CAS

Use for hot tools, actions, package objects, OCI layers, payload bodies, and recently restored cache archives. It provides high throughput without adding one VM mount per cache. Population is atomic and lock-protected.

A node-local entry is an optimization. Its loss must fall back to L3 or origin.

### L3: durable blob and metadata store

- Local: host filesystem plus SQLite.
- Self-hosted single-node: persistent disk under `/var/lib/preloop`, SQLite WAL. Postgres + S3-compatible store optional for multi-node.
- Managed (future): object storage plus Postgres, with regional placement and KMS encryption.

The runner accesses user `actions/cache` entries through the protocol, not a shared writable filesystem. BYO self-hosted runners cannot be assumed to share a mount with the control plane; remote runners use `actions/cache` archives or ecosystem-native remote caches.

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
├── payload/
└── metrics/
```

Policy:

- write-through to local disk;
- one cache root, not one virtiofs mount per cache class;
- payload cache listener on loopback only;
- seed declared toolchains into `$RUNNER_TOOL_CACHE` where resolvable;
- keep runner internals in `externals/`, separate from declared workflow tools;
- retain the Git snapshot object cache;
- preload declared service/container images by digest;
- resolve action tags but store archives by commit and digest;
- default starting quota: 20 GiB soft target and configurable hard limit; expose `preloop cache status`, `preloop cache prune`, and `preloop cache purge` before enabling automatic destructive pruning;
- encryption at rest optional (OS account is the trust boundary); permissions must be user-only; secrets remain forbidden.

### Self-hosted CI (production)

Assumptions:

- systemd deployment (e.g., `/var/lib/preloop`, `/opt/preloop/current`);
- control plane reachable by guests over LAN (`PRELOOP_CONTROL_UPSTREAM`);
- multiple repositories; fork-PR jobs may be untrusted;
- availability of `preloop` service == availability of cache (single daemon);
- operators need quota enforcement, audit logs, and a rebuildable-not-backed-up cache.

Layout: `/var/lib/preloop/cache/v1/` with the same substructure as the local profile.

Production requirements:

1. **Same-process cache** — the listener runs inside `preloop serve`; no separate service to drift out of sync with runner provisioning.
2. **TLS with deployment CA** for guest→cache traffic; CA persisted under `$PRELOOP_HOME/cache/ca/`; rotation rebuilds goldens via fingerprint.
3. **Auth boundary** — cache and payload routes require the job capability on non-loopback listeners. Public ingress (Cloudflare tunnel) routes *never* include the cache.
4. **Namespaces derived server-side** — repository scope and ref trust class come from the job record; fork PRs read base-ref caches and write only PR-scoped ones.
5. **Durability** — SQLite WAL mode; finalize is atomic (blob CAS publish + metadata commit); crash-reconcilable orphans (staging older than 24 h is reclaimed).
6. **Quota enforcement** — background eviction loop maintains soft quota; writes never fail for quota without an eviction attempt first.
7. **Ops surface** — `preloop cache status|prune|purge`, structured access logs per row, and a health endpoint consumed by the runner provision gate.
8. **Backup posture** — the cache is *rebuildable*: excluded from backup. Artifacts and logs are not caches and retain separate retention/backup policy.
9. **Ingress sizing** — cache listener serves LAN only; no per-request Cloudflare roundtrips (matches the existing `PRELOOP_CONTROL_UPSTREAM` LAN decision).

### Managed CI (future)

Regional Postgres with row-level authorization. Object storage with KMS envelope encryption. Short-lived signed URLs (5–15 minutes) with direct object-store transfers so archive bytes stay off control-plane instances. Regional NVMe hot tiers. Immutable global cache only for public tools/actions/OCI layers/payloads. Per-tenant rate limits, quotas, cost attribution, and complete audit logs.

## Security design

### Trust boundaries by profile

- **Local**: the developer's OS account. One trust domain. Deduplication across repos on one machine is a win, not a risk.
- **Self-hosted**: the host OS account is trusted; *jobs are semi-trusted* (workflow code is arbitrary, fork PRs are adversarial inputs). Physical byte deduplication of **public immutable content** (payloads, actions, OCI layers, tools) is cross-repository-safe *only because* consumers verify integrity (npm integrity fields, cargo checksums, Go sumdb, rustup manifests, OCI digests). Logical authorization stays scoped per repository/trust domain.

### Never cache (any profile)

- Runner RSA private keys or shared runner credentials;
- OAuth, GitHub, cloud, package-registry, or signing tokens;
- `.runner`, `.credentials`, `.credentials_rsaparams`;
- `.npmrc`, `.pypirc`, `.netrc`, Docker auth config, cloud CLI credential directories;
- secret-bearing `$GITHUB_ENV`, `$GITHUB_OUTPUT`, process environments, or debug state;
- decrypted job messages;
- arbitrary home directories.

Path deny-lists are defense in depth. The primary controls are narrow cache paths and no implicit whole-workspace caching.

### Payload cache threat controls

- **SSRF / egress abuse**: guests cannot make the server fetch arbitrary URLs. The origin allowlist is hardcoded (policy table), path prefixes are validated, and there is no client-supplied upstream.
- **Open-proxy abuse on public interfaces**: cache routes require the job capability on non-loopback; public tunnel ingress never mounts them.
- **Cache poisoning**: mitigated by design — the cache never interprets payloads; bytes are relayed verbatim; ecosystems verify integrity client-side. Known-digest verification for rows where the origin publishes digests is done at finalize when cheap (e.g., rustup SHA256 manifest), not as a general requirement.
- **Denial via cache**: response size caps, per-row quota, LRU bound; a guest cannot balloon the store past hard quota.
- **Trust model**: the deployment CA cert is pinned to cache endpoints in guest config, not an egress-intercepting root. Document that clearly: jobs running in guests trust the same deployment they already trust for the control plane.
- **Mutable-tag squatting**: short TTLs + revalidation; immutable content stored by resolved identity.

### Deduplication

Per profile: local — full physical dedup. Self-hosted — dedup only for the public immutable classes (payloads, actions, layers, tools); user `actions/cache` archives dedup within a repository scope only.

### Capability URLs

- **Local**: not needed, loopback.
- **Self-hosted**: short-lived, operation-specific capabilities (expiring, read-only for downloads) issued after server-side authorization (Phase 3). Signed URLs on object stores are a managed-phase concern.

## Correctness rules

1. **Misses are normal.** Backend failure should degrade to a miss/warning when official cache semantics allow it, not fail unrelated job work.
2. **Explicit failure remains explicit.** Preserve `fail-on-cache-miss` and action-defined failure behavior.
3. **Immutable publication.** Write staging, verify, atomically publish; never mutate a finalized key/version.
4. **No partial reads.** Readers see pending or complete, never both.
5. **Version archive semantics.** Compression method and cached path set contribute to the cache version so incompatible archives do not match.
6. **Safe restore keys.** Prefix matches follow official order and remain within the authenticated namespace.
7. **Reproducibility first.** Preload only declared toolchains/containers. A faster local pass that would fail on GitHub is incorrect; baking is opt-in, never a substitute for declarations.
8. **Resolved identities.** Mutable labels are lookup hints, not immutable storage identities.
9. **Crash recovery.** Finalization and metadata/blob reference updates are transactional or restart-reconcilable.
10. **Clock independence.** Ordering uses server timestamps or monotonic sequence IDs; clients do not choose creation time.
11. **Bounded retries.** Cache outages cannot add minutes of exponential backoff to a job. Set per-operation latency budgets and fall back where allowed.
12. **Isolation before deduplication.** Physical byte reuse must not change logical authorization.
13. **PATH is `GITHUB_PATH`.** Step shells run `bash --noprofile --norc`; shell profile hooks never execute. Tools for steps come from `GITHUB_PATH` or absolute locations (`/usr/local/bin`). Profile-file PATH exports in goldens are silently inert bugs.
14. **Byte-relay equivalence.** A payload cache hit returns byte-identical content to the origin response for immutable classes; metadata classes obey TTL/revalidation, not invisibility. If it cannot be made byte-equivalent, it is not cached.
15. **Corruption is recoverable.** A verified-corrupt entry is purgeable (`preloop cache purge`), and the next fetch repopulates from origin. Corruption must never require reinstalling the deployment.

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
- always admit immutable payload bodies under size cap (their origin URL is versioned; re-fetch cost is high and value is proven by being requested);
- admit package content objects after integrity verification;
- reject or short-retain oversized one-off workflow caches unless explicitly allowed;
- coalesce concurrent uploads and origin fetches for the same immutable identity (singleflight);
- use historical workflow manifests to prefetch only repeatedly used declared content.

Eviction policy:

1. Never evict active uploads, in-flight payload fetches, or blobs with active readers.
2. Expire logical entries first.
3. Prefer evicting unreferenced, old, low-hit, cheap-to-recreate bytes.
4. Bias toward retaining high-download-cost tools/images/payloads and frequently restored dependency caches.
5. Garbage-collect a physical blob only after no metadata references it and a grace period has elapsed.
6. Keep soft and hard quotas separate so background eviction runs before writes must be rejected.

Starting retention classes:

| Class | Starting policy | Reason |
|---|---|---|
| Mutable tag/ref resolution | minutes to hours | Must observe upstream movement |
| Mutable payload metadata | 5–15 min + revalidation; stale-while-revalidate one TTL | Registry correctness under movement |
| Immutable payload body | LRU under capacity; long TTL | Versioned URL; expensive to re-fetch |
| Immutable action/tool/image blob | LRU under capacity; long TTL | Verified and expensive to redownload |
| Package content store | LRU under capacity | Content-addressed and broadly reusable |
| Branch dependency cache | 7–30 days since last access | Useful during active development |
| PR/fork writable cache | PR lifetime plus short grace | Limits untrusted accumulation |
| Build output cache | shorter TTL, cost-aware admission | Large and sensitive to weak keys |
| Staging upload / orphaned payload staging | hours, never days | Incomplete and not restorable |
| Negative lookup | seconds to minutes | Avoids hiding newly published content |

These are policy starting points. Ship telemetry and operator controls before fixing service-wide constants.

## Performance techniques by layer

### Toolchains

- Maintain a host-side immutable tool distribution CAS.
- Materialize only toolchains explicitly declared by parsed setup actions/version files, in the official `$RUNNER_TOOL_CACHE` layout, with completion markers written after verification.
- Bake toolchains into goldens only behind `PRELOOP_BAKE_TOOLCHAINS` (default off once the payload cache ships); never bake undeclared environments.
- Let everything not resolvable fall through setup actions to the payload cache. This is stronger than resolver breadth: matrices, expression inputs, forks, and `curl` installs all work uniformly.
- Prefer reflink/hardlink/COW materialization where filesystem and ownership rules permit; otherwise copy from local CAS, never redownload.
- Keep the internal JavaScript action runtime separate from workflow-selected Node.

### Package managers

- Set stable package-store locations, not action-installation-relative paths.
- Cache package content, not arbitrary home directories.
- Let pnpm/npm/Cargo/Go/uv perform their integrity checks.
- Fetch through the payload cache (config injection in the golden, per policy table) instead of archiving huge stores on every job.
- For remote/BYO runners, use `actions/cache` archives or ecosystem-native remote caches; do not assume host mounts.

### Download payload cache

- Singleflight origin fetch per URL; stream to guest and disk concurrently (tee).
- Atomic publish from staging; no partial observations.
- Per-listener backpressure; Range passthrough.
- Coalesce identical concurrent requests across *all* guests, not per-VM.
- Instrument hit/miss/bytes/RPS per origin row in metrics.

### Actions cache protocol

- Replace directory scans with indexed exact/prefix lookup.
- Stream archive bytes end to end.
- Allow local durable spool plus asynchronous remote replication only after documenting the durability tradeoff. A successful finalize must mean “durable enough for this profile.”
- Avoid recompressing an already compressed cache archive.
- Keep post-job cache work visible in step telemetry.

### VM and OCI preparation

- Packed-golden fork pool is the VM fast path (shipped 2026-08-02): one artifact → forkable golden → CoW forks. Keep toolchains out of the default golden; preload stays image/`-cache`-oriented.
- Resolve base images to digests before fingerprinting.
- Requires smolvm ≥ 1.7.2 (virtio-net guest→host connectivity) for both control and payload traffic; gate on version at startup.
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
cache.origin_host        # payload rows
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

Do not log secret-bearing raw keys or signed URLs. Hash keys for correlation and expose raw workflow keys only in appropriately authorized diagnostics.

Job UI/logs should make the critical path obvious:

```text
setup-node: toolcache exact hit, Node 24.18.0, 180 ms
pnpm cache: prefix miss, reason=not_found, lookup 12 ms
pypi: payload hit, numpy-2.2.2-cp312-manylinux_2_17_x86_64.whl, 41 ms (origin 900 ms saved)
pnpm install: 16.5 s
cache save: 1.2 GiB, archive 8.1 s, upload 3.4 s
```

Dashboards:

- hit ratio and byte-hit ratio by class/profile/repository/origin row;
- time saved versus save/restore cost;
- p50/p95 lookup, first-byte, transfer, archive, and extract latency;
- cache-service errors and fallback-to-miss rate;
- payload singleflight coalescing ratio (requests ÷ origin fetches);
- logical versus physical bytes and deduplication ratio;
- eviction churn and rejected writes;
- fork-trust access denials;
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
- repository scope cannot be forged through request fields (server-derived namespace);
- expired/replayed capability URL (self-hosted profile);
- multi-gigabyte transfer with bounded process RSS;
- backend outage degrades according to action semantics.

Payload-specific:

- two concurrent cold guests fetch the same URL → exactly one origin fetch, both receive identical bytes;
- byte-identity: cached body is byte-for-byte the origin response (immutable rows);
- purge-then-refetch recovers from a planted corrupt entry;
- origin outage on mutable row: stale within one TTL, error after, no unbounded stale;
- authorization: non-loopback payload request without capability is rejected; loopback/local profile requires none;
- CA pinning: guest tools reject anything not presenting the deployment CA;
- size-cap enforcement does not leave a partial blob.

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
- 8 simultaneous forks cold-fetching the same rustup dist;
- cold node-local tier with warm object storage;
- warm node-local tier;
- upstream-error and upstream-slowdown cases with latency budgeting.

Success criteria for the first campaign:

- Vite build/lint remains within 10% of the existing measured `9.91 s`;
- Vite warm total falls below `45 s` on the same host;
- setup-node exact toolcache hit is below `2 s`;
- cache restore/save no longer reports `File name too long`;
- server RSS does not scale with archive size;
- warm `cargo build --locked` on this repository restores crates without internet;
- rustup channel update on a warm mirror is LAN-speed;
- cold first-step latency remains in the fork-pool target range observed on `main` (sub-minute end-to-end including configure), without Docker Hub rate-limit dependence.

## Phased roadmap

### Phase 0: Instrument before changing policy

Target areas:

- cache handlers in `preloop-runner-server`;
- `CacheStore` operations;
- orchestrator golden/tool/image phases;
- runner setup/post action timing.

Deliverables:

- structured phase metrics;
- cache hit/miss reasons;
- byte and latency accounting;
- benchmark command that produces a comparable JSON record.

Do not optimize a phase that cannot be separately measured.

### Phase 1: Fix Actions cache correctness and measured Vite losses

1. Reproduce and fix the cache backend `File name too long` failure at its source.
2. Add indexed metadata for exact and prefix lookup.
3. Stream v1/v2 upload, finalize, and download without complete in-memory buffers.
4. Materialize workflow-declared Node into `$RUNNER_TOOL_CACHE` in the official layout.
5. Configure a stable pnpm store and verify a real warm restore.
6. Add quota/status/prune controls before automatic eviction.

Acceptance: corrected Vite passes twice, the second run restores a cache, and warm total meets the first-campaign target.

### Phase 2: Ecosystem download payload cache

1. `preloop-cache` crate: `PayloadCache` seam (`get(url)` streaming), policy table (allowlist rows, mutability classes), singleflight, tee-to-staging, atomic publish, LRU-by-bytes with soft/hard quotas.
2. Deployment CA generation + TLS listener (`PRELOOP_CACHE_LISTEN`); self-hosted default TLS, local loopback permitted.
3. Config injection into guest provisioning for: rustup, cargo, npm/npmrc, Go `GOPROXY`, pip. Each row one table entry + integration test.
4. Provision-time health gate: runners only start when payload cache is healthy.
5. Operator controls: `preloop cache status|prune|purge`.
6. Flip `PRELOOP_BAKE_TOOLCHAINS` default to off; keep baking as opt-in.
7. Metrics: hit/miss/bytes/RPS per origin row; singleflight coalescing ratio.

Acceptance: on `main`, two consecutive `cargo fetch` runs on fresh forks — the second completes without touching the internet; the conformance suite stays green.

### Phase 3: Self-hosted production hardening

1. Server-derived namespaces and trust-class enforcement for all cache classes (fork-PR read/write split).
2. Capability URLs for downloads on non-loopback listeners; remove plaintext token rows.
3. SQLite WAL + crash reconciliation; optional Postgres + S3-compatible store for multi-node.
4. Background quota enforcement loop; quota pressure metrics.
5. Runbook: purge, rotate CA, cache-drain, restore-from-scratch.
6. Optional Docker Hub pull-through row (registry `registry-mirrors` config) — only if measured Docker rate limits still bite after preloading.

Acceptance: forked-PR simulation cannot read/write trusted entries; restart preserves cache; eviction maintains quota under adversarial writes; docs updated.

### Future phases (not in scope now)

- **Phase 4**: Managed multi-tenancy (KMS, tenant isolation, regional object storage, direct signed URLs)
- **Phase 5**: Ecosystem remote-cache integrations (sccache/Bazel/gocache server support)
- Consider when earned: full container-registry pull-through cache, deeper action-graph prefetching.

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

PayloadCache
  get(url)                          → streaming body + fetch metadata
  policy(row): allowlist / TTL / size-cap
  eviction: LRU by bytes

HotCache
  get(digest)
  populate(digest, stream)
  evict(policy)
```

Toolchains, actions, OCI layers, payloads, and Git objects may use the same underlying blob CAS where useful, but retain class-specific resolvers and policy. Do not force Git object semantics or package-manager stores through the user-facing Actions cache API.

## Rejected approaches

### One shared writable tool/package directory mounted into every VM

Rejected because concurrent jobs can corrupt it, trust boundaries become unclear, and prior extra virtiofs mounts caused measurable fork stalls and instability. Use immutable host/node CAS plus guest-local materialization.

### Bake every common tool into one universal golden

Rejected because undeclared tools make workflows pass locally and fail on GitHub, images grow without bound, and any update invalidates a large artifact. Bake runner internals and baseline packages; layer only workflow-declared toolchains, behind an opt-in flag.

### Resolver-gated toolchain environments

Rejected as a correctness mechanism. The resolver only sees the five action names it knows; it cannot expand matrices, evaluate expressions, or observe custom/`curl` installs. Keep it as a hinting accelerator for toolcache materialization and *optional* baking; let the payload cache carry correctness for every other shape.

### Fork-and-extend golden trees per environment combination

Rejected as the default. Fan-out of `(base × toolchain-set)` goldens is combinatorial (matrix × versions × repos), each golden is a long build and multi-hundred-MB artifact, and layering depth grows without bound. The payload cache provides the same speed property at byte granularity without image complexity. Allow at most base → single opt-in bake layer.

### Transparent HTTPS MITM egress cache

Rejected by default. It changes the trust model for all guest HTTPS, breaks cert-pinning clients, and is indistinguishable from surveillance tooling. Scope mirror insertion to allowlisted origins via per-tool config with a deployment-pinned CA.

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
- storage deduplication exposes cross-repo existence or timing information for private content;
- cache save/restore costs more than the origin operation for the target workload;
- the payload cache cannot guarantee byte-equivalence for an immutable class;
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
11. For payload rows: which origin is allowlisted, what path prefixes, what mutability/TTL class, what guest config injection does it need, and what is the documented tool behavior when the cache is down?
