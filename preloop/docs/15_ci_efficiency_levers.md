# CI Efficiency Levers — Fast, Cheap, Scalable, Safe

## Purpose

An exhaustive catalogue of the techniques commercial CI providers (Blacksmith,
Depot, WarpBuild, Namespace, Cirrus, Buildjet) use to beat GitHub-hosted
runners, mapped onto the Preloop stack: **aksh** (control plane), **smolvm**
(local + smolvm-KVM tiers), and **Firecracker** (scale tier). This supersedes
`docs/provider-perf.md` for Preloop planning; that file remains the
vendor-neutral background.

Each lever names: the **mechanism**, **where it lives** in the stack, and the
**tier(s)** it applies to. Levers are grouped **Fast / Cheap / Scalable / Safe /
Interactive**.

## The one insight everything hangs on: env-var injection

The official runner reads a fixed set of environment variables from the job
message to decide where cache writes, artifact uploads, log streams, tool
lookups, and OIDC tokens go. The control plane injects them before the job
starts; the workflow YAML never sees them. **Whoever controls the server
controls these variables.** aksh already rewrites them to point at itself:

```rust
// broker_acquire_job (crates/aksh-runner-server/src/lib.rs)
endpoint.data.insert("CacheServerUrl".to_owned(),   public_base_url());
endpoint.data.insert("ResultsServiceUrl".to_owned(), public_base_url());
endpoint.data.insert("FeedStreamUrl".to_owned(),
    format!("{}/ws/live-logs/{}", websocket_base_url(), message.job_id));
```

| Variable | Source field | Lever it unlocks |
|---|---|---|
| `ACTIONS_CACHE_URL` | `SystemVssConnection.data.CacheServerUrl` | local cache server |
| `ACTIONS_RESULTS_URL` | `.data.ResultsServiceUrl` | local artifact/results server |
| `ACTIONS_RUNTIME_URL` | `SystemVssConnection.url` | local live-log/results server |
| `RUNNER_TOOL_CACHE` | runner host dir | pre-baked toolchains |
| `RUNNER_TEMP` | runner host temp | tmpfs/ramdisk (or ext4 storage disk) |
| `GITHUB_WORKSPACE` | working dir | fast ext4 storage disk |
| `GITHUB_SERVER_URL` / `GITHUB_API_URL` | github context | git mirror / API proxy |
| `ACTIONS_RUNNER_HOOK_JOB_STARTED/COMPLETED` | runner host env | pre-warm / snapshot / cleanup hooks |

Everything below is a specific application of controlling these plus the smolvm
snapshot/fork substrate.

---

## Fast

### F1. Pre-baked `RUNNER_TOOL_CACHE` (highest ROI)
Every `actions/setup-*` checks `RUNNER_TOOL_CACHE` first; a hit is ~50ms, a cold
download is 10–60s. **Mechanism:** bake common tool versions (node, python, go,
java, browsers) into each tier's base image so the directory ships in the disk
image. **Where:** per-tier image build (doc 14 supply chain). **Tier:** all.
Transparent to the workflow, no protocol change.

### F2. Snapshot-as-cache (smolvm-native, the big one)
A warm `.smolmachine` / `machine fork` carries the **entire filesystem** — deps,
build caches, `.pyc`, cargo `target/`, `node_modules`, `.next` — not just a named
cache dir. After run 1, cold starts disappear, and every read stays on ext4 (no
bind-mount/APFS metadata tax). **Mechanism:** `pack create --from-vm` after a
successful run → boot next run `--from` it. **Where:** aksh orchestrator +
`SmolvmProvider`. **Tier:** Local, smolvm-KVM. (Firecracker uses its own
snapshot/diff-snapshot equivalents.)

### F3. Local cache geography
GitHub's cache is fast only when co-located with the runner; self-hosted runners
cross the public internet to Azure (40–80ms, 100–500 Mbps). Preloop's cache
server **is the control plane**, on loopback/LAN. A 500 MB restore drops from
15–40s to 1–2s. **Where:** aksh cache service (doc 07). **Tier:** all.

### F4. Zero-wait cache pre-fetch
The control plane knows the queued job's expected cache keys before the runner
boots. **Mechanism:** stream the `.tar.zst` cache archive to the worker's
staging SSD during VM boot; by the time the restore step runs, files are local.
**Where:** aksh scheduler + worker. **Tier:** smolvm-KVM, Firecracker.

### F5. Warm VM pools / fork-from-golden
Keep a small pool of pre-booted VMs, or `machine fork` a golden warm snapshot on
demand (<200ms + CoW). Eliminates boot from the critical path. **Where:** worker
+ `VmProvider.fork`. **Tier:** Local (fork), smolvm-KVM, Firecracker (snapshot
clones). Elastic memory balloon makes idle pooled VMs near-free (F-cost link).

### F6. Job hooks (aksh-native)
`ACTIONS_RUNNER_HOOK_JOB_STARTED/COMPLETED` are injected as synthetic script
steps around the user steps. **Uses:** pre-clone repo so `actions/checkout` is a
local pull; pre-pull heavy Docker images; snapshot the VM for time-travel;
post-job cleanup, telemetry, billing. **Where:** `job_runner.rs`. **Tier:** all.

### F7. Git mirror + LFS LAN cache
Point `GITHUB_SERVER_URL`/`GITHUB_API_URL`/`GITHUB_GRAPHQL_URL` at a local mirror
so `actions/checkout` clones from LAN; proxy LFS downloads from local SSD.
**Caveat:** serving private-repo clones means the source lives on provider disk —
off the table for most enterprises unless the runner is in-VPC or self-hosted
(smolvm-KVM on the customer's own box sidesteps this). **Where:** aksh git proxy.
**Tier:** smolvm-KVM, Firecracker (with trust caveats).

### F8. Fast storage path discipline
Keep all CI I/O on the ext4 storage disk (2.5–3.8× faster small-file I/O than
APFS); never bind-mount deps/build output through virtiofs. Get source in with
`tar | machine cp`, extract results with `machine cp`. **Where:** guest agent /
workspace setup (doc 04). **Tier:** Local, smolvm-KVM.

### F9. Rosetta x86-on-ARM
Run x86_64 images on ARM hosts with ~17% CPU overhead vs QEMU's 5–10×.
`--rosetta` on smolvm. **Where:** smolvm. **Tier:** Local (macOS). Firecracker
uses native per-arch hosts instead.

### F10. Ordered live log streaming
Stream logs before step completion via `FeedStreamUrl` (aksh WebSocket), so
verdicts feel instant to humans and agents. **Where:** aksh log store (doc 07).
**Tier:** all.

---

## Cheap

### C1. Egress elimination
Routing cache/artifact traffic to a co-located server eliminates cloud egress.
Napkin math: 1,000 concurrent VMs × 50 builds/day × 5 GB × 30 days ≈ 7.5 PB/mo;
at $0.05/GB that is ~$375k/mo of egress → **$0** with local caching. **Where:**
aksh cache/artifact services. **Tier:** smolvm-KVM, Firecracker.

### C2. ARM cost advantage
The smolvm-KVM tier targets cheap Linux aarch64 hosts; ARM instances are
typically 20–40% cheaper per vCPU-hour and match the developer's local arm64
`.smolmachine` for handoff. **Tier:** smolvm-KVM.

### C3. Scale-to-zero + spot
Pay only for running microVMs. Firecracker's boot speed and small footprint make
per-job spot/preemptible instances practical; drain and reschedule on
preemption. **Where:** worker fleet autoscaler. **Tier:** Firecracker (primary),
smolvm-KVM (possible).

### C4. Elastic memory over-provisioning
smolvm's virtio balloon commits only what the guest uses and reclaims the rest;
idle vCPUs sleep in the hypervisor. Over-provision warm pools and generous job
sizes at near-zero idle cost. **Where:** smolvm. **Tier:** Local, smolvm-KVM.

### C5. Firecracker density
Thousands of microVMs per host (Lambda/Fargate scale) amortize hardware far
better than one-VM-per-instance. **Where:** Firecracker + `jailer`. **Tier:**
Firecracker.

### C6. Real-time budget enforcement
Track CPU-minutes and storage per developer/team/repo at the queue layer; pause
a runaway matrix that exceeds a budget and request approval. **Where:** aksh
scheduler + metering. **Tier:** smolvm-KVM, Firecracker.

### C7. Dynamic VM sizing from history
Analyze prior runs' CPU/mem; schedule low-resource jobs on smaller VMs and
auto-upgrade jobs that historically OOM. **Where:** aksh scheduler. **Tier:**
smolvm-KVM, Firecracker (smolvm balloon softens the downside locally).

---

## Scalable

### S1. Control-plane leasing + worker fleet
aksh already models runner registration, sessions, and job leases. Scale by
adding stateless workers that acquire/renew/complete leases and run one VM per
job. **Where:** aksh + `preloop-worker`. **Tier:** smolvm-KVM, Firecracker.

### S2. Firecracker jailer multi-tenancy
Per-microVM chroot, namespaces, cgroups, seccomp, unique UID. The proven path
for hostile multi-tenant packing. **Where:** Firecracker `jailer`. **Tier:**
Firecracker.

### S3. Canary VM-image rollouts
Route a small % of jobs to a new tier image, watch failure rate, auto-rollback
on a spike — invisible to developers. **Where:** aksh job routing. **Tier:**
smolvm-KVM, Firecracker.

### S4. Dynamic matrix sharding
Count tests before dispatch; if the suite doubled, split across 8 shards instead
of 4 to hold wall-clock constant. **Where:** aksh scheduler + parser. **Tier:**
all.

### S5. Cross-repo cache sharing by lockfile hash
If repo B requests a cache key that misses but repo A has an identical lockfile
digest, serve A's cache. Content-addressed, tenant-policy-gated. **Where:** aksh
cache lookup logic (doc 07). **Tier:** smolvm-KVM, Firecracker.

### S6. Layered / content-addressed cache
Store cache and OCI layers content-addressed so identical layers dedupe across
runs, repos, and tiers, and reproducible builds map source spec → known digest.
**Where:** aksh cache + image supply chain (doc 07, doc 14).

### S7. Smart step skipping / artifact reuse
If only test files changed, skip compilation: pull the prior run's build
artifact and start at the test step. **Where:** aksh scheduler + step
transaction model (doc 09). **Tier:** all.

### S8. Git-dependency pre-fetch
Parse lockfiles; if seen before, pre-populate cargo/npm/pip dirs on the VM
storage disk before dispatch (a specialization of F2/F4). **Where:** aksh + job
hooks. **Tier:** smolvm-KVM, Firecracker.

### S9. OIDC cloud federation brokerage
aksh serves the OIDC token endpoints and signs JWTs; negotiate cloud creds (AWS
IAM Roles Anywhere, GCP, Vault) from job metadata and inject them, so developers
skip per-cloud trust config. **Where:** aksh OIDC endpoints. **Tier:**
smolvm-KVM, Firecracker.

---

## Safe (efficiency without opening holes)

### A1. Token firewall (egress proxy)
Route runner API calls through a policy proxy; enforce least-privilege — allow
check-run status posts, block repo writes/tag deletes from a PR-triggered job.
smolvm `--allow-host`/`--allow-cidr` do the network-layer enforcement; the proxy
does the API-semantics layer. **Where:** smolvm egress + aksh proxy (doc 06).
**Tier:** all.

### A2. Secret brokerage, zero secrets at rest
Pull secrets from Vault/1Password/AWS at launch through smolvm's reference model
(`--secret-env`/`--secret-file`); never store them in GitHub, the DB, or a
`.smolmachine`. **Where:** smolvm secrets + aksh broker (doc 06). **Tier:** all.

### A3. Cache-poisoning controls
Trust-tier cache namespaces, write-on-success, failed-run quarantine, lockfile
provenance. Untrusted jobs cannot write caches trusted jobs read (doc 06, doc
07). **Where:** aksh cache store. **Tier:** all.

### A4. Worker recycling + attestation
One job per VM, proven reap, periodic host recycling; attest tier image digests.
Prevents cross-job contamination at scale. **Where:** worker + Firecracker
jailer. **Tier:** smolvm-KVM, Firecracker.

---

## Interactive (differentiators, not just speed)

### I1. Pause-on-failure + shell debug
Keep the VM alive on failure; `machine shell` (vsock PTY, no SSH/net) drops the
developer/agent into `/workspace`. Fix, `preloop resume` from the failed step
(doc 09, doc 13). **Where:** smolvm shell + aksh state machine. **Tier:** Local,
smolvm-KVM.

### I2. Time-travel step snapshots
`machine fork`/snapshot at each step boundary; restart execution from any prior
step's exact on-disk state. **Where:** `VmProvider.fork` + step transactions.
**Tier:** Local, smolvm-KVM (macOS/Linux).

### I3. Fork-from-failure (speculative fixes)
CoW-clone the failed checkpoint N times and try N fixes in parallel from the same
warm state (doc 09). **Where:** `machine fork`. **Tier:** Local, smolvm-KVM.

### I4. Collaborative terminal
Host-coordinated vsock/WebSocket terminal so multiple developers (or an agent +
human) share a suspended VM. **Where:** aksh + smolvm exec plane. **Tier:**
smolvm-KVM (admin-gated for untrusted tenants).

### I5. Smart warning blamer
Parse compiler/test warnings, `git blame` the affected lines, notify the author.
**Where:** aksh log parser + problem matchers. **Tier:** all.

---

## Strategic trade-off (why Preloop can do this at all)

Reimplementing the GHA control plane (aksh) is high-effort but unlocks the
**enterprise / air-gapped** market: banks, defense, healthcare that legally
cannot send code/secrets/logs to github.com. The smolvm-KVM tier is exactly the
self-managed, single-runtime, portable package that market buys — and the same
control plane serves the Firecracker scale tier for public managed CI. Compute-
only proxy plays (Depot/Warp style) are lower-effort but cannot serve the
air-gapped segment. Preloop deliberately owns the control plane to get both.

## Lever → tier quick map

| Lever | Local | smolvm-KVM | Firecracker |
|---|:--:|:--:|:--:|
| Pre-baked tool cache (F1) | ✓ | ✓ | ✓ |
| Snapshot-as-cache (F2) | ✓ | ✓ | ~ (FC snapshot) |
| Local cache geography (F3) | ✓ | ✓ | ✓ |
| Cache pre-fetch (F4) | — | ✓ | ✓ |
| Warm pools / fork (F5) | ✓ | ✓ | ✓ |
| Job hooks (F6) | ✓ | ✓ | ✓ |
| Git mirror / LFS (F7) | — | ✓ | ✓ |
| Rosetta x86 (F9) | ✓ (macOS) | — | native |
| Egress elimination (C1) | — | ✓ | ✓ |
| Scale-to-zero / spot (C3) | — | ~ | ✓ |
| Elastic memory (C4) | ✓ | ✓ | — |
| FC density (C5) | — | — | ✓ |
| Jailer multi-tenancy (S2) | — | — | ✓ |
| Cross-repo cache (S5) | — | ✓ | ✓ |
| OIDC brokerage (S9) | — | ✓ | ✓ |
| Token firewall (A1) | ✓ | ✓ | ✓ |
| Pause/shell/fork (I1–I3) | ✓ | ✓ | ~ |

`✓` = applies, `~` = partial/via a different mechanism, `—` = not applicable.
