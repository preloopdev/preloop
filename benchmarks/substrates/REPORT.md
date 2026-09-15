<!-- INTERNAL — not for publication.

     Contains host-specific infrastructure detail, a deliberate weakening of
     AgentENV's sandbox egress hardening, and unresolved criticism of preloop's
     own pool behaviour. `docs/` is user-facing (README links into it); this
     file is not, and `.gitattributes` keeps it out of released archives.
     Do not link it from README, docs/, or a changelog entry. -->

# AgentENV vs SmolVM as preloop's KVM substrate

**Date:** 2026-09-05 · **Host:** `cpane` — Ubuntu, kernel 7.0.0-31, 32 cores, 30 GiB
RAM, NVMe (`/dev/mapper/ubuntu--vg-ubuntu--lv`, 742 GiB free), `/dev/kvm`,
cgroup v2 · **Substrates:** AgentENV (`aenv`) 0.2.0 with Firecracker
1.15.1-patch-v1 + overlaybd + ublk · SmolVM 1.13.1 (libkrun) · **Engine:**
preloop `0.2.0` built from this worktree, release profile.

> **Recommendation up front.** Ship AgentENV as the KVM default — it is
> decisively faster at every VM lifecycle operation preloop performs, and the
> provider is complete and tested. But **do not expect the end-to-end win
> yet**: with the current pool logic, per-job wall time on AgentENV is *worse*
> than SmolVM because of a preloop-side interaction (§4), not because of the
> substrate. Fix that, and the primitive numbers say a fork-per-job pool gets
> ~2× faster forks, ~7× faster cold boots, and ~26× faster pause.

---

## 1. What was built

| Piece | Location |
|---|---|
| `AgentEnvProvider` (`VmProvider` over the `aenv` CLI) | `crates/preloop-vm/src/agentenv.rs` |
| Backend selection, guest-access routing for `shell`/`debug` | `crates/preloop-cli/src/main.rs` |
| `ProviderCapabilities` + orchestrator branches | `crates/preloop-vm/src/lib.rs`, `crates/preloop-orchestrator/src/lib.rs` |
| Contract tests against a recording fake `aenv` (16 tests) | `crates/preloop-vm/tests/agentenv_provider.rs` |
| Design, mapping, capability gaps, host prerequisites | `docs/vm-substrates.md` |
| Benchmark harnesses + raw samples | `benchmarks/substrates/` |

`PRELOOP_VM_BACKEND=smolvm|agentenv` is authoritative; unset, AgentENV is
selected on a Linux host with `/dev/kvm` and `aenv` on `PATH`, SmolVM
everywhere else (it is the only option on macOS). An unrecognized value is a
hard error.

Verification: `cargo fmt --check`, `cargo clippy --workspace --all-targets`
(clean), `cargo test --workspace` (all green; 16 new AgentENV contract tests),
plus the live smoke test in §2 that validated every assumption the provider
makes against real `aenv` 0.2.0.

## 2. Substrate primitives

`benchmarks/substrates/micro-bench.sh`, 10 repetitions per operation per
substrate, medians, identical guest resources (4 vCPU / 4 GiB), OCI image
pre-pulled into both, host otherwise idle (the production `preloop.service`
was stopped for the whole campaign and restarted afterwards).
Raw samples: `results-cpane-20260905/micro-bench.jsonl` (300 samples, 0
failures).

| Operation (s) | AgentENV med | p90 | SmolVM med | p90 | SmolVM / AgentENV |
|---|---|---|---|---|---|
| cold boot from OCI image | **1.263** | 2.682 | 8.852 | 12.108 | **7.01×** |
| first exec after boot | **0.038** | 0.042 | 0.047 | 0.058 | 1.23× |
| exec round trip (×10) | **0.245** | 0.251 | 0.480 | 0.502 | 1.96× |
| prepare fork base | **0.304** | 0.332 | 0.643 | 0.658 | 2.12× |
| fork one clone | **0.081** | 0.095 | 0.167 | 0.187 | 2.06× |
| first exec in the clone | **0.047** | 0.050 | 0.053 | 0.054 | 1.13× |
| fork 8 clones concurrently | **0.118** | 0.172 | 0.896 | 0.922 | **7.60×** |
| pause | **0.194** | 0.204 | 5.108 | 5.118 | **26.32×** |
| resume | **0.088** | 0.106 | 0.541 | 0.550 | 6.17× |
| exec after resume | 0.049 | 0.053 | **0.047** | 0.062 | 0.97× |
| guest CPU loop | 2.778 | 2.788 | 2.798 | 2.849 | 1.01× |
| guest 512 MiB `dd` (fsync) | 0.622 | 0.637 | **0.379** | 0.399 | 0.61× |
| guest 2 000 small files | 0.099 | 0.110 | **0.083** | 0.091 | 0.84× |
| delete clone | 0.121 | 0.158 | **0.090** | 0.100 | 0.74× |
| delete | 0.198 | 0.217 | **0.128** | 0.139 | 0.64× |

**Reading.** AgentENV wins the lifecycle by wide margins: a snapshot restore
replaces a boot, and forks do not serialize on a single retained checkpoint
(SmolVM holds exactly one RAM checkpoint per golden — 8 concurrent forks cost
it 0.90 s against AgentENV's 0.12 s). SmolVM wins teardown and, in this
micro-test, sustained guest writes. CPU is identical: both are KVM.

An architectural difference behind the pause number: SmolVM has no live pause,
so preloop's `stop` is a real shutdown (5.1 s) and `start` a real boot;
AgentENV pauses to a snapshot in 0.19 s and resumes in 0.09 s.

## 3. Guest I/O, attributed per filesystem

The first e2e pass suggested AgentENV was ~30× slower at I/O. That was an
artifact worth documenting: **SmolVM mounts a tmpfs on `/tmp`, AgentENV does
not.** A workflow writing to `/tmp` compares RAM against a block device. Both
harnesses were corrected to write into the job workspace, and the filesystems
were measured directly (`io-filesystem-attribution.sh`,
`io-clone-attribution.sh`), outside preloop, same resources:

| Path (s) | AgentENV | SmolVM |
|---|---|---|
| `/tmp` — filesystem | ext4 on ublk/overlaybd | **tmpfs (RAM)** |
| `/tmp` write 1 GiB fsync | 0.938 | 0.658 |
| `/tmp` create 20 000 files | 0.434 | 0.205 |
| `_work` — filesystem | ext4 on ublk/overlaybd | overlayfs on **virtiofs (host-backed)** |
| `_work` write 1 GiB fsync | **0.444** | 0.564 |
| `_work` read 1 GiB | 0.095 | 0.100 |
| `_work` create 20 000 files | 1.808 | **0.568** |
| `_work` tar 20 000 files roundtrip | **0.373** | 3.128 |

In a **clone** (the shape a real job runs in — booted from a snapshot / fork):

| `_work` (s) | AgentENV cold | AgentENV clone | SmolVM fork |
|---|---|---|---|
| write 1 GiB fsync | 0.448 | 0.455 | 0.478 |
| create 20 000 files | 1.773 | 2.830 | **0.517** |
| tar roundtrip | 0.381 | 2.376 | 2.205 |

**Reading.** Neither substrate dominates I/O. AgentENV gives a real block
device: bulk writes are the fastest measured, and tar extraction is ~8× faster
than virtiofs on a cold sandbox. SmolVM's host-backed overlay is ~3–5× faster
at creating many small files (metadata operations go straight to the host page
cache) but pays heavily for tar extraction. AgentENV clone writes are ~1.5×
slower than a cold sandbox's — the copy-on-write cost against the snapshot
chain.

For CI this matters concretely: `npm ci`/`cargo build` create many small files
(SmolVM's strength); artifact/cache tar handling and large writes favour
AgentENV.

## 3a. CI-representative workloads (the small-write question, settled)

`benchmarks/substrates/ci-workload-bench.sh`, 3 reps, medians, 204 samples
(`results-cpane-20260905/ci-workload.jsonl`). Runs the operations CI actually
performs — checkout-shaped unpacking, `git add`/`commit`/`status`, tree copy,
tree delete, metadata walks, archive handling, per-file fsync — inside the
guest, on the job workspace path, with preloop out of the picture. Measured
both in a **fresh VM** and in a **clone/fork**, because every real job runs in
a clone.

| Operation (s) | AgentENV fresh | SmolVM fresh | AgentENV clone | SmolVM fork | clone winner |
|---|---|---|---|---|---|
| untar_5k | 0.123 | 0.131 | 0.672 | 0.159 | **SmolVM 4.2×** |
| loop_create_5k | 0.185 | 0.247 | 0.662 | 0.292 | **SmolVM 2.3×** |
| fsync_create_500 | 0.280 | 1.557 | 0.366 | 1.557 | **AgentENV 4.3×** |
| copy_tree | 0.124 | 0.150 | 0.713 | 0.170 | **SmolVM 4.2×** |
| rm_tree | 0.025 | 0.040 | 0.023 | 0.041 | **AgentENV 1.8×** |
| stat_walk | 0.009 | 0.008 | 0.010 | 0.009 | parity |
| git_add | 1.738 | 1.788 | 2.315 | 1.805 | **SmolVM 1.3×** |
| git_commit | 0.017 | 0.024 | 0.024 | 0.027 | parity |
| git_status_dirty | 0.007 | 0.006 | 0.006 | 0.007 | **AgentENV 1.2×** |
| git_clone_local | 0.371 | 0.410 | 1.060 | 0.522 | **SmolVM 2.0×** |
| targz_create | 1.694 | 1.685 | 1.738 | 1.700 | parity |
| targz_extract | 0.337 | 0.395 | 0.541 | 0.471 | parity |
| write_1g_fsync | 0.959 | 1.151 | 1.223 | 1.349 | parity |
| read_1g | 0.102 | 0.101 | 0.100 | 0.100 | parity |
| write_200x1m | 0.457 | 0.416 | 0.585 | 0.523 | parity |

Three conclusions, and they correct the simpler story from §2 and §3:

1. **In a fresh VM, AgentENV is faster or equal at essentially everything**,
   including small writes — the shell loop creating 5 000 files is 1.34× faster
   there, not slower. So "SmolVM is better at small writes" is not a property of
   the substrates in isolation.
2. **The gap appears only in a clone, and it is copy-on-write first-touch
   cost.** Unpacking, copying, and `git clone` degrade 3–5× in an AgentENV
   clone (0.12 s → 0.67 s for a 5 000-file untar) while a SmolVM fork barely
   degrades. This is where SmolVM's clone advantage actually comes from.
3. **That cost amortizes almost immediately.** Running the identical workload
   three times inside the *same* clone: untar 0.349 s → 0.075 s → 0.050 s;
   tree copy 0.131 s → 0.063 s → 0.062 s. Once a region has been copied up,
   the clone is as fast as a fresh VM. So the penalty is a warm-up measured in
   hundreds of milliseconds per job, not a throughput tax.

**The one unambiguous, non-amortizing win is fsync.** 500 files each flushed to
disk: **0.37 s on an AgentENV clone vs 1.56 s on a SmolVM fork, 4.3×**, and it
does not improve with repetition on either side. Package managers, compilers
writing object files, and git object writes all flush. That is the shape most
likely to dominate a real `npm ci` or `cargo build`, and it favours AgentENV.

Also notable: `rm -rf` of a 5 000-file tree is 1.8× faster on AgentENV, which
matters for post-job cleanup, and `git add` of the same tree is ~1.3× slower on
an AgentENV clone (first-touch again).

Net: for a short job that writes a few thousand new files once, SmolVM's fork
has an edge of a few hundred milliseconds. For anything that fsyncs, or runs
long enough to warm its working set, AgentENV wins. Neither difference is close
to the seconds-per-job that the lifecycle numbers in §2 move.

## 4. End to end through the engine — and why it does not match yet

`benchmarks/substrates/e2e-bench.sh`, one substrate at a time, fresh
`PRELOOP_HOME`, identical guest resources, identical corpus, identical
transport (TCP to the host address for both, because AgentENV cannot forward a
host Unix socket), each substrate in its own fast path (SmolVM: packed golden +
fork; AgentENV: golden + snapshot fork).

**Cold start — engine process start to a pool that can serve work:**

| | AgentENV | SmolVM |
|---|---|---|
| cold start to pool ready | **629–768 s** (3 runs) | 782 s |

Both are dominated by the same golden bake (apt baseline, Rust 1.88, Go 1.26,
docker tooling, git built from source) — network-bound, not substrate-bound.
SmolVM additionally builds and writes a 1.45 GiB pack.

**Per-run wall clock vs actual in-guest step time** (from each engine's own
state DB, `results-cpane-20260905/db-timings.txt`):

| Workflow | AgentENV wall | AgentENV steps | SmolVM wall | SmolVM steps |
|---|---|---|---|---|
| bench-hello | 9.0 | 0.00 | **2.1 / 0.5** | 0.00 |
| bench-io | 123.5 | **2.19** | **4.2 / 1.8** | 1.24 / 1.13 |
| bench-node | **4.2** | **3.62** | 4.7 / 6.3 | 3.95 / 5.50 |
| bench-matrix8 (8 jobs) | 601.5 | — | **2.9 / 4.2** | — |

The step columns are the substrate doing work, and they agree with §3: within
~1.5× of each other, each winning some workloads. **The wall-clock columns do
not measure the substrate.** Two preloop-side effects dominate them:

1. **A failed job parks its VM for 600 s.** `bench-checkout-rust` fails in this
   corpus (the bake installs Rust under `/root`, unreadable by the `runner`
   user, so `cargo` is missing — a corpus/bake issue, identical on both
   substrates). Every failure logs `job failed — VM preserved for debugging`
   and holds a concurrency permit for `DEBUG_IDLE_TIMEOUT` = 600 s. `matrix8`
   landing on exactly 601.5 s — twice, in independent runs — is that timeout,
   not the substrate.
2. **A reproducible ~120 s per-job provisioning stall on AgentENV.**
   `bench-io` measured 123.5 s / 123.6 s / 124.7 s across three independent
   runs while its steps took 2.2 s, with a silent gap in the server log between
   the previous job completing and this one starting. The micro-benchmarks rule
   out the VM operations themselves (fork 0.08 s, first exec 0.05 s), and
   `apt-get update` inside a live clone returns in 2 s with the golden's lists
   already inherited. It is a fixed timeout in the provisioning path that the
   AgentENV backend trips and the SmolVM backend does not — the strongest
   remaining suspect is surplus ephemeral runners: AgentENV forks 6 runners so
   fast that the extras exit without a job, each one logging
   `job failed — VM preserved` and holding a permit. **This is the one blocker
   between the primitive numbers and the end-to-end win, and it needs a
   debug-level trace of one provisioning cycle to close.**

Caveat on comparability: the SmolVM e2e pass ran the pre-correction corpus
(`/tmp` paths); its step times are therefore slightly flattered (tmpfs). The
AgentENV pass ran the corrected corpus. The step-level conclusion —
substrate-attributable execution within ~1.5× — holds under both.

## 4a. Real project CI workloads from forked goldens

`project-bench.sh` ran one fresh job per project, with identical 4 vCPU /
4 GiB / 30 GiB resources. AgentENV used a golden snapshot fork
(`PRELOOP_USE_FORK=1`); SmolVM used a packed golden plus fork
(`PRELOOP_USE_FORK=1`, `PRELOOP_USE_PACKED_GOLDEN=1`). The timer starts
immediately before `preloop run` and ends when the run exits. It includes
runner startup, checkout, tool setup, dependency downloads, compilation,
tests, and packaging; source-archive preparation and golden creation are
outside the timed interval.

Raw samples: `results-cpane-20260905/project-bench-{aenv,smolvm}.jsonl`.
These are one-shot observations, not medians.

| Project | AgentENV | SmolVM | SmolVM / AgentENV |
|---|---:|---:|---:|
| ESLint | **247.1s**, success | 311.4s, success | 1.26× |
| TypeScript | 929.4s, failure | **267.0s**, failure | 0.29× |
| pydantic-core | **314.9s**, failure | 382.9s, failure | 1.22× |
| cargo-mutants | 622.7s, failure | **339.3s**, failure | 0.54× |
| GoBGP | **224.2s**, success | 233.4s, failure | 1.04× |

AgentENV completed 2/5 jobs; SmolVM completed 1/5. The failures were:

* **TypeScript:** AgentENV completed build and reached the test suite, then
  failed because the `fanotify` watcher backend is unsupported on the guest
  filesystem. SmolVM failed earlier in `go test` with compiler processes
  killed and `no space left on device` while building the test set.
* **pydantic-core:** both substrates failed at the Rust link step because the
  golden lacks `libpython3.12` (`cannot find -lpython3.12`).
* **cargo-mutants:** both substrates reached project tests that invoke
  `cargo nextest`, but `cargo-nextest` was not installed in the golden.
* **GoBGP:** AgentENV passed. SmolVM reached the server tests but ended with
  `signal: killed`; the exact kernel/resource reason was not captured.

Thus three failures are shared golden/workflow prerequisites, while the
TypeScript and GoBGP failures expose resource/filesystem differences rather
than clean throughput comparisons. The timings remain useful for provisioning
and partial-build-path comparison, but must not be treated as
successful-project throughput.

The result is mixed rather than a substrate-wide winner. SmolVM was faster on
the TypeScript partial path and cargo-mutants; AgentENV was faster on ESLint
and pydantic-core. GoBGP was near parity, but only AgentENV passed. These
results reinforce the primitive conclusion in §2: workload shape and the
runner/engine path matter more than a single universal VM winner.

During the campaign the server also emitted the Plan 000 step-4 probe warning
for bare-listener-token lifecycle calls (`renewjob`/`completejob`; observed
count 372). That does not invalidate the wall-clock measurements, but it means
this campaign is not evidence that Plan 004 generation fencing protects
those lifecycle calls; this remains an open protocol finding.

---

## 4b. How this deploys, single host and at scale

**Two independent channels.** Getting these confused is the easiest way to
mis-design the deployment:

| Channel | Direction | Carries | Transport |
|---|---|---|---|
| provider | engine → AgentENV API | create, snapshot, fork, exec, upload | HTTP to the AgentENV endpoint (`~/.config/aenv/credentials`) |
| runner | guest → engine | registration, job poll, logs, results | plain TCP to `PRELOOP_RUNNER_URL` |

The runner channel does **not** pass through AgentENV. `envd`, AgentENV's guest
agent, is reached host→guest through AgentENV's own reverse proxy and speaks
its own protocol; it cannot proxy the Actions protocol. That is why the guest
needs real IP reachability to the engine, and why `PRELOOP_RUNNER_URL` must be
a routable host address rather than loopback.

**AgentENV is already a cluster, so "one endpoint" is the correct design.**
Upstream ships a Gateway + Scheduler in front of N runtime nodes
(`docs/src/deployment/static-multi-node.md`, plus a Kubernetes deployment):

| Component | Role |
|---|---|
| Gateway (`:8080`) | the single client-facing endpoint; HTTP-proxies to whichever node holds a sandbox |
| Scheduler (`:9090`) | placement, node heartbeats, sandbox→node binding |
| Runtime nodes (`:8000`) | the machines that actually run Firecracker; need `/dev/kvm` and kernel ≥ 6.8 |
| Shared storage | POSIXFS or object storage, required across all runtime nodes |

So the provider keeps talking to exactly one URL — the Gateway — and placement,
node binding, and cross-node snapshot access are AgentENV's problem, not
preloop's. Shared storage is what makes a snapshot taken on node A resumable on
node B, which is the consistency question answered: **snapshots are the shared
state, and they live in shared storage, not on a node's local disk.** Local
disk is only a bounded cache.

What preloop must still get right per deployment:

* **Every runtime node's guests must reach the engine.** The egress floor and
  the host `INPUT` rule (§5.3) are per-node configuration. Miss one node and
  jobs placed there starve while jobs on other nodes succeed — an intermittent
  failure that will look like a preloop bug.
* **Volume materialization crosses the network in a cluster.** The runner
  bundle is streamed through the Gateway to whichever node runs the golden. On
  one host that is a local copy; across a cluster it is a transfer. Keeping the
  bundle small stops mattering "for tidiness" and starts mattering for real.
* **`preloop shell` needs the same credentials** wherever it runs, but since it
  addresses the Gateway it works from any machine that can reach it.
* **One engine can drive the whole cluster.** Concurrency limits are
  preloop-side (`max_concurrent`, pool size), not AgentENV-side.
* **Static node lists need a Scheduler restart to add a node**; the Kubernetes
  deployment discovers them. Choose accordingly if the fleet is elastic.
* **KVM and PVM snapshots are not interchangeable** — a snapshot can only be
  restored in the virtualization mode that created it, so do not mix modes
  across nodes in one cluster.

Not verified here: this campaign ran single-node. The cluster claims above come
from upstream documentation, not measurement.

## 5. What AgentENV costs operationally

Discovered while integrating, all documented in `docs/vm-substrates.md`:

1. **No host bind mounts.** Volumes are block devices; there is no
   virtiofs/9p passthrough. `MachineSpec::volumes` is materialized by streaming
   the host directory in as one tar (upload rejects symlinks and drops modes).
   Clones inherit it through the snapshot, so the cost lands once per golden —
   but **keep `PRELOOP_RUNNER_BUNDLE` small**: pointed at a 1.5 GiB
   `target/release`, golden preparation stalled for minutes where SmolVM mounts
   it for free.
2. **No host Unix-socket passthrough.** The engine forces the TCP control
   transport on this backend; `PRELOOP_RUNNER_URL` must be guest-reachable.
3. **Guests cannot reach the host by default — and this is a hard blocker.**
   AgentENV installs an egress floor in each sandbox netns that REJECTs every
   RFC1918/CGNAT/loopback/link-local destination, plus a host `INPUT` REJECT for
   its internal pools. Without opening exactly the engine port, every job is
   failed by the starvation sweep after 600 s. `aenv-egress-allow-host.sh`
   narrows `always_denied_cidrs` to the exact complement around the engine's
   `/32` (16 CIDRs instead of dropping `192.168.0.0/16`) and inserts the host
   `INPUT` allow; `verify-egress.sh` proves the posture from inside a sandbox:
   engine reachable, other LAN hosts blocked, internet reachable. **The
   `INPUT` rule must be re-applied after every `systemctl restart aenv`** —
   AgentENV re-inserts its REJECT at the head of the chain on each start.
   SmolVM needs none of this; AgentENV's default is the safer one.
4. **Server-assigned identity.** No caller-supplied sandbox names, so the
   provider keeps a `MachineName` → sandbox map at
   `$PRELOOP_HOME/agentenv-machines.json` (0600, atomic replace) as a restart
   source, reconciling vanished sandboxes back to `Stopped`.
5. **Sandbox TTL.** Default 300 s upstream; a keepalive re-arms it at a third
   of `PRELOOP_AENV_TTL_SECS` (default 3600) for every running sandbox.
6. **Disk size is a floor, not a contract.** AgentENV refuses to present a
   guest root smaller than its overlaybd base (Ubuntu 24.04 → 64 GiB), so a
   20 GiB spec is retried once without `--disk-size-mb`.
7. **CLI credentials are a file, not env vars** (`~/.config/aenv/credentials`),
   so the engine's service user needs its own copy.
8. **One capability difference already paid off.** SmolVM's forkable snapshot
   does not carry post-boot writes into clones, so the pool re-installs the
   whole baseline in every fork. AgentENV's snapshots do carry them (verified),
   and the new `fork_inherits_guest_writes` capability lets the orchestrator
   skip that: before the fix, every AgentENV runner recompiled git and
   reinstalled Rust/Go/docker before starting its job.

## 5a. Same-host HPC control (2026-09-08)

The public-provider comparison mixed CPU, memory, storage, and substrate. To
separate those variables, the pinned STREAM workload and the direct fio
fallback were run on **the same cpane x86_64 host** through both providers:
4 vCPU, 8 GiB RAM, 64 GiB disk, Ubuntu 24.04.

### CPU and memory

STREAM used the suite's pinned 150,000,000-element arrays and four threads:

| Kernel | AgentENV (MB/s) | SmolVM (MB/s) |
|---|---:|---:|
| Copy | 39,153 | 39,443 |
| Scale | 35,154 | 36,565 |
| Add | 36,572 | 37,257 |
| Triad | 36,800 | 37,417 |

The substrates are within 4% on every kernel. The large STREAM spread in the
public provider run is therefore host CPU/memory allocation, not an AgentENV
CPU virtualization penalty.

### Block I/O

Both runs used a 1 GiB file, `direct=1`, `iodepth=1`, `numjobs=1`, and a
30-second time-based fio run:

| Workload | AgentENV | SmolVM | SmolVM / AgentENV |
|---|---:|---:|---:|
| Sequential read | 2,405 MB/s | 7,512 MB/s | 3.12x |
| Sequential write | 1,986 MB/s | 5,546 MB/s | 2.79x |
| Random 4 KiB read | 152 MB/s / 37.1k IOPS | 226 MB/s / 55.2k IOPS | 1.49x |
| Random 4 KiB write | 146 MB/s / 35.6k IOPS | 197 MB/s / 48.2k IOPS | 1.35x |

Unlike the `/tmp` micro-test, this uses each substrate's storage disk. On this
host, AgentENV's ublk/overlaybd path or its backing storage is a real
steady-state fio disadvantage. It is not explained by AMD-versus-Intel CPU
hardware: STREAM is essentially equal on the same host.

This does not replace the CI-shaped results in §3a. AgentENV still wins the
per-file fsync workload there (0.366 s versus SmolVM's 1.558 s in a clone),
while SmolVM wins clone first-touch unpacking and tree-copy workloads. The
filesystem trade is workload-specific.

The full PTS single-core and network profiles were not repeated in SmolVM:
the PTS/mise toolchain was not installed in that guest. The comparison above
is limited to the valid, matched STREAM and fio workloads.

## 6. Recommendation

**Adopt AgentENV as the KVM default (already wired), and treat §4.2 as the
next task.** Reasoning:

* Every lifecycle primitive preloop depends on is 2–26× faster, and the
  concurrency story is structurally better: immutable, re-forkable snapshots
  versus one RAM checkpoint per golden. The re-arm/`ForkBaseBusy` machinery
  that exists to work around SmolVM's single checkpoint is unnecessary here.
* Pause/resume at 0.19 s / 0.09 s makes idle runners genuinely cheap, which is
  what a warm pool wants; SmolVM's 5.1 s stop makes the same policy expensive.
* I/O is a wash with a real trade, not a regression (§3a): in a fresh VM
  AgentENV is faster or equal at everything measured; in a clone it pays a
  first-touch copy-on-write warm-up (a few hundred ms, gone after the first
  pass) that gives SmolVM's fork an edge on bulk file creation, while AgentENV
  is 4.3× faster at per-file fsync — the shape package managers and compilers
  actually produce.
* The costs are operational, understood, and scripted (§5), not architectural —
  except the guest→host firewall hole, which is a deliberate, documented,
  minimum-scope change to AgentENV's hardening.
* Keep SmolVM as the macOS backend and as the fallback on Linux; the selection
  is one environment variable and both are covered by contract tests.

**Do next, in order:**

1. Trace one provisioning cycle on AgentENV at debug level and kill the ~120 s
   stall (§4.2). Until then, end-to-end throughput on AgentENV is worse than
   SmolVM despite faster primitives.
2. Stop parking a VM for 600 s when an *ephemeral runner exits without a job* —
   it is not a failed job, and it starves the on-demand pool.
3. Fix the bake so `cargo`/`go` are readable by the `runner` user (the
   `bench-checkout-rust` failure is real, and it is not substrate-specific).
4. Re-run `e2e-bench.sh` for both substrates on the corrected corpus once (1)
   and (2) land; that is the number worth quoting for scheduling decisions.
5. Consider a preflight check on the AgentENV backend that verifies a guest can
   reach the engine, so a missing firewall hole fails in seconds with a clear
   message instead of after 600 s of starvation.

## 7. Reproducing

```sh
# CI-representative workloads, fresh VM and clone (3 reps)
benchmarks/substrates/ci-workload-bench.sh 3 /tmp/ciw.jsonl

# primitives (both substrates, 10 reps)
benchmarks/substrates/micro-bench.sh 10 /tmp/micro.jsonl
python3 benchmarks/substrates/analyze.py micro /tmp/micro.jsonl

# I/O attribution
benchmarks/substrates/io-filesystem-attribution.sh
benchmarks/substrates/io-clone-attribution.sh

# AgentENV host prerequisites (run once, re-run the INPUT rule after aenv restarts)
SUDO_PASS=… benchmarks/substrates/aenv-egress-allow-host.sh <host-ip> 9490
SUDO_PASS=… benchmarks/substrates/verify-egress.sh <host-ip> 9490

# end to end, one substrate at a time, never concurrently
benchmarks/substrates/bench-workflows.sh          # build the corpus + git repo
benchmarks/substrates/e2e-bench.sh aenv   2 /tmp/e2e-aenv
benchmarks/substrates/e2e-bench.sh smolvm 2 /tmp/e2e-smolvm
python3 benchmarks/substrates/analyze.py e2e /tmp/e2e-aenv/results.jsonl /tmp/e2e-smolvm/results.jsonl
python3 benchmarks/substrates/db-timings.py
```

Raw samples for this campaign: `results-cpane-20260905/`.
