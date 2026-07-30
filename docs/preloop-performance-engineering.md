# Preloop performance engineering — 2026-07

A record of a performance campaign on `preloop run`: what was measured, what
changed, what was rejected and why, and what is still blocked. Written so the
next person does not have to rediscover any of it.

Host for every measurement: Apple M4 Max, 16 cores, 128 GB RAM, macOS,
SmolVM 1.6.13, guest Ubuntu 24.04 (arm64).

---

## 1. Result

| workload | before | after | host baseline | ratio |
|---|---|---|---|---|
| single job | 1472 ms | **318 ms** | 327 ms | **0.97×** |
| 4-shard matrix | 1261 ms | **401–422 ms** | 550–630 ms | **0.73×** |

Preloop executes both shapes faster than running the identical work natively on
the host. The remaining per-run overhead is ~60 ms of control plane against a
~35 ms measurement noise floor.

Two things worth stating plainly:

* Most of the win came from **fixing defects**, not from tuning. The single
  largest item was a failing WebSocket connect retried inline before every job.
* `actions/checkout` **did not work at all** in VM mode when this started. Any
  workflow that checks out code failed after ~27 s of retries.

---

## 2. The benchmark harness

`autoresearch.sh` → `benchmarks/preloop-perf/bench.py`.

### What it does

1. Builds release binaries (host CLI/engine + `aarch64-unknown-linux-gnu`
   guest runner).
2. Materialises a deterministic Git workspace: 300 files across 12 directories,
   content derived from a fixed hash, committed with pinned author/committer
   dates. Regenerated only when a content stamp changes.
3. Kills any conflicting engine, deletes stale `preloop-runner-*` VMs, reaps
   orphaned hypervisor processes.
4. Starts an isolated engine — own port (19090), own `PRELOOP_HOME`
   (`~/.cache/preloop-perf`), digest-pinned base image.
5. Waits for the warm pool to settle, then measures.

### What it measures

Primary: `e2e_ms` — trimmed mean of 9 `preloop run` invocations of a 4-shard
matrix workflow, wall clock as a developer experiences it.

Secondaries: `host_ms` (same work, shards run concurrently in bash),
`overhead_ratio`, `single_ms` / `host_single_ms` / `single_ratio` (one-job
control), `submit_ms`, `api_total_ms`, `job_ms`, `dispatch_ms`, `pool_boot_ms`,
`replenish_wait_ms`, `warm_runners`.

### Design decisions and why

* **The fixture is synthetic, not this repo.** The benchmark workspace must not
  change while the repo is being edited, or the snapshot cost drifts under you.
* **Pool size is not pinned.** The harness discovers what the engine chose and
  reports it as `warm_runners`. Pinning it would hide any change to how preloop
  sizes its pool.
* **Runs are gated on a replenished pool.** Without the gate the distribution is
  bimodal and the median flips between modes. Replenishment latency is reported
  separately instead of being folded into the primary metric.
* **Trimmed mean, not median.** Early on, the CLI polled on a 250 ms timer, so
  latencies landed on a coarse grid and a median snapped to whichever mode won.
* **Digest-pinned ECR mirror, not Docker Hub.** SmolVM re-pulls the base image
  inside every freshly created VM; Docker Hub's anonymous rate limit turns that
  into a hard failure after a few dozen boots. Learned the hard way.
* **`PRELOOP_BENCH_MATRIX_JOBS` overrides the matrix width.** Added after a
  fixed width hid two real defects — see §6.

### Noise floor

`e2e_ms` reproduces within ~20–45 ms run to run (~2–5 %). Anything smaller than
that cannot be attributed to a change without independent measurement.

---

## 3. Changes, in order

### Segment 1 — single-job latency (1472 → 318 ms)

| commit | change | effect |
|---|---|---|
| `06093f1d` | `live_logs`: drop the random backoff after the *final* connect attempt | **−326 ms** |
| `6d834a02` | `live_logs`: dial the console feed in the background instead of blocking job start | **−579 ms** |
| `87a3f7fa` | `events.ndjson` streams to terminal status; consolidate `is_terminal` onto `ExecutionStatus` | **−202 ms** |
| `a2bd16d4` | snapshots: one workspace probe, parallel bare init, `fsck` only when the object cache changed | −36 ms, submit 84 → 46 ms |
| `1f45a6d6` | orchestrator: poll guest readiness instead of a fixed 2 s sleep | −1.93 s off every engine start |
| `5e14c138` | runner: bridge the advertised control-plane origin to the mounted socket | `actions/checkout` works at all |
| `d4ba86e0` | orchestrator: persist the guest APT index — **later reverted** | regression, see §5 |
| `645e8a18` | revert the APT index mount | removes 20–50 s fork stalls |
| `a5f75096` | snapshots: reuse the previous run's index so `git add --all` only re-hashes what changed | 10× on `add --all` for large repos |

### Segment 2 — parallel matrix throughput (1261 → 422 ms)

| commit | change | effect |
|---|---|---|
| `49587513` | slot builds its replacement while the current job runs | −147 ms |
| `0940fc6b` | warm pool sized to host capacity instead of a fixed 2 | −536 ms; `warm_runners` 2 → 4 |
| `cdc9d1b8` | pre-provision only when the pool has nothing left to hand out | `job_ms` 751 → 423 |
| `e1aeb354` | per-machine VM operations run concurrently (RwLock, not Mutex) | replenish 664 → 437 ms |
| `9050bf80` | delete a pre-provisioned replacement when its slot's runner fails | VM leak fix |
| `3aa766c9` | discard a run's workspace snapshot once every job is terminal | unbounded disk growth |
| `9dcbe1f6` | RSA keypairs pre-generated on the host, injected via `--secret-file` | replenish 434 → 351 ms |
| `660cc8ec` | bound `state/replay` to the 64 most recent execution plans | 1785 files → 64 dirs |
| `78ec1038` | provision against queued demand, not just an empty pool | 8-shard 1225 → 983 ms |
| `4f8f5292` | size the warm set by idle memory, not by CPU | 5-shard 888 → 442 ms |

---

## 4. Defects found (not tuning)

### `actions/checkout` was completely broken

Guest `git` could not reach the control plane, so the workspace snapshot could
never be fetched. Failed after three `git fetch` retries spanning ~27 s with
`Failed to connect to 127.0.0.1 port 9090`.

Cause: only the runner's own HTTP client knew about `PRELOOP_CONTROL_SOCKET`.
Job subprocesses dial the advertised TCP origin, which `SMOLVM_EGRESS_FLOOR=
strict` refuses with `EACCES`.

Fix: `crates/aksh-runner/src/control_bridge.rs` binds the advertised loopback
address *inside* the guest and splices each connection onto the mounted socket.
Blast radius is exactly one host endpoint; the egress floor stays strict.

Verified: same workflow succeeds in 462 ms and checks out 306 files. From inside
a pool VM, `curl` gets `healthz=200` and a `/ws/live-logs` upgrade reaches the
server (401 without a token, as designed).

### Live console logs never connected

Same root cause. Beyond the missing feature it cost ~970 ms per job:
`connect_websocket` retried three times *and slept `rand(100..500)` ms after
every attempt including the last*, all inline before the first step.

### Two unbounded-growth defects

* `state/snapshots/<run_id>` — a full bare Git repository per run, never
  removed. Fixed by discarding once every job in the run is terminal.
* `state/replay/results/<plan>/<job>/` — every job's step and job logs, never
  removed. 1785 files in the bench state directory. Fixed with a 64-plan
  retention window rather than deletion, because `get_run_logs` prefers the
  blob and only falls back to in-memory blocks.

Both were real: the second is what filled the disk mid-campaign and made the
engine return HTTP 500 on blob writes.

### A latent panic on large hosts

`host_runner_pool_size` used `clamp(by_cpu, 8)`. When the CPU budget exceeds the
cap — a 64-core host with 1-vCPU runners — `clamp`'s lower bound exceeds its
upper bound and panics. Found while writing a property test for the new sizing
formula; replaced with saturating `min`/`max` and covered by a test.

### A VM leak

A pre-provisioned replacement was orphaned when its slot's runner failed
(`9050bf80`).

---

## 5. The one self-inflicted regression

**Caching the guest APT index via a virtiofs mount (`d4ba86e0`, reverted in
`645e8a18`).**

It looked like a −27 ms win. It was not causal — the change only touches golden
preparation, and `job_ms` moved too, which it cannot influence.

Worse, it broke fork stability. The golden then carried three virtiofs mounts,
and **every fork inherits the golden's mounts**. A realistic 48 MB
`actions/checkout` workload then hit `clone agent did not respond to ping within
timeout` on **3 of 6 runs**, ~30 s each.

Controlled A/B on the same host, same workload:

| build | runs | fork timeouts |
|---|---|---|
| `HEAD~1` (2 mounts) | 6/6 at 700–749 ms | 0 |
| `HEAD` (3 mounts) | 3/6 stalled 20–50 s | 3 |
| after revert | 6/6 at 716–753 ms | 0 |

**Lesson: golden-only caching through virtiofs is not free. Every mount you add
to the golden is paid by every clone.**

---

## 6. Overfitting audit

The harness fixes the matrix width at 4, and the pool happened to also be 4.
Sweeping the width exposed two defects the primary metric structurally could not
see. **Re-run this sweep before trusting any pool change.**

```sh
for n in 4 5 6 8 9 12 16; do
  PRELOOP_BENCH_MATRIX_JOBS=$n bash autoresearch.sh
done
```

| width | pool 4, `cdc9d1b8` rule | pool 4, `78ec1038` | pool 8, `4f8f5292` | host | ratio now |
|---|---|---|---|---|---|
| 4 | 416 | 404 | **401** | 633 | 0.63 |
| 5 | — | 888 (**1.27**) | **442** | 755 | 0.59 |
| 6 | — | 903 | **493** | 1012 | 0.49 |
| 8 | 1225 (**0.93**) | 983 | **589** | 1470 | 0.40 |
| 9 | — | — | **1164** | 1721 | 0.68 |
| 12 | 1745 | 1733 | **1362** | 3021 | 0.45 |
| 16 | — | — | **1999** | 4872 | 0.41 |

**Defect A — under-provisioning past the pool.** "Build a replacement only if I
am the last idle runner" is tuned to a matrix that exactly fills the pool. Wider
matrices left the second wave paying a full fork+configure serially. Fixed by
publishing post-claim queue depth from the server and building when queued work
exceeds the idle runners left.

**Defect B — a cliff at pool+1.** Width 5 cost 888 ms against a 699 ms host —
the only shape where preloop lost. Per-shard timing: shards 1–4 finish at
335 ms, shard 5 does not start until ~510 ms.

The instructive part is that the first hypothesis was **wrong**. I assumed
over-provisioning and added build reservations; it changed nothing. Direct
measurement then showed configure's entire HTTP sequence is **2.5 ms** — the
~500 ms is `machine fork` plus two smolvm CLI round trips, inflated by
contention with the four running jobs.

Preloop's unit of parallelism is a VM (~500 ms to build under load); the host's
is a process. **A cliff at pool+1 is therefore inherent.** The only lever is how
many runners are already warm — which led to §7.

### Other dimensions swept, all clean

| dimension | result |
|---|---|
| long-lived engine | 40 consecutive matrix runs (160 jobs), no drift: first quartile 453 ms, last 440 ms. Engine RSS 27 MB, `preloop logs` 7 ms |
| steps per job | 1.3 ms marginal (1 step 67 ms, 10 → 78 ms, 30 → 105 ms). **A one-step job at 67 ms is the true end-to-end floor** |
| workspace size | 6000 files / 48 MB: snapshot 196 → ~55 ms after the persistent index; checkout 620–690 ms vs 430 ms host `git clone` |
| action workflows | checkout job 454 ms, of which 380 ms is the action's own node + ~20 git subprocesses. No preloop overhead hiding there |
| dispatch fairness | four warm runners take four queued shards within 0–2 ms |

---

## 7. Why the pool is sized by memory

The old formula was `cores / cpus_per_runner`. That sizes *idle* capacity by the
wrong resource.

Measured RSS of a runner VM:

| state | RSS |
|---|---|
| freshly forked, polling | **131 MB** |
| after running a job | ~390 MB |
| nominal ceiling in config | 4096 MB |

SmolVM balloons the guest, so the ceiling says nothing about the cost of keeping
one warm. Eight idle runners plus the golden are 2.1 GB — **1.6 % of a 128 GB
host**.

`host_runner_pool_size` now allows up to twice the CPU budget, bounded by an
eighth of host memory at a conservative 400 MiB each, capped at 8, never below
the CPU budget. Host memory is read via `sysctl hw.memsize` (macOS) or
`/proc/meminfo` (Linux), no new dependency.

Do **not** use `clamp` here — see the panic in §4.

---

## 8. Engine cold start — the one thing still blocked

`pool_boot_ms` is 9–20 s. You pay it on the first `preloop run` after a host
boot, an upgrade, or an engine crash. The HTTP surface comes up immediately, so
the run simply queues until a runner exists.

Measured breakdown of a typical ~9 s:

| phase | cost |
|---|---|
| `machine create` golden | 19 ms |
| `machine start` — SmolVM pulls `ubuntu:24.04` *inside the fresh VM* | ~1.7 s |
| guest readiness poll | 67 ms |
| `apt-get update && install git curl ca-certificates nodejs` | **~6 s** (3.1–21 s by mirror) |
| fork + configure the warm pool | ~0.7 s |

The 9→20 s spread is almost entirely apt and CDN variance. It is the only
remaining network dependency and the only nondeterministic part of the system.

### The packed-machine blocker

With strict egress, the golden must carry `--mount-socket`: the host Unix
socket is the runner's only route to the local control plane. Preloop also
volume-mounts the socket's parent directory at `/run/preloop-control`; that
ordinary virtiofs mount makes the path visible, while `--mount-socket` supplies
the connectable endpoint. The volume alone is insufficient.

The installed SmolVM 1.6.13 registry-image path preserves that combination and
is the path used by every successful benchmark above. `create --from
x.smolmachine` does not. This is not merely inferred from a missing path:

1. An Ubuntu 24.04 VM was prepared with git, curl, CA certificates, and Node,
   stopped, and packed into a 122.6 MB `.smolmachine` artifact.
2. A clean preloop engine accepted that artifact as
   `PRELOOP_RUNNER_BASE_IMAGE`, created the golden, and forked runner VMs.
3. Every runner registration repeatedly failed while posting to the local
   control origin; a `preloop run` remained queued and timed out after 300 s.
4. Current upstream source (`src/cli/machine.rs`, commit `a31810e`) parses CLI
   socket flags into `params.published_sockets` on normal `--image` creation,
   but `run_from_smolmachine` hard-codes `published_sockets: Vec::new()`.

That source difference is the root cause: the CLI accepts `--mount-socket`,
machine creation succeeds, and the option is discarded before the VM record is
built. The same `--from` branch handles `.smolmachine` artifacts pulled from a
registry, so both local and registry pack references are affected.

Earlier exploratory notes also grouped `--image <rootfs-dir>` and
`--image <OCI-archive>` with this defect. Do not use that as an upstream claim:
current upstream source routes those through the normal creation branch, which
does parse `--mount-socket`. Their SmolVM 1.6.13 runtime behavior should be
retested separately after the packed-machine fix.

`machine update` can change volumes, ports, env, CPU, memory and disks but not
sockets. `machine fork` takes only `--golden`, `--name`, `--forkable`,
`--share-weights`, `-p` — so a clone cannot add one either, and clones inherit
whatever the golden had.

Two alternative channels were tested and are closed:

* **TCP to the host.** libkrun forwards guest loopback to host loopback, so this
  would work — but `SMOLVM_EGRESS_FLOOR=strict` refuses it with `EACCES`.
  `SMOLVM_EGRESS_ALLOW_PRIVATE=1` does not lift it; `--allow-cidr 127.0.0.0/8`
  breaks the in-guest image pull.
* **Volume-mount the directory containing the socket without
  `--mount-socket`.** The socket file becomes *visible* in the guest but is not
  connectable — virtiofs passes the inode, not the endpoint.
  `curl --unix-socket` fails with exit 7. Preloop intentionally supplies both
  the directory volume and the published-socket mapping.

The failure is silent: `create` succeeds, the machine boots, and you only
discover the socket is missing later. *[Inference]* that reads like an oversight
in those code paths rather than an intentional restriction, but intent cannot be
established from outside.

### Packed artifact creation and extraction

Producing a prepared golden is cheap and verified:

* `tar --one-file-system` in-guest → 310 MB rootfs. **Without
  `--one-file-system` the tar follows the storage mount and produces 20 GB.**
* `machine cp` pulls it at 240 MB/s (~1.3 s).
* Synthesizing a `docker save`-shaped archive (manifest + config + layer) takes
  **91 ms** in Python.
* `machine create --image golden.tar` + `start` = ~1.3 s in an earlier local
  image experiment, tools already present. Retest socket forwarding on this
  branch separately; it is not the same source path as `--from`.

A packed artifact does **not** currently imply a sub-second engine cold start.
`machine start` on an already extracted packed VM measured 0.28 s, but a first
`create --from` extracted the artifact into the new machine's own directory and
took 18.1 s even for a 19 MB Alpine pack. The prepared Ubuntu pack produced
golden and clone VMs after roughly 20 s. Therefore the socket fix makes packed
goldens functional; it is not, by itself, proof of a cold-start win. A shared
extraction cache or a reusable golden would still be needed.

### Options

| # | option | effect | needs |
|---|---|---|---|
| 1 | Publish an Ubuntu base image with git/curl/ca-certificates/node baked in | 9 s → **~3 s**, keeps `ubuntu-latest` parity | nothing external |
| 2 | Upstream: preserve CLI `--mount-socket` / `--expose-socket` in `run_from_smolmachine` | makes packed machines functional with host services; latency depends on extraction | smolvm change |
| 2b | Upstream alternative: a scoped egress allowance for one host port | removes the local control socket requirement, but changes the isolation mechanism | smolvm change |
| 2c | Cache extracted pack assets across machine records | removes the measured first-create extraction cost | smolvm change |
| 3 | Persist the golden across engine restarts | helps restarts only, not reboots | leaves a ~1 GB VM alive after the daemon exits; needs `preloop stop` |
| 4 | Start golden prep at login/daemon autostart | hides it rather than removing it | product decision |
| 5 | Do nothing | it is once per boot | — |

Recommendation: **1 for cold-start latency now; 2 for packed-machine
correctness; investigate 2c before claiming packed-machine latency.** Option 4
can hide whatever unavoidable preparation remains.

Do not rely on a running VM surviving replacement of its host socket file. A
controlled unlink-and-rebind test caused subsequent guest connections to fail;
the previous claim that the host path was resolved per connection was wrong.

Caveat on measuring any of this: `pool_boot_ms` swings 9–20 s on network alone
and cannot resolve anything under a few seconds. Measure the apt step directly,
as was done for the 2 s sleep removal, where the metric could not see a real
1.9 s win.

---

## 9. SmolVM behaviours worth knowing

Gathered the hard way; all verified on 1.6.13.

1. **The base image is pulled inside every freshly created VM on `start`**, not
   cached host-side. Docker Hub rate-limits this after a few dozen boots.
2. `SMOLVM_EGRESS_FLOOR=strict` (set unconditionally by `SmolVmProvider`) blocks
   guest→host loopback. Without it, guest `127.0.0.1:<port>` transparently
   reaches the host.
3. `machine create --from` silently drops `--mount-socket` and
   `--expose-socket`: current upstream `run_from_smolmachine` sets
   `published_sockets: Vec::new()` even though the normal branch parses the CLI
   flags. Local OCI archive/rootfs paths use a different branch and need
   separate runtime validation.
4. `machine fork` has no mount flags; clones inherit the golden's mounts.
5. A third virtiofs mount on the golden destabilised forks (§5).
6. Replacing the host socket file under a running VM broke subsequent guest
   connections in a controlled test; restart or remount instead of assuming
   per-connection path resolution.
7. `exec` and `exec --stream` cost the same (~50 ms). **`--stream` output is
   capped at 11 MB** — bulk transfers must use `machine cp`.
8. The guest accepts `exec` ~67 ms after `machine start --forkable`, on the
   first attempt. The old fixed 2 s sleep was pure waste.
9. `machine status` reads the on-disk VM directory while `ls`/`delete` read the
   SQLite registry. A killed engine leaves them disagreeing, and every later
   provision fails. The harness reaps orphaned directories and `_boot-vm`
   processes for this reason.
10. An unpacked-rootfs image has no OCI config, so `start` fails with "no
    command given and image defines no entrypoint or cmd" unless a workload
    command is supplied — which then forces the container path.

---

## 10. Ideas considered and rejected

| idea | why not |
|---|---|
| Loose-file snapshot refs instead of `git update-ref` | −6 ms, ~1 % of e2e, and hand-rolls a git internal |
| `git commit` to collapse write-tree/commit-tree/update-ref | porcelain rejects `core.bare` + `GIT_WORK_TREE` |
| Share one RSA keypair across the pool | any job could decrypt another runner's job messages; the ahead-of-time pool already removed the cost |
| Bake a keypair into the golden | every fork would inherit the same one |
| Mount `/var/cache/apt/archives` | filling it through virtiofs pushed apt install 4.2 → 20.4 s, and the extra mount destabilised forks |
| Switch the base image to `node:22-slim` or similar to skip apt | trades Ubuntu parity with `ubuntu-latest` for ~6 s once per boot. Baking our own Ubuntu image (§8 option 1) gets the same saving without the trade |
| Batch per-step log uploads | 1.3 ms/step, and the ordering is part of runner fidelity |
| Optimise `replenish_wait_ms` further | off the user's critical path — eight back-to-back runs measured the same as gated runs |
| Make the CLI's two `git` spawns concurrent | ~9 ms against a ~35 ms noise floor |
| Persist goldens across engine restarts | see §8 option 3 — a product decision, narrow benefit |

---

## 11. Open items

**`inner.logs` grows in RAM for the engine's lifetime** — ~170 KB/job, 27 MB
after 160 jobs, so roughly 1 GB for a week-long engine. It is what makes
`get_run_logs` fall back once a replay blob has been pruned, so bounding it
requires deciding how long a finished run stays queryable. Not urgent; not a
unilateral call.

**Everything else in the measured path is below the noise floor**: submit
~43 ms, dispatch ~17 ms, CLI ~20 ms.

---

## 12. Reproducing

```sh
bash autoresearch.sh                          # primary metric
PRELOOP_BENCH_MATRIX_JOBS=8 bash autoresearch.sh   # width sweep
just test-ci                                  # fmt + clippy -D warnings + tests
```

Harness knobs: `PRELOOP_BENCH_MATRIX_JOBS`, `PRELOOP_BENCH_CLI_RUNS`,
`PRELOOP_BENCH_API_RUNS`, `PRELOOP_BENCH_SINGLE_RUNS`,
`PRELOOP_BENCH_HOST_RUNS`, `PRELOOP_BENCH_LISTEN`, `PRELOOP_BENCH_CACHE`,
`PRELOOP_BENCH_BASE_IMAGE`.

The harness stops any conflicting `preloop engine`, deletes `preloop-runner-*`
VMs and reaps orphaned hypervisor processes before starting. It does not touch
any other VM on the host.
