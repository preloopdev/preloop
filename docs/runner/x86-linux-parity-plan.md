# x86 Linux Parity Test Plan

Plan for validating aksh-runner on x86_64 Linux hosts (`vm103`: Intel i5-4570 @ 3.20GHz, 4 vCPU, 15GB RAM, Ubuntu 24.04).

---

## 1. Infrastructure Validated

| Component | Status | Details |
|-----------|--------|---------|
| `aksh-runner` (release) | ✅ Builds on x86_64 | `cargo build --release` succeeds; 130 missing-docs warnings (expected) |
| `cargo test --workspace` | ✅ 222 tests pass | All unit/integration tests pass natively on x86_64 |
| `cargo clippy` | ✅ Clean after fixes | Fixed `useless_format`, `needless_borrows_for_generic_args`, `needless_lifetimes` |
| `smolvm` v1.4.5 (x86_64) | ✅ Installed at `/opt/smolvm` | Requires `kvm` group; `machine run --net --image alpine -- echo hello` works |
| `firecracker` v1.16.1 (x86_64) | ✅ Installed at `/usr/local/bin` | Requires `kvm` group; boots kernel to user-space in ~127ms |
| Docker | ✅ Available | `docker ps` works; user in `docker` group |
| SSH ControlMaster | ✅ Configured | Persistent multiplexed connection to vm103 (10min persist) |

---

## 2. MicroVM Benchmark Results (Nested Virtualization)

**Important**: vm103 is itself a VM (Hetzner cloud), so all KVM operations are nested virtualization. Native bare-metal would be significantly faster.

### SmolVM (libkrun-based, v1.4.5)

| Metric | 1 vCPU / 1024MB | Notes |
|--------|-----------------|-------|
| Cold Start (clean overlay, image cached) | ~1.3–1.5s (3/5 passed) | 2 runs hit 30s agent-ready timeout |
| Warm Start (stopped → running, overlay cached) | ~1.1–2.5s | Median ~1.9s; occasional spikes to ~5s |
| Full ephemeral `machine run` | ~5.8s | Includes image pull from cache |
| `pack create` | >3min | Copies 20GB storage template; impractical on slow disk |

### Firecracker (v1.16.1)

| Metric | Value | Notes |
|--------|-------|-------|
| VMM start → kernel first line | ~117ms | `[0.000000] Linux version 6.1.175` |
| VMM start → `/bin/sh` (user-space) | ~127ms | `init=/bin/sh` boot arg |
| Total cold start (including API setup) | ~150ms | Socket + 3 PUT requests + InstanceStart |

### Comparison Summary

| | SmolVM | Firecracker |
|---|---|---|
| Boot to ready | **~1.3s** | **~127ms** (10× faster) |
| OCI image support | ✅ Native | ❌ Needs rootfs image |
| virtio-fs host sharing | ✅ | ❌ |
| `exec` into running VM | ✅ | ❌ (needs vsock agent) |
| macOS support | ✅ | ❌ |
| Nested virt overhead | High (libkrun + virtio-fs) | Low (minimal device model) |
| Production multi-tenant security | ⚠️ Weaker (no jailer) | ✅ (jailer + seccomp) |

---

## 3. Runner E2E Tests on x86_64

### Passed

| Test | Result |
|------|--------|
| `hello-world.yml` (simple echo) | ✅ `{"status":"success","success":true}` |
| `fixtures/fidelity/runner-fidelity-f042-f047.yml` — F042 (SIGINT handling) | ✅ Step succeeded in ~0.8s |
| `cargo test --workspace --quiet` | ✅ 222/222 passed |

### Blocked (not runner bugs)

| Test | Blocker |
|------|---------|
| `fixtures/fidelity/runner-fidelity-f046-remote.yml` | References `preloopdev/aksh@cachingv4` on GitHub; repo doesn't exist at that path. Needs local fixture path. |
| Docker container jobs | `runner-e2e` doesn't populate the workspace (no `git clone`), so `uses: ./fixtures/...` fails. Need to either: (a) add workspace setup to runner-e2e, or (b) create inline Docker test workflows. |
| Multi-job workflows | `runner-e2e` runs with `--once` (processes 1 job then exits). Multi-job workflows show `"status":"in_progress"`. |

---

## 4. What's Missing / Recommended Next Steps

### Priority 1: x86 Container Validation
- [ ] Create a self-contained Docker test workflow that doesn't require `actions/checkout` (inline Dockerfile via `docker build` in a script step)
- [ ] Test `docker run` with x86_64 images (alpine, ubuntu, node) to confirm native execution
- [ ] Test container job (`container: { image: ... }`) syntax via the runner
- [ ] Validate Docker action lifecycle (pre/main/post entrypoints) with local fixtures

### Priority 2: SmolVM Runner Integration
- [ ] Boot smolvm x86_64 VM with aksh-runner binary pre-loaded
- [ ] Register runner from inside smolvm against local aksh-server
- [ ] Run hello-world workflow end-to-end inside smolvm
- [ ] Measure total job execution time (boot + register + run + report)

### Priority 3: Firecracker Runner Integration (Stretch)
- [ ] Build custom rootfs with aksh-runner + deps (bash, git, docker CLI)
- [ ] Boot Firecracker VM, register runner via vsock/network
- [ ] Compare total job time vs smolvm

### Priority 4: Stress / Scale Testing
- [ ] Run matrix workflow (4+ jobs) sequentially through runner
- [ ] Measure memory/CPU under sustained job load
- [ ] Test cancel/timeout behavior under load
- [ ] Compare aksh-runner RSS vs official runner RSS on same workload

### Priority 5: Conformance Parity
- [ ] Run full `runner-watch conform` suite on x86_64 (currently only run on ARM64 Mac)
- [ ] Verify golden replay scenarios match on x86_64
- [ ] Run upstream fixture corpus through `aksh-gha-parser` for panic testing

---

## 5. Known Limitations

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| vm103 is nested virtualization | SmolVM/Firecracker slower than bare-metal | Benchmark numbers are floor estimates; bare-metal would be 2-5× faster |
| SmolVM `pack create` copies 20GB template | Takes >3min, impractical for CI | Use `machine create` + `machine start` instead of packed binaries |
| SmolVM agent-ready timeout (30s) | ~40% cold start failure rate at default resources | Use `--cpus 1 --mem 1024` to reduce resource contention |
| `runner-e2e` runs single job | Can't validate multi-job workflows | Need orchestrator enhancement or multiple `--once` invocations |
| No x86 Rosetta on smolvm/macOS | Can't run x86 images on ARM64 dev machines | x86 testing must happen on x86 hosts like vm103 |
