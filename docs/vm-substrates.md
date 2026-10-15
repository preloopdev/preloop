# VM substrates

Preloop executes every job in a microVM obtained from the
[Sandbox Service](sandbox-service.md). Substrates are drivers attached to a
host agent; the runner pool never talks to one directly. Placement uses the
capabilities declared by a driver, not its name.

| Driver | Runtime | Hosts | Build |
|---|---|---|---|
| `smolvm` | [SmolVM](https://github.com/smol-machines/smolvm) — libkrun (Hypervisor.framework on macOS, KVM on Linux) | macOS arm64, Linux arm64/x86_64 | public (default) |
| private backends | backend-specific; see the private repository | private hosts | private build |

The public build ships the `smolvm` driver. AgentENV and other private drivers
are supplied by the private build; they are not part of the public binary.
`PRELOOP_SANDBOX_DRIVERS` lists the drivers a host attaches (default `smolvm`),
and an unknown name is a hard error rather than a silent fallback.
`PRELOOP_SANDBOX_POOL_DRIVER` picks the driver on which the CI pool places jobs.

## SmolVM specifics

* **Driver model**: the `smolvm` CLI
  (`crates/preloop-sandbox/src/drivers/smolvm`). `smolvm serve` has native fork
  pools, but its clones lose `--mount-socket` relays (the socket file exists but
  connections are reset), and preloop mounts its control socket into every
  guest. CLI branches keep the relay, so the driver stays on the CLI and
  pooling is the service's.
* **Naming**: a caller-named sandbox (the CI pool names its machines) uses that
  name as the SmolVM machine name, so `preloop shell` / `preloop debug` and
  `smolvm machine exec` address it directly.
* **Agent rootfs**: the rootfs bundled beside the resolved `smolvm` binary wins
  over the platform data dir. A stale copy there (left by an older install)
  boots guests whose agent refuses newer operations — observed as every delete
  failing with "running guest agent does not support safe shutdown".
* **Egress**: `PUBLIC_ONLY` uses virtio-net with `SMOLVM_EGRESS_FLOOR=strict`;
  `PRELOOP_SMOLVM_NET_BACKEND=tsi|virtio-net` overrides.

Guest `/tmp` is tmpfs on SmolVM; compare I/O on the job workspace instead.

## Shutdown and crash recovery

A `preloop serve` stop (SIGTERM, or launchd bootout and a restart) first lets
the pool tear down: every slot stops its runner machine, the golden fork bases
are *stopped* (not deleted) so the next start adopts them instead of rebaking,
and the stale-machine sweep deletes anything else this pool owns. The
preparation phases observe the same cancellation token, so a signal that arrives
while the golden is unpacking or a runner is booting does not hold teardown
behind them. Whatever a cut-short teardown leaves behind is reaped as the
engine process exits, and again at the next engine start.

Two host-level facts make that reaping necessary, because both look like "the
VM is gone" when it is not:

* `_boot-vm` is *detached*: Preloop's `smolvm machine …` invocations exit after
  a boot, and the hypervisor keeps running as a parentless process, reachable
  only through the SmolVM registry.
* On macOS a packed machine's layer store is an APFS sparseimage mounted at
  `<machine data dir>/pack/layers-cs` for the life of the VM. `machine
  stop`/`delete` detach it; a hypervisor that died for any other reason leaves
  it attached, and the mount point is inside the home, so the home cannot be
  removed (`Resource busy`).

Startup, shutdown, and process exit therefore SIGKILL the `_boot-vm` processes
whose boot-config path is under this home's machine data root
(`preloop_sandbox::drivers::smolvm::cli::purge_orphaned_vms`), then detach the
layer mounts no live hypervisor claims
(`preloop_sandbox::drivers::smolvm::cli::detach_orphaned_layer_mounts`).
Ownership is a parsed path compared component-wise against the data root, so a
neighboring home whose path merely shares a prefix (`…/smolvm` vs
`…/smolvm-vm-other`) is never matched; a mount belonging to a running machine
is skipped, and `hdiutil` refuses a busy volume, so the reap is safe while the
engine serves. To reset a home by hand after an engine was SIGKILLed, run those
two steps before `rm -rf`: deleting the registry row first makes `machine stop`
unable to reach the leaked VM.

## Verification

* `cargo test -p preloop-sandbox` — includes `tests/smolvm_cli.rs`, the exact
  `smolvm` argv against a recording fake.
* `PRELOOP_SANDBOX_E2E_SMOLVM=1 cargo test -p preloop-sandbox --test smolvm_e2e`.
* `benchmarks/substrates/` — historical substrate measurements, including the
  retained AgentENV results from before the Sandbox Service cutover.
