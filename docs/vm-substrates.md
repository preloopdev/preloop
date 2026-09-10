# VM substrates

Preloop executes every job in a microVM. Two substrates are supported behind
one trait, [`preloop_vm::VmProvider`]:

| Backend | Runtime | Hosts | Selected by |
|---|---|---|---|
| `smolvm` | [SmolVM](https://github.com/smol-machines/smolvm) — libkrun (Hypervisor.framework on macOS, KVM on Linux) | macOS arm64, Linux arm64/x86_64 | default off Linux, or `PRELOOP_VM_BACKEND=smolvm` |
| `agentenv` | [AgentENV](https://github.com/kvcache-ai/AgentENV) — a Firecracker control plane with overlaybd images and ublk storage | Linux ≥ 6.8 with `/dev/kvm` | default on such a host when `aenv` is on `PATH`, or `PRELOOP_VM_BACKEND=agentenv` |

`PRELOOP_VM_BACKEND` is authoritative; an unrecognized value is a hard error
rather than a silent fallback, because booting jobs on a substrate the operator
did not ask for is exactly the surprise a typo must not cause. The engine logs
`VM substrate selected` with the resolved backend at startup.

## Why AgentENV is the default on KVM

On a KVM host, AgentENV is the faster substrate for the runner-pool lifecycle
— boot, fork, pause, and resume — because a snapshot restore replaces a boot
and its snapshots are immutable, so concurrent forks do not serialize on a
single retained checkpoint the way SmolVM's do.

Guest CPU is equivalent in the controlled same-host measurements. Guest I/O is
workload- and path-specific rather than a general AgentENV win: the same-host
HPC control found SmolVM 1.35–3.12× faster on the matched steady-state fio
workloads, while AgentENV is 4.3× faster at per-file fsync in a clone and wins
some tar/artifact paths. AgentENV's clone first-touch cost can make unpacking
and tree copies slower until the changed blocks are materialized.

Note that SmolVM mounts a tmpfs on `/tmp` and AgentENV does not, so any
measurement that writes there compares RAM against a block device. Compare on
the job workspace instead.

The measured numbers, including the same-host HPC control, the end-to-end
campaign, and the open follow-ups live in an internal report —
`benchmarks/substrates/REPORT.md`, alongside the harnesses and raw samples —
because it carries host-specific infrastructure detail. Run
`benchmarks/substrates/micro-bench.sh` to reproduce the primitives on your own
host.

## What the provider maps onto

`crates/preloop-vm/src/agentenv.rs` shells out to the `aenv` CLI, mirroring how
`SmolVmProvider` drives `smolvm`. The mapping:

| `VmProvider` | AgentENV |
|---|---|
| `create` | nothing — records the spec; AgentENV has no defined-but-unstarted sandbox |
| `start` | `aenv start --cold <image> --cpu --memory --disk-size-mb -d`, or `aenv resume` for a paused sandbox |
| `start_forkable` | `start`; the snapshot is taken lazily by the first `fork` |
| `fork` | `aenv snapshot create <golden> --name <snap>` once, then `aenv start <snap> -d` per clone |
| `stop` | `aenv pause` — the sandbox keeps its id, so `start` resumes it |
| `delete` | `aenv delete` |
| `status` | `aenv ls --output json`, matched on the recorded sandbox id |
| `list` | the local registry |
| `exec`, `exec_stream` | `aenv exec <id> -- argv` |
| `exec_with_secret_env` | secret file uploaded 0600, sourced by a wrapper shell, unlinked before the payload runs |
| `copy` | `aenv upload` / `aenv download` |
| `pack` | `aenv snapshot create --name`, plus a JSON descriptor at the output path |
| `rearm_fork_base` | always succeeds — AgentENV snapshots are persistent and re-forkable |

## Capability differences that change engine behaviour

`VmProvider::capabilities()` reports what a backend can express, and the
orchestrator branches on it instead of assuming SmolVM's semantics.

* **No host bind mounts** (`live_host_volumes: false`). AgentENV volumes are
  block devices (`aenv volume create`, attached as `--volume /guest=<vol>`);
  there is no virtiofs/9p host-directory passthrough in the CLI or the HTTP
  API. `MachineSpec::volumes` is therefore *materialized*: the host directory
  is streamed in as one tar archive at start time, because `aenv upload`
  rejects symlinks and does not preserve modes, and because one transfer beats
  thousands of gRPC round trips. Fork clones inherit an already-unpacked
  filesystem from the snapshot, so the cost lands on the golden only.
  A read-only mount is approximated by stripping write bits in the guest;
  that is weaker than virtiofs `:ro`, and there is no host directory behind it
  to protect in the first place.
  **Operational consequence:** keep `PRELOOP_RUNNER_BUNDLE` minimal. Pointing
  it at a 1.5 GiB `target/release` makes every golden pay for the whole
  directory (observed: golden preparation stalls for minutes), where SmolVM
  would mount it for free.
* **No host Unix-socket passthrough** (`socket_mounts: false`). A spec carrying
  `sockets` is rejected with an error naming the remedy. The engine therefore
  runs the guest control plane over TCP: `preloop serve` forces
  `control_socket = None` on this backend, and the runner uses the plain TCP
  transport it already supports. Set `PRELOOP_RUNNER_URL` to an address the
  guest can reach (the host's LAN address, not loopback).
* **Packs are not files** (`file_packs: false`). `pack` creates a server-side
  snapshot and writes a small JSON descriptor naming it (at both `<output>` and
  the `<output>.smolmachine` sidecar the orchestrator looks for). The
  descriptor is portable only within that AgentENV server. The pool therefore
  skips artifact build/download entirely on this backend and prepares its
  golden straight from the base image, then forks per job:
  `use_packed_artifact` is forced off and `use_fork` on.

## Identity, TTL, and the registry

Sandbox ids are server-assigned; AgentENV accepts no caller-supplied name. The
provider keeps a `MachineName` → sandbox mapping in
`$PRELOOP_HOME/agentenv-machines.json` (0600, atomically replaced). Like the
store, it is a **restart source, not the source of truth**: the live server is
authoritative, and `status` reconciles a sandbox the server no longer knows
(expired TTL, out-of-band delete) back to `Stopped` so the next `start`
provisions a replacement instead of resuming something that would 404.

`preloop shell` and `preloop debug` run in a different process from the engine,
so they read that registry to find the sandbox behind a preserved machine name
(`crates/preloop-cli/src/main.rs`: `guest_exec_command`, `guest_upload_command`,
`guest_shell_command`). If the registry is gone, those commands say so instead
of failing obscurely.

Attach resumes a suspended sandbox: `resume_guest_if_needed` probes the guest
with `/bin/true` and, when it does not answer, runs `aenv resume --timeout`
before `guest_shell_command`/`guest_exec_command` proceed. The engine-side
watcher does the same when a debug-session marker appears, so both the
interactive CLI and the orchestrator can reach a parked sandbox
(measured: resume 0.09 s + first exec 0.05 s on `cpane`).

Every AgentENV sandbox has a deadline (upstream default 300 s). A CI job
outliving it would be killed mid-run, so each running sandbox gets a keepalive
task that re-arms the TTL at a third of its length. `PRELOOP_AENV_TTL_SECS`
(default 3600, minimum 60) sets the window; it only bounds how long an
*abandoned* sandbox survives a crashed engine.

## Disk sizing

AgentENV materializes a guest root from an overlaybd base and refuses to
present it smaller than the base's virtual size — Ubuntu 24.04 resolves to
64 GiB, so a preloop spec asking for 20 GiB is rejected with
`requested overlaybd runtime virtual size … is smaller than base virtual size …;
shrinking is disabled`. Preloop's `--storage` is a sparse ceiling written
against SmolVM, so the provider treats it as a floor: the cold start is retried
once without `--disk-size-mb`, inheriting the larger base size, rather than
under-provisioning or failing a spec that is merely smaller than the base.

## Environment reference

| Variable | Effect |
|---|---|
| `PRELOOP_VM_BACKEND` | `smolvm` \| `agentenv`. Authoritative; invalid values error |
| `PRELOOP_AENV_BINARY` | path to the `aenv` executable (default: `aenv` on `PATH`) |
| `PRELOOP_AENV_TTL_SECS` | sandbox TTL requested and re-armed (default 3600) |
| `PRELOOP_RUNNER_URL` | must be guest-reachable on this backend (no socket relay) |
| `PRELOOP_RUNNER_BUNDLE` | keep it small: it is copied into every golden |

## Prerequisites on the host

1. Linux ≥ 6.8 with `/dev/kvm` and cgroup v2.
2. AgentENV server and CLI installed, service running:
   `curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install.sh | sudo bash`
   then `sudo systemctl start aenv`.
3. The `aenv` CLI authenticated **as the user the engine runs as** — AgentENV
   reads credentials from `~/.config/aenv/credentials` (TOML: `url`,
   `api_key`), not from the environment. The server's key is at
   `/var/lib/aenv/secrets/api-key`. For a systemd-managed engine, write that
   file into the service user's home.
4. **The guest must be able to reach the engine.** This is not optional
   plumbing — without it every job is failed by the starvation sweep after
   600 s with "no matching runner", because the runner inside the sandbox
   never registers. AgentENV is hardened against exactly this: each sandbox
   netns carries an `AGENTENV-EGRESS` floor that REJECTs every
   RFC1918/CGNAT/loopback/link-local destination (so an agent can reach the
   internet but never a host service), and the host's own `INPUT` chain gets
   `-s <internal pool> -i veth-+ -j REJECT`. Two gates must open, and only for
   the engine:

   * `[network.egress].always_denied_cidrs` in `/var/lib/aenv/config/config.toml`
     is a deny list evaluated *before* per-sandbox `allowOut`, so the only way
     to permit one address is to stop denying the CIDR that covers it. Replace
     the covering range with its exact complement around the engine's `/32`
     rather than dropping `192.168.0.0/16` wholesale, which would expose the
     whole LAN to every sandbox.
   * `iptables -I INPUT 1 -s 10.11.0.0/16 -p tcp --dport <engine port> -j ACCEPT`
     (and the same for `10.12.0.0/16`, AgentENV's other internal pool).
     **Re-apply after every `systemctl restart aenv`:** AgentENV re-inserts its
     own REJECT at the head of `INPUT` on each start, shadowing an allow rule
     added earlier.

   `benchmarks/substrates/aenv-egress-allow-host.sh <host-ip> <port>` performs
   both steps and backs up the config first. Verify with
   `benchmarks/substrates/verify-egress.sh`, which asserts the intended
   posture from inside a sandbox: engine reachable, other LAN hosts blocked,
   internet reachable.

   SmolVM needs none of this — its guests can reach host services by default —
   so this is a real operational difference, and arguably AgentENV's is the
   safer default.

## Verification

* `cargo test -p preloop-vm` — contract tests for both providers, including
  `tests/agentenv_provider.rs`, which asserts the exact `aenv` argv the
  provider emits against a recording fake (a wrong flag here is a job that
  never boots).
* `benchmarks/substrates/micro-bench.sh` — substrate primitives, both backends.
* `benchmarks/substrates/e2e-bench.sh` — full CI runs through the engine on one
  substrate; run once per backend, never concurrently.
