# Sandbox Service

Preloop gets every VM from a Sandbox Service: a generic service that hands out
running machines from pluggable backends. The CI runner pool is one client;
`preloop sandbox …` The contract is
[`proto/preloop/sandbox/v1/sandbox.proto`](../proto/preloop/sandbox/v1/sandbox.proto);
the implementation is `crates/preloop-sandbox`, which never depends on the CI
control plane.

```text
callers ──Sandboxes API──▶ Sandbox Service ──Providers API──▶ host agents / connectors
(CI pool, CLI, agents)     templates, pools,                   drivers: smolvm, …
                           leases, placement,
                           reconcile (own schema)
```

## Two APIs

* **Sandboxes** (callers): `PutTemplate`, `PutPool`, `Acquire` (idempotency key,
  pool or template, identity to inject), `Renew`, `Release` (destroy or
  preserve), `Exec` (server-streaming), `OpenStream` (data-plane ticket),
  `Snapshot`, `Pause`/`Resume`, `Get`/`List`, `Stats` (read-only per-provider
  host and sandbox aggregates).
* **Providers** (capacity owners): `Register` (drivers + capabilities →
  provider id and fencing generation), `Heartbeat`, `Poll` (long-poll for
  work), `Report` / `ReportExec` (fenced by generation), `Reconcile` (full
  inventory: lost sandboxes fail, orphans and stale incarnations are destroyed).

Both are served with the Connect protocol, JSON codec, over one HTTP port:
`POST /preloop.sandbox.v1.<Service>/<Method>`. Every RPC is unary or
server-streaming, so it works over HTTP/1.1 — proxies, and hosts behind NAT that
dial out. Bulk bytes never ride inside RPCs: `OpenStream` returns a short-lived
ticket and a tar moves with plain `PUT`/`GET` on `/sandbox/v1/streams/<ticket>`;
agents fetch and return blobs on `/sandbox/v1/agent/blobs/<ticket>`.

The Connect layer is hand-rolled (`crates/preloop-sandbox/src/connect.rs`):
adding `connectrpc` would bring its own protobuf runtime, while the subset used
here — JSON unary and server streaming — is small. Any Connect client generated
from the `.proto` interoperates.

## Model

* **Template** — a logical image: an OCI image (optionally per architecture), a
  driver-specific `recipe` interpreted by the selected driver, or a snapshot
  captured by `Snapshot` (then bound to the provider and driver that hold it).
* **Pool** — template + shape + network + mounts + warm target + max. The
  service keeps `warm` idle sandboxes per pool (the generic warm pool) and
  refills after each claim, backing off after creation failures.
* **Stop / Start** — `Stop` powers a machine off but keeps it and its disk
  (packing needs a stopped machine), `Start` boots it again, `--forkable` when
  it must serve forks. Both work under the same lease.
* **Sandbox / Lease** — `Acquire` with the same idempotency key returns the same
  lease. Lease environment is merged into every exec; injected files are written
  before the lease is returned. Leases expire (the sandbox is destroyed);
  `Release(PRESERVE)` keeps a sandbox for debugging until its window ends.

Capabilities decide placement; nothing branches on driver names. A request no
registered provider can satisfy fails with the unmet requirement named
(`no provider satisfies template "mac": host/smolvm: os=macos`), and an
operation a backend does not declare (`Pause`, `Snapshot`) fails with
`failed_precondition` instead of being emulated. `one_shot` backends run one
command per sandbox; callers needing several execs require `multi_exec`.

## Drivers

A driver is the host-side, in-process interface a host agent drives
(`crates/preloop-sandbox/src/driver.rs`). It is not a public plugin ABI: third
parties integrate by implementing the Providers protocol.

| Driver | Where | Model |
|---|---|---|
| `smolvm` | public | CLI. Golden-fork: a build sandbox starts `--branchable`; `Snapshot` keeps it as the fork base; sandboxes from the snapshot template are `machine branch` clones. |
| `fake` | public | Process-backed, for tests and conformance. |
| Private drivers | private build | See the private repository for driver-specific implementations. |

`smolvm serve` has native fork pools, but its clones lose `--mount-socket`
relays (the CLI's branches keep them), so the driver stays on the CLI and pools
are the service's.

## Security model

* Data-plane tickets are single-use, scoped to one sandbox and direction, and
  short-lived; the URL carries its own token.
* `Report`/`ReportExec`/`Reconcile` are fenced by the provider's registration
  generation, and a provider may not report output for another provider's
  exec, nor destroy another provider's machine.
* **The provider token authenticates a role, not an identity.** Any holder can
  register any provider *name*, which bumps that name's generation and fails
  its in-flight work, poll its queue, or report its work items. Deployments
  that hand the provider token to more than one trusted operator need
  per-provider identities (a follow-up); on a single-operator tailnet the
  shared token is the trust boundary, exactly like the engine token.

## State

The service keeps its working set in memory and writes through to its own
store, the restart source: SQLite (`<state>/sandbox.db`, default) or a
`sandbox` schema in Postgres (`PRELOOP_SANDBOX_STORE_URL=postgres://…`). It
never references control tables.

## Observability

The service records its own `preloop.sandbox.*` instruments on the process's
shared meter (`Observability::meter()`), so a standalone `preloop sandbox
serve` and the in-process engine export the same set: `/metrics` replaces dots
with underscores, appends `_seconds` to durations, and `_total` to counters.
Every value is recorded from the service's own state transitions — callers
never push numbers in — so a service with no caller still reports accurate
gauges.

**Inventory**

* `preloop.sandbox.sandboxes` (gauge, `state`/`provider`/`template`) — live
  sandboxes per combination, recorded from the service's own inventory on
  every maintenance tick. A combination that disappears is recorded as zero
  on the next tick instead of keeping its last value, so a drained pool
  reads as `0`.
* `preloop.sandbox.pool.warm_depth` (gauge, `pool`) — idle, unleased
  sandboxes currently held for a pool.
* `preloop.sandbox.heartbeat.truncated` (counter) — per-sandbox runtime
  entries a heartbeat dropped because the host runs more than the
  per-heartbeat cap.

**Acquire, creation, and leases**

* `preloop.sandbox.acquire.duration` (histogram) — acquire latency from
  request to a ready sandbox or failure.
* `preloop.sandbox.acquire.failures` (counter, `reason`) — failed acquires by
  Connect error code; `failed_precondition` is an unmet placement requirement.
* `preloop.sandbox.create.failures` (counter, `reason`) — `timeout` (the
  provider never reported) or `reported` (it reported a failure).
* `preloop.sandbox.lease.transitions` (counter, `outcome`) — `renewed`,
  `expired`, or `preserved`.

**Pools and reconcile**

* `preloop.sandbox.pool.refill_failures` (counter, `pool`) — warm-pool
  refills that could not place a sandbox.
* `preloop.sandbox.reconcile.lost` (counter) — sandboxes a provider's
  inventory no longer reports; they are failed as lost.
* `preloop.sandbox.reconcile.orphans` (counter) — provider-observed machines
  no record owns, sent back for destruction as orphans or stale incarnations.

**Exec**

* `preloop.sandbox.exec.duration` (histogram) — request to the terminal exit
  event.
* `preloop.sandbox.exec.exit` (counter, `class`) — `success`, `failure`,
  `error`, or `unknown`.

Acquire, Release, Exec, and Reconcile also emit tracing spans carrying the
pool, template, sandbox, or lease being touched.

### Runtime stats

Host and per-sandbox facts only the owning driver can measure travel on the
provider's `Heartbeat`: a `HostReport` with a `HostSample` (RAM, swap, engine
and summed guest RSS) plus one `SandboxRuntimeStats` per sandbox (native ref,
memory, start time). Every field is optional and best-effort — a backend that
cannot measure something omits it instead of reporting a fabricated zero, and
an empty report never fails or delays the heartbeat. A report replaces that
provider's previous sandbox set rather than merging into it.

One heartbeat carries at most 512 per-sandbox entries, most recently active
first: "active" is work the agent handled for that native ref, falling back
to newest start time. A host running more sandboxes drops the tail; the agent
logs it and increments `preloop.sandbox.heartbeat.truncated` by the number of
dropped entries. The cap bounds both the wire payload and the stored
per-provider report.

`Stats` exposes the stored inventory read-only, per registered provider:
`ProviderStats` carries the provider's id, name and drivers, its latest
`HostSample` with `host_sample_age_secs` (0 when never sampled), and its
`SandboxStats` — each `Sandbox` record with the runtime facts the provider
last reported (default/empty when never reported). Every client-token holder
can read `Stats`, so it carries aggregates only: host process names, PIDs and
data-directory paths stay on the provider that measured them. An unavailable
`Stats` read is reported as an unknown fleet (source `unavailable` plus a
`vm_sample_unavailable` condition), never as an empty one.

## How preloop uses it

`preloop engine` runs the service in-process with this host's drivers attached
through an in-process agent, and serves the API on `PRELOOP_SANDBOX_LISTEN`
(default `127.0.0.1:9191`; `off` disables). The runner pool
(`preloop_orchestrator::fleet::SandboxMachines`) acquires every VM from it:
goldens are build sandboxes, forks are acquires from the golden's snapshot
template, and deletes are releases. The pool keeps its golden, packed-artifact
and debug-hold strategy; the machines under it are sandboxes.

| Variable | Effect |
|---|---|
| `PRELOOP_SANDBOX_LISTEN` | API address, or `off` |
| `PRELOOP_SANDBOX_TOKEN` / `PRELOOP_SANDBOX_PROVIDER_TOKEN` | bearer tokens; required off loopback |
| `PRELOOP_SANDBOX_STORE_URL` | store URL |
| `PRELOOP_SANDBOX_DRIVERS` | drivers on this host (default `smolvm`) |
| `PRELOOP_SANDBOX_POOL_DRIVER` | driver the CI pool places on (default: first) |
| `PRELOOP_SANDBOX_SMOLVM_SLOTS` | concurrent SmolVM sandboxes (0 = no cap) |

## CLI

```sh
preloop sandbox serve --listen 0.0.0.0:9191            # standalone service (+ local SmolVM)
preloop sandbox agent --service http://hub:9191        # join as a provider
preloop sandbox template put --name alpine --image alpine:3.20
preloop sandbox pool put --name alpine --template alpine --warm 2
preloop sandbox acquire --pool alpine --env JOB=1      # → lease + sandbox JSON
preloop sandbox exec sbx-… -- uname -a
preloop sandbox cp ./dir sbx-…:/work                   # and sbx-…:/path ./out
preloop sandbox stop sbx-…                            # power off, keep the disk
preloop sandbox start sbx-… [--forkable]               # boot it again (bakes)
preloop sandbox snapshot sbx-… --name golden
preloop sandbox release lse-… [--preserve 600]
preloop sandbox ls | providers
preloop sandbox stats                                   # per-provider host + sandbox inventory
```

## Verification

* `cargo test -p preloop-sandbox` — service/agent conformance over the fake
  driver, in-process and across a real HTTP server with a remote agent
  (idempotency, warm pools, placement errors, unsupported operations,
  snapshot → template → fork, create failure, lease expiry and preservation,
  orphan/lost reconcile, generation fencing, SQLite restart, streaming exec,
  ticketed upload/download). Set `PRELOOP_SANDBOX_TEST_PG` to also run the
  Postgres store test.
* `PRELOOP_SANDBOX_E2E_SMOLVM=1 cargo test -p preloop-sandbox --test smolvm_e2e`
  — real SmolVM: bake, snapshot, warm pool of branches, socket mount, files,
  streaming, cleanup.
