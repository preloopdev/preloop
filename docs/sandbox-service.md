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
  `Snapshot`, `Stop`/`Start`, `Pause`/`Resume`, `Get`/`List`, `Stats`
  (read-only per-provider host and sandbox aggregates), and the provider
  lifecycle controls `CordonProvider`, `UncordonProvider`, `DrainProvider`.
  Everything scoped to one sandbox (`Get`, `Exec`, `OpenStream`, `Snapshot`,
  `Stop`, `Start`, `Pause`, `Resume`) also carries the caller's `leaseId`;
  `List` is the only sandbox read that is not scoped to one (and it redacts
  lease ids).
* **Providers** (capacity owners): `Register` (drivers + capabilities →
  provider id and fencing generation), `Heartbeat`, `Poll` (long-poll for
  work), `Report` / `ReportExec` (fenced by generation), `Reconcile` (full
  inventory: lost sandboxes fail, orphans are destroyed, and a `Terminating`
  machine that is still there gets its destroy re-queued).

Both are served with the Connect protocol, JSON codec, over one HTTP port:
`POST /preloop.sandbox.v1.<Service>/<Method>`. Every RPC is unary or
server-streaming, so it works over HTTP/1.1 — proxies, and hosts behind NAT that
dial out. Bulk bytes never ride inside RPCs: `OpenStream` returns a short-lived
ticket and a tar moves with plain `PUT`/`GET` on `/sandbox/v1/streams/<ticket>`;
agents fetch and return blobs on `/sandbox/v1/agent/blobs/<ticket>`.

### Work delivery

Delivery is at-least-once. `Poll` returns at most `PRELOOP_SANDBOX_POLL_BATCH`
items (default 64) and does not drain the queue: delivered items stay in
flight, keyed by their work id, until the agent's `Report` for that id
arrives. An unacknowledged item becomes visible again after the visibility
timeout (`PRELOOP_SANDBOX_DELIVERY_TIMEOUT_SECS`, default 120 s) and is
redelivered; re-registering a provider releases everything the previous
generation still held, so a restarted agent picks its work back up.

Agents dedupe by work id: a redelivered item that is still running is ignored,
and a finished one re-reports its stored result instead of running again.
Reports are retried with jittered exponential backoff (capped at 15 s) until
the service accepts them; `Aborted` (a newer generation registered) and
`NotFound` (the work completed) end the retry. `ReportExec` batches carry a
per-exec sequence number, so a retried batch is applied once, in order, and the
caller's exec stream closes on the exit event even if later batches arrive.

An exec with `timeout_secs = 0` gets the service deadline
(`PRELOOP_SANDBOX_EXEC_TIMEOUT_SECS`, default 3600 s); the driver kills the
command at it and reports exit code 124. When the caller drops the exec
response stream, the service queues `CancelExec` for the provider and drops
the exec; the agent calls the driver's optional `cancel_exec` (SmolVM kills the
exec child) and reports exit code 130. The service also sweeps execs and
pending work past their deadlines; `Acquire` is bounded by its wait window,
and every request that waits on a provider to report (`Snapshot`, copy, power)
is bounded by the same work timeout.

The Connect layer is hand-rolled (`crates/preloop-sandbox/src/connect.rs`):
adding `connectrpc` would bring its own protobuf runtime, while the subset used
here — JSON unary and server streaming — is small. Any Connect client generated
from the `.proto` interoperates.

## Model

* **Template** — a logical image: one or more `Artifact`s
  (`format`, `arch`, `ref`, `digest`), a driver-specific `recipe` interpreted
  by the selected driver, or a snapshot captured by `Snapshot` (then bound to
  the provider and driver that hold it). `PutTemplate` requires a
  `sha256:` digest on every artifact and a digest-pinned (`@sha256:…`)
  registry ref; a driver materializes the first artifact whose format and
  architecture it supports, so one template can serve several hosts. Formats
  are `oci-image` (the backend pulls the registry image itself) and
  `smolmachine` (a monolithic pack at `file://…` or a digest-pinned OCI
  artifact on GHCR).
* **Pool** — template + shape + network + mounts + warm target + max. The
  service keeps `warm` idle sandboxes per pool (the generic warm pool) and
  refills after each claim, backing off after creation failures.
* **Provider cordon and drain** — `CordonProvider` stops new placements on one
  provider; its existing leases and warm members keep running. `DrainProvider`
  cordons and also destroys that provider's idle pool members, then reports
  how many leases remain (they are not killed). The cordon is persisted on
  the provider record and survives re-registration until `UncordonProvider`.
* **Stop / Start** — `Stop` powers a machine off but keeps it and its disk
  (packing needs a stopped machine), `Start` boots it again, `--forkable` when
  it must serve forks. Both work under the same lease.
* **Sandbox / Lease** — `Acquire` with the same idempotency key returns the same
  lease, provided the payload matches: the key is bound to the pool/template,
  shape, requirements, purpose, network, mounts, sockets, options and name of
  the request that first used it, and a reuse with any of those different is
  `invalid_argument`. TTL, wait and injected files are call tactics, not
  identity: they may change on a retry. Lease environment is merged into every
  exec; injected files are written before the lease is returned. Leases expire
  (the sandbox is destroyed); `Release(PRESERVE)` keeps a sandbox for debugging
  until its window ends, and the lease id keeps working as the debugging
  capability until then.

Capabilities decide placement; nothing branches on driver names. A request no
registered provider can satisfy fails with the unmet requirement named
(`no provider satisfies template "mac": host/smolvm: os=macos`), and an
operation a backend does not declare (`Pause`, `Snapshot`) fails with
`failed_precondition` instead of being emulated. `one_shot` backends run one
command per sandbox; callers needing several execs require `multi_exec`.
Placement also enforces each provider's declared per-sandbox ceiling
(`maxVcpus`/`maxMemoryMib`, zero meaning unreported): a `Shape` asking for more
vCPUs or memory is rejected by name instead of being sent to a driver that
cannot boot it.

## Placement

Placement is split in two. The service filters: it drops offline providers
(and, for a snapshot template, every provider that does not hold the snapshot)
and every provider/driver pair that misses a required capability or is at its
slot cap, leaving a candidate list. A `Placement`
(`crates/preloop-sandbox/src/placement.rs`) then ranks those candidates by
preference, and the service takes the first one. A placement therefore orders
the candidates the service proved eligible; it can neither admit a pair the
filter rejected nor widen a request.

`LeastLoaded` is the default: fewest live sandboxes first, ties in candidate
order (provider id, then the provider's driver order). `ServiceConfig::placement`
takes any `Placement` implementation; `PRELOOP_SANDBOX_PLACEMENT` selects a
built-in one by name (default `least-loaded`) for `preloop sandbox serve` and
the engine, and an unknown name is a startup error naming the valid values.

## Drivers

A driver is the host-side, in-process interface a host agent drives
(`crates/preloop-sandbox/src/driver.rs`). It is not a public plugin ABI: third
parties integrate by implementing the Providers protocol. `exec` carries the
exec id and a non-zero deadline the driver enforces, and the optional
`cancel_exec` kills a running command when the caller goes away (it may arrive
before `exec` starts; drivers remember it and refuse to start that exec).

| Driver | Where | Model |
|---|---|---|
| `smolvm` | public | CLI. Golden-fork: a build sandbox starts `--branchable`; `Snapshot` keeps it as the fork base; sandboxes from the snapshot template are `machine branch` clones. |
| `fake` | public | Process-backed, for tests and conformance. |
| Private drivers | private build | See the private repository for driver-specific implementations. |

A clone inherits the fork base's full configuration, so a fork request is a
delta: the `smolvm` driver applies network, mounts, shape, overlay size and
Rosetta by stopping the clone, running `machine update`, and starting it again
(with `--branchable` when the request asks for a snapshot-capable clone). A
request that changes nothing forks without that stop and start. Settings SmolVM
cannot change on an existing machine (sockets, DNS, role, guest volumes and
workload) fail with `failed_precondition` naming the field rather than
returning a clone that silently differs from the request.

`smolvm serve` has native fork pools, but its clones lose `--mount-socket`
relays (the CLI's branches keep them), so the driver stays on the CLI and pools
are the service's.

Each driver owns materialization behind `PackFormat`
(`crates/preloop-sandbox/src/drivers/smolvm/pack.rs`): `oci-image` hands the
digest-pinned reference to SmolVM, `smolmachine` fetches a `file://` pack or a
registry artifact blob into a content-addressed cache and verifies its sha256
before booting. The cache lives under the driver's data directory
(`<state>/sandbox-cache` in the engine), is bounded by
`PRELOOP_SANDBOX_CACHE_MAX_BYTES` (default 50 GiB), and evicts
least-recently-used artifacts after each materialization — never one a running
sandbox still references.

## Security model

* **The lease is the capability.** Every sandbox-scoped RPC presents the
  `leaseId` `Acquire` returned, and the service checks it is that sandbox's
  active lease. A missing or foreign lease is `PERMISSION_DENIED`; an unknown
  sandbox is `NOT_FOUND`. `List` returns sandboxes with lease ids redacted,
  and `Get` only echoes the lease the caller already presented, so holding the
  client token alone cannot drive someone else's machine.
* Data-plane tickets are single-use, scoped to one sandbox and direction, and
  short-lived. `OpenStream` returns the stream URL and a separate `token`; the
  caller sends it as `Authorization: Bearer <token>` on the upload or download,
  so the URL alone is not enough.
* **The host agent owns the mount policy.** A `Create` whose `Mount.host` or
  `SocketMount.host` is outside the agent's allowlist is rejected with
  `FAILED_PRECONDITION` before the driver runs (the caller sees it as the
  sandbox's create failure, with the code and path in the message). Paths are
  canonicalized, so `..` traversal and symlinks cannot step outside an allowed
  tree. Forks arrive as `Create` work and take the same check. The default is
  empty: deny every host mount and socket until an operator names the prefixes.
* `Report`/`ReportExec`/`Reconcile` are fenced by the provider's registration
  generation, and a provider may not report output for another provider's
  exec, nor destroy another provider's machine.
* **The provider token authenticates a role, not an identity.** Any holder can
  register any provider *name*, which bumps that name's generation, fences the
  previous session and redelivers its in-flight work to the new one, poll its
  queue, or report its work items. Deployments that hand the provider token to
  more than one trusted operator need per-provider identities (a follow-up);
  on a single-operator tailnet the shared token is the trust boundary, exactly
  like the engine token.

## Limits and pagination

Unary Connect requests are capped at 4 MiB; bulk bytes never belong in them.
The data-plane `PUT` routes stream to disk and stop at
`PRELOOP_SANDBOX_BLOB_MAX_BYTES` (default 8 GiB), answering `413` and deleting
the partial file. At boot the service deletes every file left in its blob
directory: tickets live only in memory, so no restart can claim them.

`List`, `ListTemplates`, `ListPools` and `ListProviders` are paginated.
`pageSize` defaults to 100 and is capped at 1000; `nextPageToken` from a
response is passed back as `pageToken`. Items are ordered by id, and the token
is an opaque encoding of the last id. `SandboxClient` follows the tokens, so
the in-process and network APIs still return complete lists.

## State

The service keeps its working set in memory and writes through to its own
store, the restart source: SQLite (`<state>/sandbox.db`, default) or a
`sandbox` schema in Postgres (`PRELOOP_SANDBOX_STORE_URL=postgres://…`). It
never references control tables. Writes are best-effort in the project's
sense: in-memory state is authoritative, so a store failure is logged at error
level with the record kind and never fails the caller's RPC.

## Restart and failure

The store is the only thing that survives a process: provider queues, pending
work, exec streams, tickets and pool backoff are memory. Boot therefore starts
with three decisions, before any provider is placed on or caller served:

* **Every provider's generation is bumped and persisted.** An agent session
  that outlived the process is fenced (`aborted`) on its next call and
  re-registers with a fresh generation; until it does, its records are not
  placed on. Sandboxes, leases, pools and templates are restored as they were.
* **A sandbox caught `PROVISIONING` becomes `FAILED`** with `restarted while
  creating: …`. It cannot be re-driven: the create request (shape, network,
  mounts, sockets, options) is not on the record, so a second create would be
  a different sandbox, and nothing will ever report the first. If the machine
  did land, no record names it and the next reconcile destroys it as an
  orphan. The caller's retry with the same key releases the failed record and
  starts over.
* **A sandbox left `TERMINATING` stays so.** Its destroy was decided but its
  report died with the process; when the provider reports the machine in a
  `Reconcile`, the service re-queues the destroy (bounded: three attempts,
  after which the record is dropped and the machine is reaped as an orphan).
  When the machine is missing from the inventory the record is dropped
  immediately, so it stops holding a slot.

Stop, start, pause, resume and snapshot are operations, not record states: the
record moves only when their report lands, so a restart leaves the last settled
state. A machine may therefore be stopped while the record says leased; the
driver answers with its real state on the next operation, and the callers whose
replies were lost see `unavailable`.

A provider silent for `provider_lost_factor × provider_timeout` (default
135 s) is declared lost. Its sandboxes are failed, its in-flight work is
answered with `unavailable`, and its leases are released: the lease row is
removed, so the caller's next `Renew` is `not_found` and its next `Acquire`
with the same key starts over. `Renew` on a `FAILED` or `TERMINATING` sandbox
is `failed_precondition` — a lease never outlives the machine it names, and
one that did would promise capacity that cannot be used.

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
and debug-hold strategy; the machines under it are sandboxes. An image string
becomes a template by content: a local pack is hashed and a registry tag is
resolved to the digest the registry serves, so a rebuilt pack registers a new
template instead of silently reusing the old one.

The engine also sweeps orphaned SmolVM hypervisors under its home at pool
startup and teardown (`OrphanPurge::All`). Those sweeps only see a machine's
data directory, so the SmolVM driver writes an ownership marker into every
machine it serves for a caller that is not the CI pool; the sweeps spare
marked machines. Another caller's leased sandbox therefore survives an engine
restart or stop, while pool machines stay unmarked and the sweep remains their
crash backstop. The service's own delete path escalates to a targeted kill when
`machine delete` refuses.

| Variable | Effect |
|---|---|
| `PRELOOP_SANDBOX_LISTEN` | API address, or `off` |
| `PRELOOP_SANDBOX_TOKEN` / `PRELOOP_SANDBOX_PROVIDER_TOKEN` | bearer tokens; required off loopback |
| `PRELOOP_SANDBOX_STORE_URL` | store URL |
| `PRELOOP_SANDBOX_BLOB_MAX_BYTES` | blob upload cap (default 8 GiB) |
| `PRELOOP_SANDBOX_DRIVERS` | drivers on this host (default `smolvm`) |
| `PRELOOP_SANDBOX_POOL_DRIVER` | driver the CI pool places on (default: first) |
| `PRELOOP_SANDBOX_PLACEMENT` | placement strategy (default `least-loaded`) |
| `PRELOOP_SANDBOX_SMOLVM_SLOTS` | concurrent SmolVM sandboxes (0 = no cap) |
| `PRELOOP_SANDBOX_MOUNT_ALLOW` | comma-separated host directory prefixes the agent may mount into guests (default: none) |
| `PRELOOP_SANDBOX_SOCKET_ALLOW` | comma-separated host socket prefixes the agent may mount (default: none) |
| `PRELOOP_SANDBOX_CACHE_DIR` | artifact cache directory (default `<state>/sandbox-cache`) |
| `PRELOOP_SANDBOX_CACHE_MAX_BYTES` | artifact cache budget (default 50 GiB) |
| `PRELOOP_SANDBOX_POLL_BATCH` | most work items one `Poll` returns (default 64) |
| `PRELOOP_SANDBOX_DELIVERY_TIMEOUT_SECS` | unacknowledged work redelivery delay (default 120) |
| `PRELOOP_SANDBOX_EXEC_TIMEOUT_SECS` | default exec deadline (default 3600) |
| `PRELOOP_SANDBOX_AGENT_CONCURRENCY` | work items an agent runs at once (default 32) |

The engine's embedded agent allows exactly the host paths its CI pool mounts
(runner bundle, Node externals, control-bridge tree and control socket), plus
anything the two `_ALLOW` variables add. A standalone `preloop sandbox serve`
or `preloop sandbox agent` uses only those variables.

## CLI

```sh
preloop sandbox serve --listen 0.0.0.0:9191            # standalone service (+ local SmolVM)
preloop sandbox agent --service http://hub:9191        # join as a provider
preloop sandbox template put --name alpine --artifact \
  format=oci-image,arch=aarch64,ref=alpine@sha256:…,digest=sha256:…
preloop sandbox pool put --name alpine --template alpine --warm 2
preloop sandbox acquire --pool alpine --env JOB=1      # → lease + sandbox JSON
preloop sandbox exec sbx-… --lease lse-… -- uname -a
preloop sandbox cp ./dir sbx-…:/work --lease lse-…     # and sbx-…:/path ./out
preloop sandbox stop sbx-… --lease lse-…               # power off, keep the disk
preloop sandbox start sbx-… --lease lse-… [--forkable] # boot it again (bakes)
preloop sandbox snapshot sbx-… --lease lse-… --name golden
preloop sandbox release lse-… [--preserve 600]
preloop sandbox ls | providers                        # lease ids are redacted
preloop sandbox stats                                   # per-provider host + sandbox inventory
preloop sandbox provider cordon host-a               # no new placements
preloop sandbox provider drain host-a                # cordon + release idle pool members
preloop sandbox provider uncordon host-a
```

## Verification

* `cargo test -p preloop-sandbox` — service/agent conformance over the fake
  driver, in-process and across a real HTTP server with a remote agent
  (idempotency, warm pools, placement errors and configured ranking,
  unsupported operations, snapshot → template → fork, create failure, lease
  expiry and preservation, orphan/lost reconcile, generation fencing, SQLite
  restart, streaming exec, ticketed upload/download, lease ownership and list
  redaction, host mount/socket allowlisting, cordon/drain, pagination, request
  and blob caps, the boot blob sweep, artifact validation, digest-verified pack
  materialization, cache GC, restart fencing and recovery, terminating reaping,
  lease release on provider loss, idempotency payload mismatch, poll batching,
  redelivery and report dedupe, exec deadlines and disconnect cancellation).
  Set `PRELOOP_SANDBOX_TEST_PG` to also run the Postgres store test.
* `PRELOOP_SANDBOX_E2E_SMOLVM=1 cargo test -p preloop-sandbox --test smolvm_e2e`
  — real SmolVM: bake, snapshot, warm pool of branches, socket mount, files,
  streaming, cleanup.
