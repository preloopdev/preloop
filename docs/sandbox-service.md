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
  `Snapshot`, `Pause`/`Resume`, `Get`/`List`.
* **Providers** (capacity owners): `Register` (drivers + capabilities →
  provider id and fencing generation), `Heartbeat`, `Poll` (long-poll for
  work), `Report` / `ReportExec` (fenced by generation), `Reconcile` (full
  inventory: lost sandboxes fail, orphans are destroyed, and a `Terminating`
  machine that is still there gets its destroy re-queued; a template's fork
  base is never an orphan, whichever provider reports it).

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
  The captured machine stays live as the template's fork base. It has no
  sandbox record, but it is never an orphan: any provider that reports it —
  including a second agent sharing the host's engine home — must leave it
  alone, and only deleting the template destroys it.
* **Pool** — template + shape + network + mounts + warm target + max. The
  service keeps `warm` idle sandboxes per pool (the generic warm pool) and
  refills after each claim, backing off after creation failures.
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
  until its window ends.

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

## How preloop uses it

`preloop engine` runs the service in-process with this host's drivers attached
through an in-process agent, and serves the API on `PRELOOP_SANDBOX_LISTEN`
(default `127.0.0.1:9191`; `off` disables). The runner pool
(`preloop_orchestrator::fleet::SandboxMachines`) acquires every VM from it:
goldens are build sandboxes, forks are acquires from the golden's snapshot
template, and deletes are releases. The pool keeps its golden, packed-artifact
and debug-hold strategy; the machines under it are sandboxes.

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
```

## Verification

* `cargo test -p preloop-sandbox` — service/agent conformance over the fake
  driver, in-process and across a real HTTP server with a remote agent
  (idempotency, warm pools, placement errors, unsupported operations,
  snapshot → template → fork, create failure, lease expiry and preservation,
  orphan/lost reconcile, generation fencing, SQLite restart, streaming exec,
  ticketed upload/download, restart fencing and recovery, terminating reaping,
  lease release on provider loss, idempotency payload mismatch). Set
  `PRELOOP_SANDBOX_TEST_PG` to also run the Postgres store test.
* `PRELOOP_SANDBOX_E2E_SMOLVM=1 cargo test -p preloop-sandbox --test smolvm_e2e`
  — real SmolVM: bake, snapshot, warm pool of branches, socket mount, files,
  streaming, cleanup.
