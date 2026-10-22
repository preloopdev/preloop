# Sandbox Service

Preloop gets every VM from a Sandbox Service: a generic service that hands out
running machines from pluggable backends. The CI runner pool is one client;
`preloop sandbox …` The contract is
`proto/preloop/sandbox/v1/sandbox.proto` (in the repository root);
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
  `Snapshot`, `Stop`/`Start`, `Pause`/`Resume`, `Get`/`List`. Everything
  scoped to one sandbox (`Get`, `Exec`, `OpenStream`, `Snapshot`, `Stop`,
  `Start`, `Pause`, `Resume`) also carries the caller's `leaseId`; `List` is
  the only sandbox read that is not scoped to one (and it redacts lease ids).
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
  `Release(PRESERVE)` keeps a sandbox for debugging until its window ends, and
  the lease id keeps working as the debugging capability until then.

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

* **The lease is the capability.** Every sandbox-scoped RPC presents the
  `leaseId` `Acquire` returned, and the service checks it is that sandbox's
  active lease. A missing or foreign lease is `PERMISSION_DENIED`; an unknown
  sandbox is `NOT_FOUND`. `List` returns sandboxes with lease ids redacted,
  and `Get` only echoes the lease the caller already presented, so holding the
  client token alone cannot drive someone else's machine.
* Data-plane tickets are single-use, scoped to one sandbox and direction, and
  short-lived; the URL carries its own token. `OpenStream` mints a ticket only
  for the lease holder, and the ticket then authorizes the transfer itself.
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
| `PRELOOP_SANDBOX_MOUNT_ALLOW` | comma-separated host directory prefixes the agent may mount into guests (default: none) |
| `PRELOOP_SANDBOX_SOCKET_ALLOW` | comma-separated host socket prefixes the agent may mount (default: none) |

The engine's embedded agent allows exactly the host paths its CI pool mounts
(runner bundle, Node externals, control-bridge tree and control socket), plus
anything the two `_ALLOW` variables add. A standalone `preloop sandbox serve`
or `preloop sandbox agent` uses only those variables.

## CLI

```sh
preloop sandbox serve --listen 0.0.0.0:9191            # standalone service (+ local SmolVM)
preloop sandbox agent --service http://hub:9191        # join as a provider
preloop sandbox template put --name alpine --image alpine:3.20
preloop sandbox pool put --name alpine --template alpine --warm 2
preloop sandbox acquire --pool alpine --env JOB=1      # → lease + sandbox JSON
preloop sandbox exec sbx-… --lease lse-… -- uname -a
preloop sandbox cp ./dir sbx-…:/work --lease lse-…     # and sbx-…:/path ./out
preloop sandbox stop sbx-… --lease lse-…               # power off, keep the disk
preloop sandbox start sbx-… --lease lse-… [--forkable] # boot it again (bakes)
preloop sandbox snapshot sbx-… --lease lse-… --name golden
preloop sandbox release lse-… [--preserve 600]
preloop sandbox ls | providers                        # lease ids are redacted
```

## Verification

* `cargo test -p preloop-sandbox` — service/agent conformance over the fake
  driver, in-process and across a real HTTP server with a remote agent
  (idempotency, warm pools, placement errors, unsupported operations,
  snapshot → template → fork, create failure, lease expiry and preservation,
  orphan/lost reconcile, generation fencing, SQLite restart, streaming exec,
  ticketed upload/download, lease ownership and list redaction, host
  mount/socket allowlisting). Set `PRELOOP_SANDBOX_TEST_PG` to also run the
  Postgres store test.
* `PRELOOP_SANDBOX_E2E_SMOLVM=1 cargo test -p preloop-sandbox --test smolvm_e2e`
  — real SmolVM: bake, snapshot, warm pool of branches, socket mount, files,
  streaming, cleanup.
