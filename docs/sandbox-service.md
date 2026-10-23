# Sandbox Service

Preloop gets every VM from a Sandbox Service: a generic service that hands out
running machines from pluggable backends. The CI runner pool is one client;
the `preloop sandbox …` CLI is another. The contract is
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
  `Snapshot`, `Stop`/`Start`, `Pause`/`Resume`, `Get`/`List`, `Stats`
  (read-only per-provider host and sandbox aggregates), and the provider
  lifecycle controls `CordonProvider`, `UncordonProvider`, `DrainProvider`.
  Everything scoped to one sandbox (`Get`, `Exec`, `OpenStream`, `Snapshot`,
  `Stop`, `Start`, `Pause`, `Resume`) also carries the caller's `leaseId`;
  `List` is the only sandbox read that is not scoped to one (and it redacts
  lease ids).
* **Providers** (capacity owners): `Enroll` (single-use join token → 24h
  client certificate), `Register` (drivers + capabilities + the agent's
  `protocolVersion` → provider id and fencing generation; a version the
  service does not speak is `failed_precondition` naming both versions),
  `Heartbeat`, `Poll` (long-poll for work), `Report` /
  `ReportExec` (fenced by generation), `Reconcile` (full inventory: lost
  sandboxes fail, orphans are destroyed, a `Terminating` machine that is
  still there gets its destroy asked again unless one is in flight, and a
  tombstone's machine is destroyed; a template's fork base is never an
  orphan, whichever provider reports it), `RenewCertificate`.

In simple mode both are served over one HTTPS port; split mode gives callers
and agents separate ports with the same route families:
`POST /preloop.sandbox.v1.<Service>/<Method>`. Every RPC is unary or
server-streaming, so it works over HTTP/1.1 — proxies, and hosts behind NAT
that dial out. Bulk bytes never ride inside RPCs: `OpenStream` returns a
short-lived ticket and a tar moves with plain `PUT`/`GET` on
`/sandbox/v1/streams/<ticket>`; agents fetch and return the same stream on
`/sandbox/v1/agent/blobs/<ticket>`. The ticket is a rendezvous, not a file:
the caller and agent connect their halves and bytes flow through bounded
channels with backpressure, so neither the service nor the agent writes the
archive to disk.

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
host-side `machine exec` client and, with a second `machine exec`, the guest
command it left behind) and reports exit code 130. The service also sweeps
execs and pending work past their deadlines; a swept exec ends the caller's
stream with the same exit code 124 the driver reports, so a late report can
never turn the caller's result into an error. `Acquire` is bounded by its wait
window, and every request that waits on a provider to report (`Snapshot`,
copy, power) is bounded by the same work timeout. A wait that is blocked only
because the pool is at capacity — not on provisioning — is bounded separately
and much more tightly (30 s by default): the acquire then fails with
`resource_exhausted`, the overload error itself, so the caller can fail over
instead of queueing behind a full pool.

Exec work is the one redelivery that must not replay: an exec is a command,
and running it twice is a side effect, not a retry. The agent records every
exec it starts in a durable started-set beside its identity state
(`started-execs/` in its scratch directory; the CLI pins that directory to
`<state>/sandbox-agent`, so a restart reuses it) *before* running it, and
refuses a redelivery of an exec id already in the set, reporting it as a
failed attempt; the service closes the caller's stream with that error rather
than a phantom second run. The service counts each delivery on the exec record,
so redeliveries are visible where the exec lives. The fence is host-local
and lasts as long as the agent's scratch directory does: wiping the scratch,
or redelivering the exec to a different host, reopens the window — and an
exec that ran to completion but whose every report was lost ends the
caller's stream with the fencing error instead of the lost success, the
conservative direction, never a duplicate run.

The Connect layer is generated from the proto, not hand-written.
`scripts/gen-sandbox-proto.sh` runs `protoc` with two plugins:
`protoc-gen-buffa` (0.9.x) for the message types and their proto3 JSON
mapping, and `connectrpc-codegen`'s `protoc-gen-connect-rust` for the
`Sandboxes`/`Providers` service traits, typed clients and method specs. The
output is checked in under `crates/preloop-sandbox/src/gen/` and re-exported
through `api`, so there is exactly one definition of every message and every
RPC; changing the wire is a `.proto` edit plus a regeneration, never a
parallel Rust mirror. Any Connect client generated from the same `.proto`
interoperates.

### Wire encoding

The generated types speak the proto3 JSON mapping:

* **64-bit integers are JSON strings.** `int64`/`uint64` fields (`createdAt`,
  `expiresAt`, `sizeBytes`, `ttlSecs`, lease epochs, …) encode as decimal
  strings, as proto3 JSON requires; 32-bit fields stay JSON numbers. Clients
  must accept a quoted decimal.
* **Enums are open.** A value this build does not know decodes into
  `EnumValue::Unknown(n)` and re-encodes as `n` rather than failing the
  message; service predicates treat an unknown sandbox state as neither
  terminal nor ready. Adding an enum value is therefore additive.
* **Message fields are presence-aware.** Absent submessages stay absent
  instead of materializing a default, and `null` reads as unset.

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
  artifact on GHCR). The captured machine stays live as the template's fork
  base. It has no sandbox record, but it is never an orphan: any provider that
  reports it — including a second agent sharing the host's engine home — must
  leave it alone, and only deleting the template destroys it.
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
  identity: they may change on a retry. A replay returns the recorded sandbox
  in whatever state its owner left it — including `Stopped`: a retry can
  never destroy a sandbox that was stopped on purpose, and the caller starts
  it again itself. Lease environment is merged into every
  exec; injected files are written before the lease is returned. Leases expire
  (the sandbox is destroyed); `Release(PRESERVE)` keeps a sandbox for debugging
  until its window ends, and the lease id keeps working as the debugging
  capability until then. Preserve applies from `Leased`, `Preserved`, `Paused`
  and `Stopped` — the machine keeps its power state, so a stopped sandbox
  must be started before it executes — and from any other state it fails with
  `failed_precondition` and changes nothing; a successful release response
  never means the sandbox was destroyed.

Capabilities decide placement; nothing branches on driver names. A request no
registered provider can satisfy fails with the unmet requirement named
(`no provider satisfies template "mac": host/smolvm: os=macos`), and an
operation a backend does not declare (`Pause`, `Snapshot`) fails with
`failed_precondition` instead of being emulated. A cordon is an operator
choice, not a lost provider: an acquire only a cordoned provider could serve
fails at once with `failed_precondition` naming the cordon rather than waiting
and reporting "no provider is online". `one_shot` backends run one
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

A driver is the host-side interface a host agent drives
(`crates/preloop-sandbox/src/driver.rs`). It runs in-process, or as an
out-of-process plugin that speaks the same operations over a Unix socket. See
[Sandbox drivers](sandbox-drivers.md) for the plugin protocol, the
`PRELOOP_SANDBOX_DRIVERS=external:…` configuration, stub generation, the Go
skeleton and the conformance suite. Third parties can integrate by
implementing the Providers protocol directly or by shipping a driver plugin.

`exec` carries the exec id and a non-zero deadline the driver enforces, and
the optional `cancel_exec` kills a running command when the caller goes away
(it may arrive before `exec` starts; drivers remember it and refuse to start
that exec).

| Driver | Where | Model |
|---|---|---|
| `smolvm` | public | CLI. Golden-fork: a build sandbox starts `--branchable`; `Snapshot` keeps it as the fork base; sandboxes from the snapshot template are `machine branch` clones. |
| `fake` | public | Process-backed, for tests and conformance. |
| `external` | public | A plugin process speaking `preloop.sandbox.driver.v1.Driver` over a Unix socket, in any language. |

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

## Security

The service creates a CA in `<state>/sandbox-ca/` (`ca.pem` public, `ca.key`
mode 0600) and reuses it. Generated server certificates are issued from that
CA with SANs from `PRELOOP_SANDBOX_TLS_SANS` plus exact names in
`PRELOOP_SANDBOX_ALLOWED_HOSTS` and the detected Tailscale MagicDNS name. A
changed SAN set causes a fresh generated certificate on restart. A supplied
certificate is used as-is.

* **Callers** (`Sandboxes`) present one bearer token. When
  `PRELOOP_SANDBOX_TOKEN` is unset the service generates one on first start at
  `<state>/sandbox-client-token` (0600). The CLI resolves `--token` /
  `PRELOOP_SANDBOX_TOKEN`, then that file, and the CA through `--ca` /
  `PRELOOP_SANDBOX_CA`, then `<home>/state/sandbox-ca/ca.pem`. The in-process
  path (`InProcess`) needs no credentials: it is the same process.
* **Providers** authenticate with a client certificate issued by the service
  CA. Every `Providers` RPC except `Enroll` requires one; the service maps the
  leaf's SHA-256 fingerprint to a provider record, so no X.509 parsing happens
  past the handshake. The CA and the leaves it issues carry Subject and
  Authority Key Identifiers (plus `basicConstraints` and the matching EKU), so
  OpenSSL 3 verifiers — Python `ssl`, `curl` — accept the chain. A certificate
  only acts as the provider it was enrolled for: `Register`'s `name` must equal
  the enrolled name and every other RPC may only name its own provider id.
  Removed providers and expired certificates are rejected with
  `permission_denied`. `PRELOOP_SANDBOX_PROVIDER_TOKEN` is gone.
* **Enrollment** is a single-use token:
  `preloop sandbox token create --ttl 1h` prints
  `plj1.<id>.<secret>.<ca_sha256>`. The agent fetches the service CA and
  refuses to send its CSR unless it hashes to the token's pin.
* **Loopback Host guard.** A guarded client listener accepts `localhost`,
  loopback IPs, its bound address, configured exact names, and one-label
  `*.suffix` patterns. Host matching is case-insensitive, ignores a port and a
  trailing dot, and never uses substring matching.
* **Endpoint files.** The client address is published at
  `<state>/sandbox-endpoint` (mode 0600). Split mode also publishes the agent
  address at `<state>/sandbox-agent-endpoint`. Each file records the scheme,
  bound address and owner PID; shutdown or stale cleanup never removes a live
  owner's file.
* **The lease is the capability.** Every sandbox-scoped RPC presents the
  `leaseId` `Acquire` returned, and the service checks it is that sandbox's
  active lease. A missing or foreign lease is `PERMISSION_DENIED`; an unknown
  sandbox is `NOT_FOUND`. `List` returns sandboxes with lease ids redacted,
  and `Get` only echoes the lease the caller already presented, so holding the
  client token alone cannot drive someone else's machine.
* **Data-plane tickets** are single-use, scoped to one sandbox and direction,
  and short-lived; each side (producer and consumer) is claimable once, and a
  claimed transfer gets the work timeout. Their URLs are relative, so they
  inherit the `https` base.

## Authorization

Authentication and authorization are separate. Handlers first verify the
existing bearer token, provider certificate, or ticket token, then call the
replaceable `Authorizer` seam with a named `Principal` and coarse `Action`.
`PRELOOP_SANDBOX_AUTHORIZER` is unset or `builtin` today; any other value
refuses startup and names the setting. The built-in policy
allows every authenticated principal to use every action on its surface, so
the default behavior is unchanged. Roles, namespaces, and per-caller
credentials are follow-up work in #777/#792.

The action groups are `Read`, `Write`, `Admin`, `Exec`, `Transfer`, `Enroll`,
and `Provider`; the complete route mapping lives in
`crates/preloop-sandbox/src/authz.rs`.

## State

The database is the authority. Every durable decision — inventory, leases,
provider generations, work queues, exec lifecycle, ticket metadata and
snapshot receipts — is a typed command in one short SQL transaction. Memory
holds only bounded live channels (an exec's output queue, one transfer's
rendezvous) and the instants a caller is currently waiting on; it is never the
source of truth. A read that is not answered from SQL does not exist.

SQLite (`<state>/sandbox.db`, default) serves one node. Postgres
(`PRELOOP_SANDBOX_STORE_URL=postgres://…`, an independent `sandbox` schema or
database) serves several replicas: each opens its own pool, claims work with
`SKIP LOCKED`, and coordinates through conditional updates and rows, never a
global lock. The service never references control-plane tables.

The schema is greenfield v1. On an empty database it is created and its
version and key fingerprints are recorded in `schema_meta`; a non-empty store
that was not created by this service, a different schema version, or a
different seal key is refused at open instead of being imported, upgraded or
dual-written. There is nothing to migrate: no legacy sandbox state exists.
`memory:` is a real in-memory SQLite database, used by tests and embedded
single-process callers.

Each host keeps its own `<state>/sandbox-service.lock`: the lock protects a
local state directory, not a database, so replicas on one Postgres do not
block each other. Secrets live outside the database: lease and exec
environment is sealed with AES-256-GCM from the locked `ring` dependency,
using a key shared by configuration (`PRELOOP_SANDBOX_SEAL_KEY`) or a 0600
file in the state directory. `schema_meta` records the key fingerprint, so a
node configured with a different key fails closed at open. Bearer, join and
ticket tokens are stored only as SHA-256 hashes; CSR keys and inject payloads
are never persisted.

Multi-replica routing keeps one stable URL for callers and agents. An exec or
transfer stream is owned by the replica that opened it, recorded as a fenced
node/session handle. A request that lands on another replica looks up the
owner and forwards over an authenticated internal hop: the sending node proves
its registered node id, session and credential, and the receiving node checks
them against its node registry. Peer base URLs come only from configuration
(`PRELOOP_SANDBOX_PEERS`), never from request fields, and replicas share the
service CA (`PRELOOP_SANDBOX_CA_DIR`) so that hop is verified TLS. If the
owning replica dies, its node session expires, its live channels fail
explicitly and its durable leases are released by maintenance; other replicas
and their leases are untouched.

### Exec replay boundary

Delivery is at-least-once, execution is at-most-once. A delivery attempt is
not an execution identity: a redelivered work item does not re-run a command.
The agent records a started exec durably on its host and the service records a
durable start claim (`execs.started_attempt`) before the guest command runs;
the first claim wins and output from the same execution is accepted across
delivery attempts. A later delivery that tries to start the same exec fails
the exec with an explicit error instead of replaying it. An ambiguous outcome
— the command may or may not have run — fails the caller explicitly; it is
never retried automatically. Conversely, ordinary work results (create,
snapshot, power, copy) are persisted before any waiter observes success, so a
lost response is answered from the durable result rather than repeated.

## Restart and failure

Only the database survives a process, and it survives every replica the same
way: provider queues, pending work, exec/ticket rows, leases, pools and
templates are SQL, not a boot-time reconstruction. In-memory channels are
rebuilt from owner handles: a replica that starts again registers a new node
session, which fences the previous process's live channels and provider
sessions; agents re-register and resume durable leases and work. There is no
"load the whole store into state" pass and no write-through shadow copy.

Boot makes those decisions in SQL before serving traffic:

* **A provider session that outlived its process is fenced** on its next call
  (`aborted`) and re-registers with a fresh generation; until it does its
  records are not placed on. Durable sandboxes, leases, pools and templates
  are unaffected.
* **A sandbox caught `PROVISIONING`** keeps its durable create work: the
  request is part of the sandbox spec row, so a replica resumes the accepted
  provisioning instead of failing it whole. The caller's retry with the same
  idempotency key returns the same lease.
* **A sandbox left `TERMINATING` stays so until its machine is gone.** A
  destroy in flight is awaited; a failed destroy is retried by `Reconcile`
  while the provider still reports the machine, and after three failures the
  record becomes a `GONE` tombstone that still lets `Reconcile` reclaim the
  machine. The tombstone holds no capacity slot.

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
through an in-process agent. With no agent-listener setting it keeps the simple
single HTTPS listener on `PRELOOP_SANDBOX_LISTEN` (default `127.0.0.1:9191`;
`off` disables). The runner pool (`preloop_orchestrator::fleet::SandboxMachines`)
acquires every VM from it: goldens are build sandboxes, forks are acquires from
the golden's snapshot template, and deletes are releases. The pool keeps its
golden, packed-artifact and debug-hold strategy; the machines under it are
sandboxes. An image string becomes a template by content: a local pack is
hashed and a registry tag is resolved to the digest the registry serves, so a
rebuilt pack registers a new template instead of silently reusing the old one.

The engine also sweeps orphaned SmolVM hypervisors under its home at pool
startup and teardown (`OrphanPurge::All`). Those sweeps only see a machine's
data directory, so the SmolVM driver writes an ownership marker into every
machine it creates, pool machines included, and the sweeps spare marked
machines. Another caller's leased sandbox therefore survives an engine restart
or stop. The pool has no sweep backstop for its own machines: it deletes them
through the service on teardown, and its startup cleanup releases leftovers
through the service. A pool that the guest-agent guard disables starts no
service and runs no cleanup, so its leftover runners keep running until the
pool next starts. The service's own delete path escalates to a targeted kill
when `machine delete` refuses.

| Variable | Effect |
|---|---|
| `PRELOOP_SANDBOX_LISTEN` | client API address, or `off` |
| `PRELOOP_SANDBOX_AGENT_LISTEN` | separate agent API address; unset keeps one listener |
| `PRELOOP_SANDBOX_LISTEN_ALL_INTERFACES` | `1` permits `0.0.0.0` or `::` binds |
| `PRELOOP_SANDBOX_CLIENT_TLS` | `service-ca` (default), `file`, or `plain-loopback` |
| `PRELOOP_SANDBOX_CLIENT_TLS_CERT` / `_KEY` | PEM certificate and key for `file` mode |
| `PRELOOP_SANDBOX_ALLOWED_HOSTS` | comma-separated exact names or `*.suffix` patterns |
| `PRELOOP_SANDBOX_TOKEN` | caller bearer token; generated into `<state>/sandbox-client-token` when unset |
| `PRELOOP_SANDBOX_CA` | CA bundle the CLI verifies the service with |
| `PRELOOP_SANDBOX_JOIN_TOKEN` | join token for `sandbox agent` on its first run |
| `PRELOOP_SANDBOX_TLS_SANS` | additional generated server certificate SANs |
| `PRELOOP_SANDBOX_STORE_URL` | `sandbox` SQLite file/URL or Postgres URL (default `<state>/sandbox.db`) |
| `PRELOOP_SANDBOX_SEAL_KEY` | 64-hex lease/exec env envelope key shared by replicas (default: a 0600 `<state>/sandbox-seal.key`) |
| `PRELOOP_SANDBOX_CA_DIR` | shared CA directory for replicas that must trust each other's client certificates |
| `PRELOOP_SANDBOX_PG_POOL` | Postgres connection pool ceiling (default 8) |
| `PRELOOP_SANDBOX_NODE_ID` | this replica's node id (default: hostname; unique per serving process) |
| `PRELOOP_SANDBOX_NODE_CREDENTIAL` | inter-node credential this replica presents on the internal hop |
| `PRELOOP_SANDBOX_PEER_URL` | this replica's base URL as other replicas reach it |
| `PRELOOP_SANDBOX_PEERS` | `node_id=https://host:port,…` peer directory; the only source of internal routes |
| `PRELOOP_SANDBOX_NODE_LOST_AFTER_SECS` | node heartbeat expiry before its live channels are failed (default 30) |
| `PRELOOP_SANDBOX_BLOB_MAX_BYTES` | per-transfer byte cap (default 8 GiB) |
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
preloop sandbox serve --listen 0.0.0.0:9191 --listen-all-interfaces  # standalone service (+ local SmolVM)
preloop sandbox token create --ttl 1h --name host-a    # join token bound to one provider
preloop sandbox agent --join https://hub:9191 --token plj1.…  # enroll, then reuse the identity
preloop sandbox agent                                         # later starts reuse the stored URL
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
preloop sandbox provider ls
preloop sandbox provider cordon host-a               # no new placements
preloop sandbox provider drain host-a                # cordon + release idle pool members
preloop sandbox provider uncordon host-a
preloop sandbox provider rm host-a                     # revoke its certificates
```

Without `--service` (or `PRELOOP_SANDBOX_URL`), the CLI reads
`<home>/state/sandbox-endpoint`, which the local service writes when it binds,
and refuses to send its bearer token to any other address.

## Verification

* `cargo test -p preloop-sandbox` — service/agent conformance over the fake
  driver, in-process and across a real TLS server with a remote enrolled agent
  (idempotency, warm pools, placement errors and configured ranking,
  unsupported operations, snapshot → template → fork, create failure, lease
  expiry and preservation, orphan/lost reconcile, generation fencing, SQLite
  restart, streaming exec, ticketed upload/download, lease ownership and list
  redaction, host mount/socket allowlisting, cordon/drain, pagination, request
  and transfer caps, streaming transfers with no staging files, artifact
  validation, digest-verified pack
  materialization, cache GC, restart fencing and recovery, terminating reaping,
  lease release on provider loss, idempotency payload mismatch, poll batching,
  redelivery and report dedupe, exec deadlines and disconnect cancellation),
  plus enrollment: single-use and expired join tokens, CA pinning, name
  binding, mTLS denial without a certificate, renewal, and revocation. Set
  `PRELOOP_SANDBOX_TEST_PG` to also run the Postgres store test.
* `cargo test -p preloop-sandbox --test external_driver` — the out-of-process
  driver protocol end to end: `driver_plugin::serve` on a Unix socket,
  `ExternalDriver` conformance, a service/agent acquire → exec → release
  through the plugin, and spawn-mode startup. See
  [Sandbox drivers](sandbox-drivers.md) for the protocol and conformance
  cases.
* `PRELOOP_SANDBOX_E2E_SMOLVM=1 cargo test -p preloop-sandbox --test smolvm_e2e`
  — real SmolVM: bake, snapshot, warm pool of branches, socket mount, files,
  streaming, cleanup.
