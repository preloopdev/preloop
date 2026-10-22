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
  `Snapshot`, `Pause`/`Resume`, `Get`/`List`.
* **Providers** (capacity owners): `Enroll` (single-use join token → 24h
  client certificate), `Register` (drivers + capabilities → provider id and
  fencing generation), `Heartbeat`, `Poll` (long-poll for work), `Report` /
  `ReportExec` (fenced by generation), `Reconcile` (full inventory: lost
  sandboxes fail, orphans and stale incarnations are destroyed),
  `RenewCertificate`.

Both are served with the Connect protocol, JSON codec, over one HTTPS port:
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

## Security

Every listener speaks TLS. On first start the service creates a CA in
`<state>/sandbox-ca/` (`ca.pem` public, `ca.key` mode 0600) and reuses it, issuing its
own server certificate with SANs from `PRELOOP_SANDBOX_TLS_SANS` (default: the
hostname, `localhost`, `127.0.0.1`, `::1`).
`PRELOOP_SANDBOX_TLS_CERT` / `PRELOOP_SANDBOX_TLS_KEY` replace the server
certificate. There is no plaintext mode and no loopback exemption.

* **Callers** (`Sandboxes`) present one bearer token. When
  `PRELOOP_SANDBOX_TOKEN` is unset the service generates one on first start at
  `<state>/sandbox-client-token` (0600). The CLI resolves `--token` /
  `PRELOOP_SANDBOX_TOKEN`, then that file, and the CA through `--ca` /
  `PRELOOP_SANDBOX_CA`, then `<home>/state/sandbox-ca/ca.pem`. The in-process
  path (`InProcess`) needs no credentials: it is the same process.
* **Providers** authenticate with a client certificate issued by the service
  CA. Every `Providers` RPC except `Enroll` requires one; the service maps the
  leaf's SHA-256 fingerprint to a provider record, so no X.509 parsing happens
  past the handshake. A certificate only acts as the provider it was enrolled
  for: `Register`'s `name` must equal the enrolled name and every other RPC may
  only name its own provider id. Removed providers and expired certificates are
  rejected with `permission_denied`. `PRELOOP_SANDBOX_PROVIDER_TOKEN` is gone.
* **Enrollment** is a single-use token:
  `preloop sandbox token create --ttl 1h` prints
  `plj1.<id>.<secret>.<ca_sha256>` and the store keeps only a hash of the
  secret. `preloop sandbox agent --join https://hub:9191 --token plj1.…`
  exchanges it for a 24h certificate and records the leaf's fingerprint on the
  provider. The agent fetches the service CA first and refuses to send its CSR
  unless that CA hashes to the token's pin. The identity (`key.pem`, `cert.pem`,
  `ca.pem`, `identity.json`, all 0600) lives under
  `<home>/state/sandbox-identity/`; later starts reuse it with no token.
  Re-enrolling an existing name with a new token replaces its fingerprints.
* **Renewal** is automatic: the agent asks for a new certificate once two
  thirds of the lifetime have passed and hot-reloads its client identity. The
  previous fingerprint stays valid until it expires, so a failure between the
  service signing and the agent persisting the new key does not lock it out.
* **Revocation** is `preloop sandbox provider rm <name>`, which deletes the
  provider record and its fingerprints. Its machine records are dropped too:
  once the host cannot authenticate, nothing will ever reconcile them.
* **Data-plane tickets** are single-use, scoped to one sandbox and direction,
  and short-lived; their URLs are relative, so they inherit the `https` base.
* `Report`/`ReportExec`/`Reconcile` are additionally fenced by the provider's
  registration generation, and a provider may not report output for another
  provider's exec, nor destroy another provider's machine.

## State

The service keeps its working set in memory and writes through to its own
store, the restart source: SQLite (`<state>/sandbox.db`, default) or a
`sandbox` schema in Postgres (`PRELOOP_SANDBOX_STORE_URL=postgres://…`). It
never references control tables.

## How preloop uses it

`preloop engine` runs the service in-process with this host's drivers attached
through an in-process agent, and serves the API over HTTPS on
`PRELOOP_SANDBOX_LISTEN` (default `127.0.0.1:9191`; `off` disables). The runner
pool (`preloop_orchestrator::fleet::SandboxMachines`) acquires every VM from it:
goldens are build sandboxes, forks are acquires from the golden's snapshot
template, and deletes are releases. The pool keeps its golden, packed-artifact
and debug-hold strategy; the machines under it are sandboxes.

| Variable | Effect |
|---|---|
| `PRELOOP_SANDBOX_LISTEN` | API address, or `off` |
| `PRELOOP_SANDBOX_TOKEN` | caller bearer token; generated into `<state>/sandbox-client-token` when unset |
| `PRELOOP_SANDBOX_CA` | CA bundle the CLI verifies the service with |
| `PRELOOP_SANDBOX_JOIN_TOKEN` | join token for `sandbox agent` on its first run |
| `PRELOOP_SANDBOX_TLS_SANS` | server certificate SANs (comma list) |
| `PRELOOP_SANDBOX_TLS_CERT` / `PRELOOP_SANDBOX_TLS_KEY` | replace the generated server certificate |
| `PRELOOP_SANDBOX_STORE_URL` | store URL |
| `PRELOOP_SANDBOX_DRIVERS` | drivers on this host (default `smolvm`) |
| `PRELOOP_SANDBOX_POOL_DRIVER` | driver the CI pool places on (default: first) |
| `PRELOOP_SANDBOX_SMOLVM_SLOTS` | concurrent SmolVM sandboxes (0 = no cap) |

## CLI

```sh
preloop sandbox serve --listen 0.0.0.0:9191            # standalone service (+ local SmolVM)
preloop sandbox token create --ttl 1h                  # join token for a provider
preloop sandbox agent --join https://hub:9191 --token plj1.…  # enroll, then reuse the identity
preloop sandbox template put --name alpine --image alpine:3.20
preloop sandbox pool put --name alpine --template alpine --warm 2
preloop sandbox acquire --pool alpine --env JOB=1      # → lease + sandbox JSON
preloop sandbox exec sbx-… -- uname -a
preloop sandbox cp ./dir sbx-…:/work                   # and sbx-…:/path ./out
preloop sandbox stop sbx-…                            # power off, keep the disk
preloop sandbox start sbx-… [--forkable]               # boot it again (bakes)
preloop sandbox snapshot sbx-… --name golden
preloop sandbox release lse-… [--preserve 600]
preloop sandbox ls
preloop sandbox provider ls
preloop sandbox provider rm host-a                     # revoke its certificates
```

## Verification

* `cargo test -p preloop-sandbox` — service/agent conformance over the fake
  driver, in-process and across a real TLS server with a remote enrolled agent
  (idempotency, warm pools, placement errors, unsupported operations,
  snapshot → template → fork, create failure, lease expiry and preservation,
  orphan/lost reconcile, generation fencing, SQLite restart, streaming exec,
  ticketed upload/download), plus enrollment: single-use and expired join
  tokens, CA pinning, name binding, mTLS denial without a certificate,
  renewal, and revocation. Set `PRELOOP_SANDBOX_TEST_PG` to also run the
  Postgres store test.
* `PRELOOP_SANDBOX_E2E_SMOLVM=1 cargo test -p preloop-sandbox --test smolvm_e2e`
  — real SmolVM: bake, snapshot, warm pool of branches, socket mount, files,
  streaming, cleanup.
