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
  `Snapshot`, `Pause`/`Resume`, `Get`/`List`.
* **Providers** (capacity owners): `Enroll` (single-use join token → 24h
  client certificate), `Register` (drivers + capabilities → provider id and
  fencing generation), `Heartbeat`, `Poll` (long-poll for work), `Report` /
  `ReportExec` (fenced by generation), `Reconcile` (full inventory: lost
  sandboxes fail, orphans and stale incarnations are destroyed),
  `RenewCertificate`.

In simple mode both are served over one HTTPS port:
`POST /preloop.sandbox.v1.<Service>/<Method>`. Split mode gives callers and
agents separate ports with the same route families. Every RPC is unary or
server-streaming, so it works over HTTP/1.1 — proxies, and hosts behind NAT
that dial out. Bulk bytes never ride inside RPCs: `OpenStream` returns a
short-lived ticket and a tar moves with plain `PUT`/`GET` on
`/sandbox/v1/streams/<ticket>`; agents fetch and return blobs on
`/sandbox/v1/agent/blobs/<ticket>`.

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

`smolvm serve` has native fork pools, but its clones lose `--mount-socket`
relays (the CLI's branches keep them), so the driver stays on the CLI and pools
are the service's.

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
  leaf's SHA-256 fingerprint to a provider record. A certificate only acts as
  the provider it was enrolled for.
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

## Deployment

The service has a client listener and, optionally, an agent listener:

| Listener | Routes | Default transport | Authentication |
|---|---|---|---|
| Client (`PRELOOP_SANDBOX_LISTEN`) | `Sandboxes`, caller streams, health | TLS with the service CA | bearer token |
| Agent (`PRELOOP_SANDBOX_AGENT_LISTEN`) | `Providers`, `Enroll`, `ca.pem`, agent blobs | TLS with the service CA | client certificate except `Enroll` and `ca.pem` |

When `PRELOOP_SANDBOX_AGENT_LISTEN` is unset, one listener serves both APIs,
which is the simple mode used by the embedded engine and single-host setups.
When it is set, each listener serves only its own routes; the other API returns
404. A Providers or agent-blob request without a certificate reports that the
agent listener must be exposed as raw TCP.

Client TLS is selected by `PRELOOP_SANDBOX_CLIENT_TLS`:

* `service-ca` (default) uses a certificate issued from the service CA. Callers
  pin that CA, so a public certificate cannot impersonate the service.
* `file` uses the PEM files in `PRELOOP_SANDBOX_CLIENT_TLS_CERT` and
  `PRELOOP_SANDBOX_CLIENT_TLS_KEY`. This is for a public certificate or a
  certificate issued by a deployment CA; clients use normal system trust unless
  `--ca` is supplied. Restart to reload either file.
* `plain-loopback` serves the client listener as HTTP and refuses a non-loopback
  bind. Use it only behind a local proxy that provides the external HTTPS
  connection.

An unspecified bind (`0.0.0.0` or `::`) is refused unless
`PRELOOP_SANDBOX_LISTEN_ALL_INTERFACES=1` or `preloop sandbox serve
--listen-all-interfaces` is set. An allowed all-interface bind logs a warning.
`PRELOOP_SANDBOX_ALLOWED_HOSTS` is a comma-separated case-insensitive list of
exact names or `*.suffix` patterns. The guard is active for a loopback client
listener, for `plain-loopback`, or whenever the setting is present. Tailscale
MagicDNS is detected with a short best-effort status query and added to both
the allowlist and generated certificate SANs.

### Direct private-overlay dial

Bind the client and agent listeners to specific interface addresses and dial
them directly over the private overlay network. Pin the service CA for agents
and callers using `PRELOOP_SANDBOX_CA`. **Untested recipe.**

### Tailscale raw TCP agent

Expose the agent port as raw TCP:

```text
tailscale funnel --tcp <port> tcp://localhost:<agent port>
```

The agent connects with its enrolled identity and pins the service CA. HTTPS
Funnel and `--tls-terminated-tcp` terminate TLS and therefore break mTLS.
Include the caller and agent names in `PRELOOP_SANDBOX_TLS_SANS` when clients
will verify the generated certificate by name. **Untested recipe.**

### Cloudflare Tunnel

Put the client listener behind a normal tunnel hostname. Expose the agent
listener with `cloudflared access tcp` so the TLS stream reaches the service
without termination. **Untested recipe.**

### Caddy client proxy

Run the client listener in `plain-loopback` mode and terminate HTTPS at Caddy:

```text
reverse_proxy 127.0.0.1:<port>
```

Set `PRELOOP_SANDBOX_ALLOWED_HOSTS` to the public hostname and include that
name in `PRELOOP_SANDBOX_TLS_SANS` when TLS is served by preloop instead.
**Untested recipe.**

### nginx or HAProxy agent passthrough

Use a TCP stream passthrough; do not configure HTTP proxying or TLS
termination:

```text
stream {
    server {
        listen <public-agent-port>;
        proxy_pass 127.0.0.1:<agent-port>;
    }
}
```

HAProxy uses the equivalent `mode tcp` frontend/backend. **Untested recipe.**

The threat model has two separate controls: the Host guard blocks DNS
rebinding from turning a browser-visible loopback service into a named target,
while the agent channel pins the service CA and never trusts a public CA for
provider identity. A proxy that terminates the agent TLS connection removes
that client-certificate proof and is not an agent deployment.

## State

The service keeps its working set in memory and writes through to its own
store, the restart source: SQLite (`<state>/sandbox.db`, default) or a
`sandbox` schema in Postgres (`PRELOOP_SANDBOX_STORE_URL=postgres://…`). It
never references control tables.

## How preloop uses it

`preloop engine` runs the service in-process with this host's drivers attached
through an in-process agent. With no agent-listener setting it keeps the simple
single HTTPS listener on `PRELOOP_SANDBOX_LISTEN` (default `127.0.0.1:9191`;
`off` disables). The runner pool (`preloop_orchestrator::fleet::SandboxMachines`)
acquires every VM from it.

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
| `PRELOOP_SANDBOX_STORE_URL` | store URL |
| `PRELOOP_SANDBOX_DRIVERS` | drivers on this host (default `smolvm`) |
| `PRELOOP_SANDBOX_POOL_DRIVER` | driver the CI pool places on (default: first) |
| `PRELOOP_SANDBOX_SMOLVM_SLOTS` | concurrent SmolVM sandboxes (0 = no cap) |

## CLI

```sh
preloop sandbox serve --listen 127.0.0.1:9191
preloop sandbox serve --listen 0.0.0.0:9191 --listen-all-interfaces
preloop sandbox token create --ttl 1h                  # join token for a provider
preloop sandbox agent --join https://hub:9191 --token plj1.…  # enroll, then reuse the identity
preloop sandbox agent                                         # later starts reuse the stored URL
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

Without `--service` (or `PRELOOP_SANDBOX_URL`), the CLI reads
`<home>/state/sandbox-endpoint`, which the local service writes when it binds,
and refuses to send its bearer token to any other address.

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
