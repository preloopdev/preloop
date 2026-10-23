# Sandbox drivers

The Sandbox Service creates every machine through a **driver**: the host-side
interface that turns generic operations (`create`, `exec`, `copy`, `snapshot`,
…) into one VM technology's API. `smolvm` and `fake` are in-process drivers.
A driver plugin is the same interface in another process, in any language,
speaking the `preloop.sandbox.driver.v1.Driver` protocol over a Unix socket.

The protocol is defined in `proto/preloop/sandbox/driver/v1/driver.proto` and
mirrors the Rust trait in `crates/preloop-sandbox/src/driver.rs` one-to-one. A
plugin sees exactly the work an in-process driver sees.

## Architecture

```text
                     Sandbox Service (HTTPS, Connect)
                              │   Providers API: poll / report / reconcile
                              ▼
                        host agent
                 ┌────────────┴────────────┐
                 │                         │
        in-process drivers          external driver shim
        smolvm, fake, …             (crates/preloop-sandbox/src/drivers/external.rs)
                                           │ Connect h2c
                                           │ unix:///…/acme.sock
                                           ▼
                                   driver plugin process
                                   (any language; your VM backend)
```

The agent and the plugin are on the same host. The plugin is not a remote
control plane and does not talk to the Sandbox Service.

### What stays in the agent

Moving a driver out of process does **not** move the trust boundary. The agent
keeps everything that is not a VM operation:

* enrollment, mTLS client identity, `Register`/`Heartbeat`, fencing
  generations, work polling and acknowledgement;
* at-least-once delivery bookkeeping: work ids, deduplication, retries and
  redelivery;
* placement inputs: driver name, capabilities and capacity;
* the host mount/socket allowlist, checked **before** a `CreateWork` reaches
  the plugin;
* caller lease authorization, request validation, exec deadlines and the
  caller-facing error mapping;
* blob transfer: `OpenStream` tickets and the agent blob endpoints. The agent
  stages a tar at a host path and tells the plugin where it is; the plugin
  never receives a ticket or a service URL;
* reconciliation: inventory is compared with the service's records, orphans
  are destroyed and lost sandboxes fail.

The plugin only performs backend work: create/destroy/exec/copy/snapshot and
the optional power operations. It receives no service credentials (see
[Security](#security)).

## Protocol

All RPCs are unary except `Exec`, which is server-streaming. The service name
is `preloop.sandbox.driver.v1.Driver`.

| RPC | Request → response | Semantics |
|---|---|---|
| `GetInfo` | `GetInfoRequest` → `GetInfoResponse` | Identity, protocol version, capabilities and plugin version. Called once at start and after every restart. `agentProtocolVersion` is the agent's version; return `protocolVersion = 1`. The agent refuses a mismatch. |
| `Create` | `CreateRequest{work}` → `CreateResponse{nativeRef}` | Create and start a sandbox from `CreateWork`, return its native reference. Idempotent on `work.sandboxId`. |
| `Destroy` | `NativeRequest{nativeRef}` → `Empty` | Stop and delete the sandbox. A sandbox that is already gone is success. |
| `Exec` | `ExecRequest{nativeRef, execId, request}` → stream of `ExecEvent` | Run the command, stream stdout/stderr, and end with exactly one `ExitCode` event. `request.timeoutSecs` is never zero; kill at the deadline and report `124`. |
| `CancelExec` | `CancelExecRequest{nativeRef, execId}` → `Empty` | Kill a running command; report `130`. May arrive before `Exec`; remember it and refuse to start that exec. Cancelling a finished exec succeeds. |
| `CopyIn` | `CopyInRequest{nativeRef, hostTar, guestDir}` → `Empty` | Extract the agent-staged tar at `hostTar` into `guestDir` in the guest. |
| `CopyOut` | `CopyOutRequest{nativeRef, guestPath, hostTar}` → `Empty` | Write a tar of `guestPath` to `hostTar`. The agent created and owns that file. |
| `Inventory` | `InventoryRequest` → `InventoryResponse{sandboxes}` | Every sandbox this plugin owns on this host. Each `Observed` carries `driver`, `nativeRef` and `sandboxId` (when recoverable). |
| `HostReport` | `HostReportRequest` → `HostReport` | Best-effort host and per-sandbox runtime facts. Never fails; return an empty report when nothing can be measured. |
| `Snapshot` | `SnapshotRequest{nativeRef, template}` → `SnapshotResponse{nativeRef}` | Capture the sandbox as a template. Only when `capabilities.snapshot`; return `failed_precondition` otherwise. |
| `DropSnapshot` | `NativeRequest{nativeRef}` → `Empty` | Release a snapshot. Idempotent; an unknown snapshot is success. |
| `Stop` | `NativeRequest{nativeRef}` → `Empty` | Power off, keeping the disk. Only when `capabilities.pause`. |
| `Start` | `StartRequest{nativeRef, forkable}` → `Empty` | Boot a stopped sandbox; `forkable` asks for a branch base. Only when `capabilities.pause`. |
| `Pause` | `NativeRequest{nativeRef}` → `Empty` | Pause with live state. Only when `capabilities.pause`. |
| `Resume` | `NativeRequest{nativeRef}` → `Empty` | Resume a paused sandbox. Only when `capabilities.pause`. |

`fork` is not a separate RPC: a fork is a `Create` whose `work.template`
carries a snapshot `nativeRef` and whose `work.snapshotCapable` asks for a new
branch base. The `fork` capability only advertises that the backend can do it.

### Capabilities

`GetInfoResponse.capabilities` is the same message the agent registers with the
service (`preloop.sandbox.v1.Capabilities`). The fields that matter to a
plugin:

| Field | Meaning |
|---|---|
| `os`, `arch` | Guest OS and architectures (`linux`/`macos`/`windows`; `x86_64`/`aarch64`). |
| `features` | Free-form labels used by placement (`docker`, `rosetta`, …). |
| `pause` | Live pause/resume **and** stop/start are supported. When false, all four RPCs must return `failed_precondition`. |
| `snapshot` | `Snapshot`/`DropSnapshot` are supported. |
| `fork` | A snapshot template can be used as a fork base. |
| `hostMounts`, `socketMounts` | The backend can mount host directories/sockets. |
| `networkModes` | Enforceable egress modes. Do not advertise `NETWORK_MODE_UNSPECIFIED`. |
| `slots` | Concurrent sandboxes on this host, or `0` for unbounded. |
| `oneShot` | Each sandbox runs exactly one command; clients needing several need another driver. |
| `maxVcpus`, `maxMemoryMib`, `defaultShape` | Capacity ceilings and the size applied to zero fields. |

A capability is a promise. Declaring `snapshot` and then failing `Snapshot` is
a conformance failure; not declaring it and succeeding anyway is also a
failure, because the service will never route the operation.

## Idempotency and errors

Work delivery is **at least once**. A plugin must assume `Create`, `Destroy`,
`Exec` and the copy/power RPCs can be redelivered:

* **`Create` is keyed by `work.sandboxId`.** A redelivered create for an id
  the plugin already made returns the **same** `nativeRef`; it must not create
  a second machine. Sandbox ids are never reused, so a plugin may keep the
  mapping for the sandbox's lifetime. The same id with a different payload is
  a conflict (`failed_precondition` or `already_exists`), never a second
  machine.
* **`Destroy` is idempotent.** A second destroy and a destroy of an unknown
  reference both succeed. Returning `not_found` is acceptable: the agent maps
  it to success for `Destroy` and `DropSnapshot`.
* **`CancelExec` is idempotent.** Record a cancel that arrives before the exec
  starts; the next `Exec` for that id must return `130` without running the
  command. Cancelling a finished or unknown exec succeeds.
* **`CopyOut` overwrites `hostTar`.** The agent creates the file and consumes
  it immediately.
* **Power operations are idempotent where the backend allows it.** Stopping a
  stopped sandbox and starting a running one should converge, not corrupt
  state.
* **Rebuild inventory from the backend after a restart.** Do not rely on
  process memory as the record of which machines exist.

Connect error codes carry the trait's meaning:

| Code | Use |
|---|---|
| `invalid_argument` | Malformed native ref, argv, tar path or template. |
| `failed_precondition` | The operation is not declared in capabilities, or the request is refused (paused/stopped sandbox, an immutable setting that cannot be applied, a conflicting create). |
| `not_found` | The native reference does not exist. The agent treats this as success for `Destroy` and `DropSnapshot`. |
| `unavailable` | Transient backend failure. The agent reports a failed work item; the service retries or reconciles later. |
| `resource_exhausted` | Host capacity is exhausted. |
| `internal` | A plugin bug. Include a message that identifies the operation. |

An error a plugin returns — including its own `unavailable` — fails that work
item and passes through to the caller; it never takes the plugin out of
service. Only a transport failure (the connection refused, reset, or gone)
makes the agent mark the plugin down and reconnect or restart it.

Exit codes are part of the contract: `124` for a command killed at its
deadline, `130` for a cancelled command. `Exec` must still send one terminal
exit event for those cases; if the RPC itself fails, return a Connect error and
end the stream.

## Configuration

The engine's host agent reads `PRELOOP_SANDBOX_DRIVERS`, a comma-separated
list (default `smolvm`). An entry prefixed with `external:` is a driver plugin
spec:

```sh
# Spawn: the agent starts the binary and appends `--socket <path>`.
PRELOOP_SANDBOX_DRIVERS='external:acme=/usr/local/bin/acme-driver'

# Spawn with arguments. Arguments are split on whitespace; there is no shell
# quoting, so use a small wrapper script when an argument contains spaces.
PRELOOP_SANDBOX_DRIVERS='external:acme=/usr/local/bin/acme-driver --config /etc/acme.toml'

# Socket: the plugin is already running (systemd, launchd, a container, …).
PRELOOP_SANDBOX_DRIVERS='external:acme=unix:///run/acme/driver.sock'

# Mix in-tree and external drivers.
PRELOOP_SANDBOX_DRIVERS='smolvm,external:acme=/usr/local/bin/acme-driver'
```

The spec is `name=target`:

* `name` is the driver name reported to the service and used for the socket
  file. It may contain letters, digits, `-`, `_` and `.`, and is at most 64
  bytes. This is the identity placement, pools and
  `PRELOOP_SANDBOX_POOL_DRIVER` address, so it wins even when
  `GetInfoResponse.name` differs (the agent logs a warning in that case;
  `GetInfo.name` is display metadata, not the registration key). Two entries
  that register the same name are a startup error naming both of them.
* `target` is either a binary path (optionally followed by whitespace-split
  arguments) or `unix:///absolute/path.sock`.
* Commas separate entries, so an argument containing a comma needs a wrapper
  script.

**Spawn mode.** The agent creates a private runtime directory (mode `0700`,
`<home>/state/driver-sockets` in the engine), checks `<runtime>/<name>.sock`,
starts the binary with its configured arguments plus
`--socket <runtime>/<name>.sock`, and polls `GetInfo` for up to 30 seconds. A
socket that still accepts connections belongs to a live plugin (for example
one an earlier agent left behind): the agent refuses to start a second plugin
on it instead of giving two processes the same backend. A socket nobody
serves is stale and is replaced. The plugin must create the socket and serve
in the foreground; the agent owns the process and restarts it with exponential
backoff (1–30 s) if it exits. RPCs return `unavailable` while it is down. Do
not daemonize in spawn mode.

The spawned plugin runs in its own process group. Stopping the agent (SIGINT
or SIGTERM) and dropping the driver both kill that group, so a plugin's own
children do not outlive it.

**Socket mode.** The plugin creates the socket itself and must set mode
`0600`; the agent only dials it. The agent reconnects in the background after a
transport failure, so the plugin must accept a new client without restarting
the backend. Socket mode is right when a supervisor outside the agent owns the
plugin's lifecycle.

The socket is created by `driver_plugin::serve` with mode `0600`; a stale
socket at the same path is removed first. For a Rust driver, the reference
implementation is:

```sh
cargo run -p preloop-sandbox --example fake_driver_plugin -- --socket /tmp/fake-driver.sock
```

and `preloop sandbox driver serve --driver fake --socket /tmp/fake-driver.sock`
serves an in-tree driver the same way.

## Generating stubs

Vendor the two protos into your repository:

```text
proto/preloop/sandbox/v1/sandbox.proto
proto/preloop/sandbox/driver/v1/driver.proto
```

or point the input at a Buf Schema Registry module that publishes them. A
`buf.gen.yaml` for Go, TypeScript and Python:

```yaml
version: v2
inputs:
  - directory: proto
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: example.com/acme-driver/gen/go
plugins:
  - local: protoc-gen-go
    out: gen/go
    opt: paths=source_relative
  - local: protoc-gen-connect-go
    out: gen/go
    opt: paths=source_relative
  # Connect-ES v2: protoc-gen-es emits messages, services and clients.
  # With Connect-ES v1, add protoc-gen-connect-es here as well.
  - local: protoc-gen-es
    out: gen/ts
    opt: target=ts
  - local: protoc-gen-python
    out: gen/python
  - local: protoc-gen-connect-python
    out: gen/python
```

```sh
buf generate
```

Install the plugins first: `protoc-gen-go`/`protoc-gen-connect-go`
(`connectrpc.com/connect/cmd/protoc-gen-connect-go`),
`@bufbuild/protoc-gen-es`, and `connect-python` (which provides
`protoc-gen-connect-python`). The generated package names and import paths
depend on `go_package_prefix` and the output layout; the skeleton below uses
one common layout.

## Go skeleton

This is illustrative, not a complete backend. It shows the shape of the
service and the rules that are easy to get wrong: idempotent create, destroy
that treats “already gone” as success, a stream that always ends with exactly
one exit event, and the socket setup.

```go
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	driverv1 "example.com/acme-driver/gen/go/preloop/sandbox/driver/v1"
	"example.com/acme-driver/gen/go/preloop/sandbox/driver/v1/driverv1connect"
	sandboxv1 "example.com/acme-driver/gen/go/preloop/sandbox/v1"
)

type driver struct {
	mu        sync.Mutex
	sandboxes map[string]*machine // keyed by work.sandboxId
}

func (d *driver) GetInfo(context.Context, *connect.Request[driverv1.GetInfoRequest]) (*connect.Response[driverv1.GetInfoResponse], error) {
	return connect.NewResponse(&driverv1.GetInfoResponse{
		Name:            "acme",
		ProtocolVersion: 1,
		Capabilities: &sandboxv1.Capabilities{
			Os:           "linux",
			Arch:         []string{"x86_64"},
			Pause:        true,
			Snapshot:     true,
			NetworkModes: []sandboxv1.NetworkMode{sandboxv1.NetworkMode_DISABLED, sandboxv1.NetworkMode_UNRESTRICTED},
		},
	}), nil
}

func (d *driver) Create(ctx context.Context, req *connect.Request[driverv1.CreateRequest]) (*connect.Response[driverv1.CreateResponse], error) {
	work := req.Msg.GetWork()
	if work.GetSandboxId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("sandboxId is required"))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// A redelivered create must return the same machine.
	if existing, ok := d.sandboxes[work.GetSandboxId()]; ok {
		return connect.NewResponse(&driverv1.CreateResponse{NativeRef: existing.ref}), nil
	}
	ref, err := startMachine(ctx, work) // your backend
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	d.sandboxes[work.GetSandboxId()] = &machine{ref: ref}
	return connect.NewResponse(&driverv1.CreateResponse{NativeRef: ref}), nil
}

func (d *driver) Destroy(ctx context.Context, req *connect.Request[driverv1.NativeRequest]) (*connect.Response[sandboxv1.Empty], error) {
	if err := stopMachine(ctx, req.Msg.GetNativeRef()); err != nil && !errors.Is(err, errNotFound) {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(&sandboxv1.Empty{}), nil
}

func (d *driver) Exec(ctx context.Context, req *connect.Request[driverv1.ExecRequest], stream *connect.ServerStream[sandboxv1.ExecEvent]) error {
	code, err := runCommand(ctx, req.Msg, func(isStdout bool, chunk []byte) error {
		event := &sandboxv1.ExecEvent{}
		if isStdout {
			event.Event = &sandboxv1.ExecEvent_Stdout{Stdout: chunk}
		} else {
			event.Event = &sandboxv1.ExecEvent_Stderr{Stderr: chunk}
		}
		return stream.Send(event)
	})
	if err != nil {
		return connect.NewError(connect.CodeUnavailable, err)
	}
	return stream.Send(&sandboxv1.ExecEvent{
		Event: &sandboxv1.ExecEvent_ExitCode{ExitCode: int32(code)},
	})
}

func (d *driver) CancelExec(_ context.Context, req *connect.Request[driverv1.CancelExecRequest]) (*connect.Response[sandboxv1.Empty], error) {
	// Remember a cancel that beats the exec; it must refuse to start that id.
	d.cancel(req.Msg.GetExecId())
	return connect.NewResponse(&sandboxv1.Empty{}), nil
}

// Implement Inventory and HostReport, plus Snapshot/DropSnapshot and
// Stop/Start/Pause/Resume for the capabilities declared above. An undeclared
// optional method returns connect.CodeFailedPrecondition.

func main() {
	socket := flag.String("socket", "", "unix socket path from the agent")
	flag.Parse()
	if *socket == "" {
		fmt.Fprintln(os.Stderr, "usage: acme-driver --socket <path>")
		os.Exit(2)
	}
	_ = os.Remove(*socket) // stale socket from a previous process
	listener, err := net.Listen("unix", *socket)
	if err != nil {
		panic(err)
	}
	if err := os.Chmod(*socket, 0o600); err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	path, handler := driverv1connect.NewDriverHandler(&driver{sandboxes: map[string]*machine{}})
	mux.Handle(path, handler)
	server := &http.Server{Handler: h2c.NewHandler(mux, &http2.Server{})}
	panic(server.Serve(listener)) // the agent supervises the process
}
```

## Conformance

`crates/preloop-sandbox/src/driver_conformance.rs` runs the behavior the
service depends on against any `Driver`: in-process, or through
`ExternalDriver` over the protocol. It checks:

| Case | What it proves |
|---|---|
| `capabilities` | Name, OS/arch, feature deduplication, known network modes, capacity ceilings and default shape are self-consistent. |
| `create_inventory` | Create returns a parseable native name and inventory lists the sandbox exactly once; destroy removes it. |
| `create_idempotent` | A redelivered `sandbox_id` returns the same native ref and does not create a second machine. |
| `destroy_idempotent` | Destroying twice and destroying an unknown ref both succeed. |
| `exec_streams` | stdout and stderr are forwarded and the returned exit code is the command's. |
| `exec_timeout` | A command past `timeoutSecs` is killed and reports `124`. |
| `cancel_exec` | A cancel before start refuses the exec; cancelling a running exec ends it with `130`. |
| `copy_round_trip` | A tar copied in and out has identical paths and bytes. |
| `snapshot` | Declared: capture and release a snapshot. Not declared: `failed_precondition`. |
| `stop_start` | Declared (`pause`): stop and start work. Not declared: `failed_precondition`. |
| `pause_resume` | Declared (`pause`): pause and resume work. Not declared: `failed_precondition`. |
| `host_report` | The report returns within the case budget. |

Each case creates its own sandbox from a minimal `CreateWork` and destroys it
afterwards, including after a failure or timeout. Exec-based cases are marked
`skipped` when `guest_supports_shell` is false.

The CLI runs the suite with the defaults (empty template, shell assumed, 5 s
exec deadline, 180 s per case):

```sh
preloop sandbox driver conformance --driver fake
preloop sandbox driver conformance --driver 'external:acme=/usr/local/bin/acme-driver'
preloop sandbox driver conformance --driver 'external:acme=unix:///run/acme/driver.sock'
```

`--driver` accepts the same `external:`-prefixed spec
`PRELOOP_SANDBOX_DRIVERS` uses; the prefix may be omitted. A spec that
neither names an in-tree driver nor parses is rejected with the accepted
forms in the message.

It prints one line per case, then `N passed, M failed, K skipped`, and exits
non-zero when any case failed (skips do not fail the run).

A real VM driver needs a pinned image. Call the library with
`ConformanceOptions::new`:

```rust
use std::time::Duration;
use preloop_sandbox::api::{Artifact, Template, TemplateSpec};
use preloop_sandbox::driver_conformance::{ConformanceOptions, run};

let template = Template {
    spec: Some(TemplateSpec {
        name: "conformance".into(),
        os: "linux".into(),
        artifacts: vec![Artifact {
            format: Artifact::OCI_IMAGE.into(),
            arch: "aarch64".into(),
            r#ref: "ghcr.io/acme/base@sha256:…".into(),
            digest: "sha256:…".into(),
            ..Artifact::default()
        }],
        ..TemplateSpec::default()
    })
    .into(),
    ..Template::default()
};
let options = ConformanceOptions::new(template)
    .with_shell(true)
    .with_exec_timeout(Duration::from_secs(5));
let results = run(driver, &options).await;
```

`ConformanceOptions` also has `with_case_timeout` and
`with_create_options` for driver-specific `CreateWork.options` hints.

## Security

The plugin runs on the host that runs the agent and can control VMs there.
Treat it as trusted host software:

* **The socket is the whole authentication boundary.** Spawn mode creates the
  socket with mode `0600` inside a `0700` runtime directory. Socket mode must
  do the same itself. Never place it in `/tmp` or another world-writable
  directory; use a private runtime directory (`RuntimeDirectory=` under
  systemd, a private Application Support directory on macOS).
* **The plugin never receives service credentials.** There is no service URL,
  bearer token, join token, CA private key, client certificate or blob ticket
  in the protocol. A plugin sees `CreateWork`, native refs and agent-chosen
  host tar paths — nothing that could enroll another provider or read the
  service store.
* **The agent enforces the mount allowlist first.** `work.mounts` and
  `work.sockets` have already been admitted against the agent's
  `PRELOOP_SANDBOX_MOUNT_ALLOW` / `PRELOOP_SANDBOX_SOCKET_ALLOW` prefixes. Use
  the paths verbatim; do not resolve symlinks, widen a prefix or add mounts.
* **Validate native references.** Do not interpolate them into a shell or use
  them as unchecked path components. The in-tree fake driver rejects `/` and
  `..` for exactly this reason.
* **Do not execute guest commands on the host.** `Exec` argv comes from
  callers; run it inside the guest, under the requested guest user, and apply
  `request.workDir`, `request.env` and `request.stdin`.
* **Keep host facts out of `HostReport` unless the service stores them.** The
  Stats API intentionally carries aggregates only: no host PIDs, process names
  or data-directory paths. Report per-sandbox memory/start time and a host
  sample, not raw host process data.
* **Stay in the foreground in spawn mode.** The agent owns the process and
  restarts it; a daemonizing plugin looks like a crash loop. Socket-mode
  plugins must survive the agent reconnecting without losing backend state;
  rebuild inventory from the backend after a plugin restart.
* **Update carefully.** The protocol version is compiled into the agent
  (`DRIVER_PROTOCOL_VERSION = 1`). A plugin reporting another version is
  refused at startup, so ship the plugin and the agent together and roll the
  agent only after the plugin speaks the new version.

Repository references: `proto/preloop/sandbox/driver/v1/driver.proto`,
`crates/preloop-sandbox/src/driver.rs`,
`crates/preloop-sandbox/src/drivers/external.rs`,
`crates/preloop-sandbox/src/driver_plugin.rs`,
`crates/preloop-sandbox/src/driver_conformance.rs`, and
`crates/preloop-sandbox/tests/external_driver.rs`.
