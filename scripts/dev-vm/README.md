# preloop-dev: per-worktree SmolVM dev VMs

Each Git worktree gets its own Linux VM, forked copy-on-write from one frozen
golden (`preloop-dev`) that already holds the Rust toolchain, `cargo-nextest`,
`cargo-vet`, build dependencies, preloop and OMP. The worktree is mounted
read-write at `/workspace`, so host edits are visible immediately and nothing
needs syncing. `target/` is VM-local, so worktrees never share build artifacts.

## For agents

```sh
just dev-vm-check              # exit 0 only if the golden exists on this host
just dev-vm-up                 # fork it for this worktree (idempotent)
just dev-vm-exec cargo nextest run --locked -p preloop-runner-server
just dev-vm-shell omp          # interactive; no args = bash
just dev-vm-down               # delete this worktree's VM (worktree untouched)
just dev-vm-ls
```

If `dev-vm-check` fails, use the host toolchain. Note: SmolVM on macOS guests
have no nested KVM, so tests that boot another VM must run elsewhere.

## Reaching servers started in the VM

SmolVM does **not** relay the guest's `localhost` to the host. To reach a
server from the host, publish a port and bind `0.0.0.0` in the guest
(`127.0.0.1` in the guest is not reachable):

```sh
PRELOOP_DEV_PORTS="18080:8080" just dev-vm-up      # at creation, HOST:GUEST
just dev-vm-publish 18081:8081                     # later; restarts the VM
```

Host ports are machine-wide, so use distinct ones per worktree.

## Brokered OMP (provider keys stay on the host)

Set both of these to a gateway reachable *from the guest* (not `127.0.0.1`):

```sh
export PRELOOP_DEV_GATEWAY_URL=http://<host-addr>:4000
export PRELOOP_DEV_GATEWAY_TOKEN_FILE=/path/to/auth-gateway.token
```

`dev-vm-up` routes every provider the gateway serves through it. The token is
injected per command with `--secret-file`, never written to the machine record
or guest disk, so rotating it needs no VM rebuild. Anything running in the VM
can still use the token during a command to make brokered model requests; it
cannot read the provider credentials. Run `omp auth-broker serve` and
`omp auth-gateway serve --bind=<host-addr>:4000` on the host first.

## Building the golden (once per host)

```sh
smolvm machine create --name preloop-dev --image ubuntu:24.04 --net \
  --cpus 6 --mem 16384 --storage 80 --overlay 30 -- \
  /bin/sh -c 'trap : TERM INT; while :; do sleep 3600; done'
smolvm machine start --name preloop-dev --branchable
smolvm machine exec --name preloop-dev --timeout 30m -- sh -lc '
  apt-get update && apt-get install -y --no-install-recommends \
    build-essential clang lld cmake pkg-config libssl-dev libsqlite3-dev \
    protobuf-compiler git curl jq unzip zstd ca-certificates openssh-client \
    python3 just iproute2
  curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
  . /root/.cargo/env && rustup toolchain install 1.99.0 && rustup default 1.99.0
  cargo install cargo-nextest --locked --version 0.9.146
  cargo install cargo-vet --locked --version 0.10.1
  curl -fsSL https://raw.githubusercontent.com/preloopdev/preloop/main/install.sh | sh
  curl -fsSL https://omp.sh/install | sh -s -- --binary'
# Freeze it: branching once with --freeze-source leaves the source as the base.
smolvm machine branch --from preloop-dev --name probe --freeze-source
smolvm machine delete --name probe --force
```

Keep the golden free of credentials and checkouts. Pause/resume of the golden
corrupted its checkpoint in testing; do not use `machine pause` on it.
Override names with `PRELOOP_DEV_BASE` / `PRELOOP_DEV_CHILD_PREFIX`; point at a
different smolvm with `SMOLVM_BIN`. No host-specific paths are used.
