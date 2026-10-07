# Building official goldens

This runbook covers the architecture-specific official GitHub runner-image
`.smolmachine` artifacts published to GHCR.

## Published artifacts

The engine selects the default artifact from `std::env::consts::ARCH` in
`crates/preloop-orchestrator/src/lib.rs`:

| Host architecture | GHCR package | Platform manifest pin |
|---|---|---|
| `aarch64` | `preloopdev/preloop-arm64-smolvm-golden` | `sha256:cf50db4cbbb38f47f0a533e1e35b523c6427df30261a3cd5bb49d9ef7e072414` |
| `x86_64` | `preloopdev/preloop-x86_64-smolvm-golden` | `sha256:5757a8319aba0604672e62425559f207cc9f21e9380be01e8e80227eaec21914` |

The package tags are `latest` (an OCI index) and
`latest-linux-amd64` / `latest-linux-arm64` (platform manifests). Pin the
platform manifest digest in the engine, not the index digest.

The packages MUST be public. The engine's default OCI path intentionally pulls
without credentials.

## Source image

The official amd64 source is the digest-pinned entry in `official-image.toml`:

```text
ghcr.io/preloopdev/runner-images:ubuntu24-runner-large-latest@sha256:4f7e4be438e4eb9c0f23bebdec12cf1d25520876ad2052412ba18580919b7795
```

Use a host matching the guest architecture. For x86-64, cpane provides Linux,
KVM, and sufficient disk. Keep at least the builder disk plus 20 GiB of pack
staging free; the official recipe uses a 200 GiB builder.

## Why cpane needs a host-side archive

A guest pull uses four workers. Each worker streams one layer through:

```text
crane blob -> decompression -> tar extraction -> guest ext4
           -> virtio-blk -> sparse host disk -> host filesystem
```

GHCR's blob URL is short-lived (approximately five minutes). If extraction and
virtual-disk writes slow a large layer past that lifetime, GHCR closes the
stream. The symptoms are `unexpected EOF` or HTTP/2 `PROTOCOL_ERROR`.

On cpane, pull the exact pinned image on the host first. This keeps the
registry transfer fast and moves the unpacking deadline-free into the local
bake:

```sh
CRANE="$HOME/golden-bake/smolvm-1.18.1-linux-x86_64/agent-rootfs/usr/local/bin/crane"
export DOCKER_CONFIG="$HOME/golden-bake/.docker"
"$CRANE" pull \
  --platform linux/amd64 \
  --format tarball \
  ghcr.io/preloopdev/runner-images@sha256:4f7e4be438e4eb9c0f23bebdec12cf1d25520876ad2052412ba18580919b7795 \
  "$HOME/golden-bake/runner-images.tar"
```

`PRELOOP_REQUIRE_BASE_DIGEST=1` accepts an absolute local archive path because
local files are immutable inputs. Verify the archive before baking: its config
digest and all 18 layer digests MUST match the linux/amd64 registry manifest.

## Export-helper race fix

`smolvm pack create --from-vm` starts a temporary export helper VM. The helper's
storage disk defaults larger than the 200 GiB template. Growing the filesystem
at boot delays the agent's accept loop; the agent's control listener has a
backlog of one, and libkrun can exhaust its connect retries before the helper
accepts all connections. The resulting error is:

```text
Connection reset by peer
```

Both official golden workflows set the helper size to the template size:

```sh
export SMOLVM_EXPORT_HELPER_STORAGE_GIB="$GOLDEN_GUEST_GIB"
```

The template files MUST use the same variable:

```sh
truncate -s "${GOLDEN_GUEST_GIB}G" "$HOME/.smolvm/storage-template.ext4"
truncate -s "${GOLDEN_GUEST_GIB}G" "$HOME/.smolvm/overlay-template.ext4"
```

The verified cpane result was a mount time change from approximately 420 ms to
0 ms, with no connect-retry exhaustion.

## Bake

Build the Linux runner bundle and CLI from the current conformance checkout:

```sh
cargo build --release -p preloop-cli -p preloop-runner-server
cargo build --release -p preloop-runner \
  --target x86_64-unknown-linux-gnu

export PRELOOP_HOME="$HOME/golden-bake/preloop-home"
export PRELOOP_REQUIRE_BASE_DIGEST=1
export PRELOOP_RUNNER_BASE_IMAGE="$HOME/golden-bake/runner-images.tar"
export PRELOOP_RUNNER_STORAGE_GB=200
export PRELOOP_RUNNER_OVERLAY_GB=200
export SMOLVM_EXPORT_HELPER_STORAGE_GIB=200
export SMOLVM_MAX_IMAGE_BYTES=34359738368
export PRELOOP_GOLDEN_NAME_PREFIX=preloop-official-golden-x86-64

./target/release/preloop build-golden \
  --runner-bundle target/x86_64-unknown-linux-gnu/release \
  --base-image "$PRELOOP_RUNNER_BASE_IMAGE" \
  --output dist/preloop-ubuntu-24.04-x86_64
sha256sum dist/preloop-ubuntu-24.04-x86_64 \
  > dist/preloop-ubuntu-24.04-x86_64.sha256
```

`build-golden` bakes the image it is given: `--base-image <ref>` (or the
configured `PRELOOP_RUNNER_BASE_IMAGE` / `[golden] base_image` when the flag is
absent). There is no default — the official golden is published packed and
cannot be built locally. The bake adds the runner contract only; see
[vm-images.md](vm-images.md#what-the-golden-contains).

`build-golden` renames the `.smolmachine` sidecar to the requested output
path. The launcher stub is discarded; the output file itself is the packed
payload.

The orchestrator's pack path sets `SMOLVM_PACK_STAGING`, `TMPDIR`, and a 64 GiB
file-transfer limit. Manual `smolvm pack create` commands MUST set equivalent
staging variables; otherwise cpane's 15 GiB `/tmp` tmpfs can fail with
`Disk quota exceeded` even when the root filesystem has free space.

## Publish

Authenticate with a mode-600 Docker config containing GHCR `packages:write`
credentials. Never commit or print the token.

```sh
smolvm pack push \
  --file dist/preloop-ubuntu-24.04-x86_64 \
  ghcr.io/preloopdev/preloop-x86_64-smolvm-golden:latest
```

For a successful push, verify:

1. `latest` resolves to an OCI index.
2. `latest-linux-amd64` resolves to an OCI image manifest.
3. The platform manifest has artifact type
   `application/vnd.smolmachines.smolmachine.v1`.
4. Its single layer digest equals the SHA-256 of the local packed payload.
5. Anonymous pull of the pinned platform manifest returns HTTP 200.

Then update `GOLDEN_OCI_REF_X86_64` with the
`latest-linux-amd64` manifest digest and run the golden tests.

## Runtime verification

Use a fresh `PRELOOP_HOME`, a separate listen port, and no
`PRELOOP_GOLDEN_OCI_REF` override. This proves the architecture selector and
frozen digest path rather than an operator override:

```sh
export PRELOOP_HOME="$HOME/.preloop-golden-smoke"
export PRELOOP_LISTEN=127.0.0.1:9191
preloop serve --listen 127.0.0.1:9191
```

Submit two independent jobs. A useful smoke pair is:

- a bare-metal job exercising Node, Go, Python, and Docker;
- a `container: python:3.12-slim` job.

The server log MUST show the pinned x86_64 OCI reference, a completed OCI
layer-digest verification, and successful job VM creation from the downloaded
payload. The job logs MUST show both jobs reaching success.

## Checks

```sh
cargo fmt --all -- --check
cargo test --locked -p preloop-orchestrator --lib golden
just test-ci
```

The package, digest pin, workflow guards, and this runbook are separate from
secret material. Do not add GHCR tokens, system tokens, or sudo passwords to
the repository.
