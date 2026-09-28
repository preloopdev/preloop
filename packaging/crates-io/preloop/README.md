# preloop

Name reservation. **The `preloop` CLI is not installed with `cargo install`.**

A single binary embeds the control plane, the orchestrator, and the microVM
runner pool, so preloop ships as a prebuilt binary:

| Channel | Command |
|---|---|
| Homebrew (macOS, Linuxbrew) | `brew install preloopdev/tap/preloop` |
| npm | `npm install -g @preloop-dev/cli` |
| Release archive | `preloop-cli-<target>.tar.gz` from [releases](https://github.com/preloopdev/preloop/releases/latest) |

Every release asset is checksummed and carries keyless build provenance:

```sh
base=https://github.com/preloopdev/preloop/releases/latest/download
asset=preloop-cli-aarch64-apple-darwin
curl -fsSLO "$base/$asset.tar.gz" "$base/$asset.tar.gz.sha256"
shasum -a 256 -c "$asset.tar.gz.sha256"          # GNU/Linux: sha256sum -c
gh attestation verify "$asset.tar.gz" --repo preloopdev/preloop
```

preloop is a local, self-hosted GitHub Actions control plane: the official
runner protocol, hardware-isolated microVM jobs, and a debug session for every
failed job. Documentation and source:
<https://github.com/preloopdev/preloop>.
