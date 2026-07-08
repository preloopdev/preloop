#!/usr/bin/env bash
# vm-setup-common.sh — runs inside a per-job smolvm before the runner
# Mounts expected:
#   /workspace             — aksh source + binaries (macos-runners)
#   /workspace/.rustup     — Linux ARM64 toolchains (cachingv4/.rustup)
#   /workspace/.cargo      — cargo registry cache (cachingv4/.cargo)
#   /opt/actions-runner    — official C# runner (cachingv4/actions-runner)
set -euo pipefail

log() { echo "[setup $(date +%T.%3N)] $*"; }
# Ensure Rust toolchain is in PATH (for pre-packed VMs)
if [ -d /root/.cargo/bin ]; then
  export PATH="/root/.cargo/bin:$PATH"
fi

if command -v node &>/dev/null && command -v git &>/dev/null; then
  log "Packages already installed (node $(node --version 2>/dev/null))"
else
  log "Installing packages..."
  apt-get update -qq
  apt-get install -y -qq --no-install-recommends git curl ca-certificates nodejs npm 2>&1 | tail -1
  log "Packages installed (node $(node --version 2>/dev/null || echo '?'))"
fi

# Verify caches are mounted
for d in /workspace/.rustup /workspace/.cargo; do
  if [ -d "$d" ]; then
    log "Cache present: $d ($(du -sh "$d" 2>/dev/null | cut -f1))"
  else
    log "WARNING: Cache not found: $d — jobs will download from network"
  fi
done

if [ -d /opt/actions-runner ]; then
  log "Official runner present: /opt/actions-runner"
fi

log "Setup complete"
