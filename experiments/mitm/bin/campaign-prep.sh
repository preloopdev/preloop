#!/usr/bin/env bash
# campaign-prep.sh — provision the two campaign VMs on this host.
#   camp-gh : github cells (official runner + preloop-runner, mitm capture)
#   camp-pl : preloop cells (server-side capture, runners on LAN IP)
# Idempotent; safe to re-run. Requires: smolvm, curl; the campaign checkout
# at $PWD (for building preloop-runner inside the build VM).
set -euo pipefail

RUNNER_VERSION="${RUNNER_VERSION:-2.338.0}"
RUNNER_TARBALL="/tmp/actions-runner-linux-arm64-$RUNNER_VERSION.tar.gz"
SRC_TARBALL="/tmp/preloop-src-338.tar.gz"
CA_SRC="${MITM_CA:-$PWD/.cache/mitmproxy/mitmproxy-ca-cert.pem}"

log() { echo "[prep] $*"; }
vexec() { smolvm machine exec --name "$1" -- bash -lc "$2"; }

# --- fetch official runner tarball on host ---------------------------------
if [ ! -f "$RUNNER_TARBALL" ]; then
  log "downloading actions-runner v$RUNNER_VERSION (linux-arm64)"
  curl -fsSL "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-arm64-${RUNNER_VERSION}.tar.gz" -o "$RUNNER_TARBALL"
fi

# --- source bundle for in-VM preloop-runner build ---------------------------
log "packaging preloop source"
git -C "$PWD" archive --format=tar.gz -o "$SRC_TARBALL" HEAD 2>/dev/null || {
  tar --exclude='./target' --exclude='./.git' -czf "$SRC_TARBALL" -C "$PWD" .
}

mk_vm() {
  local name="$1"
  if smolvm machine ls --json 2>/dev/null | grep -q "\"name\": *\"$name\""; then
    log "vm $name exists"
    smolvm machine start --name "$name" >/dev/null 2>&1 || true
    return 0
  fi
  log "creating vm $name (ubuntu:24.04)"
  smolvm machine create --name "$name" --net --image ubuntu:24.04 --cpus 4 --memory 8192 2>/dev/null \
    || smolvm machine create --name "$name" --net --image ubuntu:24.04
  smolvm machine start --name "$name" >/dev/null
}

provision_common() {
  local name="$1"
  log "installing base packages in $name"
  vexec "$name" "export DEBIAN_FRONTEND=noninteractive; apt-get update -qq &&
    apt-get install -y -qq curl ca-certificates git jq python3 sudo tar gzip unzip xz-utils iproute2 >/dev/null &&
    (apt-get install -y -qq docker.io >/dev/null 2>&1 || true)"
  # dockerd, best-effort (may not run in all guests; container scenarios degrade)
  vexec "$name" "nohup dockerd > /var/log/dockerd.log 2>&1 & sleep 3; docker info >/dev/null 2>&1 && echo DOCKER_OK || echo DOCKER_UNAVAILABLE" || true
  # runner layout
  vexec "$name" "mkdir -p /opt/runner/actions-runner /opt/plrunner /etc/mitmproxy"
  smolvm machine cp "$RUNNER_TARBALL" "$name:/tmp/runner.tar.gz"
  vexec "$name" "tar xzf /tmp/runner.tar.gz -C /opt/runner/actions-runner && /opt/runner/actions-runner/bin/installdependencies.sh >/dev/null 2>&1 || true"
  # mitm CA (only needed by gh cells but harmless to install everywhere)
  if [ -f "$CA_SRC" ]; then
    smolvm machine cp "$CA_SRC" "$name:/etc/mitmproxy/mitmproxy-ca-cert.pem" || true
    vexec "$name" "cp /etc/mitmproxy/mitmproxy-ca-cert.pem /usr/local/share/ca-certificates/mitmproxy.crt && update-ca-certificates >/dev/null 2>&1 || true"
  fi
}

build_preloop_runner() {
  local name="$1"
  log "building preloop-runner in $name (this takes a while once)"
  smolvm machine cp "$SRC_TARBALL" "$name:/tmp/preloop-src.tar.gz"
  vexec "$name" "rm -rf /opt/prevsrc && mkdir -p /opt/prevsrc && tar xzf /tmp/preloop-src.tar.gz -C /opt/prevsrc"
  vexec "$name" "export DEBIAN_FRONTEND=noninteractive; apt-get install -y -qq build-essential pkg-config libssl-dev >/dev/null 2>&1 || true;
    if ! command -v cargo >/dev/null; then curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none >/dev/null 2>&1; fi"
  vexec "$name" "export PATH=\$HOME/.cargo/bin:\$PATH; cd /opt/prevsrc &&
    (rustup toolchain install 2>/dev/null || rustup default stable) &&
    cargo build --release -p preloop-runner 2>&1 | tail -3 &&
    cp target/release/preloop-runner /opt/plrunner/preloop-runner"
  vexec "$name" "/opt/plrunner/preloop-runner --version || true"
}

mk_vm camp-gh
mk_vm camp-pl
provision_common camp-gh
provision_common camp-pl
build_preloop_runner camp-gh
vexec camp-pl "cp /opt/plrunner/preloop-runner /dev/null 2>/dev/null; mkdir -p /opt/plrunner"
smolvm machine cp "camp-gh:/opt/plrunner/preloop-runner" /tmp/preloop-runner-guest 2>/dev/null \
  && smolvm machine cp /tmp/preloop-runner-guest camp-pl:/opt/plrunner/preloop-runner \
  && vexec camp-pl "chmod +x /opt/plrunner/preloop-runner" \
  || { echo "WARN: guest binary copy failed; building in camp-pl too"; build_preloop_runner camp-pl; }

log "vm state:"; smolvm machine ls | grep -E "camp-(gh|pl)" || true
log "done"
