#!/bin/sh
# Exit 0 when the preloop-dev golden exists on this host and can be forked.
# Agents: run this first; fall back to the host toolchain when it fails.
set -eu
. "$(dirname "$0")/common.sh"
dev_vm_require_golden
echo "golden '$PRELOOP_DEV_BASE' ready ($(dev_vm_golden_state))"
if dev_vm_gateway_enabled; then
  echo "OMP gateway brokering: enabled ($PRELOOP_DEV_GATEWAY_URL)"
else
  echo "OMP gateway brokering: disabled (set PRELOOP_DEV_GATEWAY_URL and PRELOOP_DEV_GATEWAY_TOKEN_FILE)"
fi
