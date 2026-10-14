#!/bin/sh
# Publish guest ports to the host on an existing dev VM.
#   publish.sh HOST:GUEST [HOST:GUEST...]
# smolvm can only change ports while the machine is stopped, so this restarts
# the VM: processes running in it are killed. Prefer PRELOOP_DEV_PORTS at `up`.
# Servers must bind 0.0.0.0 inside the guest (127.0.0.1 is not reachable).
set -eu
. "$(dirname "$0")/common.sh"

[ $# -gt 0 ] || die "usage: publish.sh HOST:GUEST [HOST:GUEST...]"
NAME=${PRELOOP_DEV_NAME:-$(dev_vm_name_for)}
CHILD=$(dev_vm_machine "$NAME")
dev_vm_require_child "$CHILD"
for _p in "$@"; do set -- "$@" --port "$_p"; shift; done
"$SMOLVM_BIN" machine stop --name "$CHILD" >/dev/null 2>&1 || true
"$SMOLVM_BIN" machine update --name "$CHILD" "$@"
"$SMOLVM_BIN" machine start --name "$CHILD"
