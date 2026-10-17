#!/bin/sh
# Run a command in the current worktree's dev VM, in /workspace.
#   exec.sh COMMAND [ARGS...]
# Set PRELOOP_DEV_NAME to target a VM other than the one named after the
# current worktree. Cargo is on PATH; exit status is the command's.
set -eu
. "$(dirname "$0")/common.sh"

[ $# -gt 0 ] || die "usage: exec.sh COMMAND [ARGS...]"
NAME=${PRELOOP_DEV_NAME:-$(dev_vm_name_for)}
CHILD=$(dev_vm_machine "$NAME")
dev_vm_require_child "$CHILD"

# Login shell so rustup's PATH applies; "$@" is passed through verbatim.
if dev_vm_gateway_enabled; then
  exec "$SMOLVM_BIN" machine exec --name "$CHILD" --workdir /workspace \
    --timeout "${PRELOOP_DEV_EXEC_TIMEOUT:-30m}" \
    --secret-file "OMP_GATEWAY_TOKEN=$PRELOOP_DEV_GATEWAY_TOKEN_FILE" \
    -- sh -lc '. /root/.cargo/env; export PATH=/root/.local/bin:$PATH; exec "$@"' dev-vm "$@"
fi
exec "$SMOLVM_BIN" machine exec --name "$CHILD" --workdir /workspace \
  --timeout "${PRELOOP_DEV_EXEC_TIMEOUT:-30m}" \
  -- sh -lc '. /root/.cargo/env; export PATH=/root/.local/bin:$PATH; exec "$@"' dev-vm "$@"
