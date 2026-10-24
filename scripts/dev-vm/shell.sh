#!/bin/sh
# Interactive shell in the current worktree's dev VM (OMP gateway token injected).
#   shell.sh [COMMAND [ARGS...]]     e.g. shell.sh omp
set -eu
. "$(dirname "$0")/common.sh"

NAME=${PRELOOP_DEV_NAME:-$(dev_vm_name_for)}
CHILD=$(dev_vm_machine "$NAME")
dev_vm_require_child "$CHILD"
[ $# -gt 0 ] || set -- bash -l

if dev_vm_gateway_enabled; then
  exec "$SMOLVM_BIN" machine exec --name "$CHILD" --workdir /workspace -it \
    --secret-file "OMP_GATEWAY_TOKEN=$PRELOOP_DEV_GATEWAY_TOKEN_FILE" \
    -- sh -lc '. /root/.cargo/env; export PATH=/root/.local/bin:$PATH; exec "$@"' dev-vm "$@"
fi
exec "$SMOLVM_BIN" machine exec --name "$CHILD" --workdir /workspace -it \
  -- sh -lc '. /root/.cargo/env; export PATH=/root/.local/bin:$PATH; exec "$@"' dev-vm "$@"
