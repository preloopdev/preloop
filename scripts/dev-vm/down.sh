#!/bin/sh
# Delete the current worktree's dev VM. The worktree itself is untouched.
#   down.sh [NAME]
set -eu
. "$(dirname "$0")/common.sh"

NAME=${1:-${PRELOOP_DEV_NAME:-$(dev_vm_name_for)}}
CHILD=$(dev_vm_machine "$NAME")
"$SMOLVM_BIN" machine status --name "$CHILD" >/dev/null 2>&1 || { echo "no dev VM '$CHILD'"; exit 0; }
"$SMOLVM_BIN" machine stop --name "$CHILD" >/dev/null 2>&1 || true
"$SMOLVM_BIN" machine delete --name "$CHILD" --force
echo "deleted $CHILD"
