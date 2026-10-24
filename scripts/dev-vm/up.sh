#!/bin/sh
# Fork the frozen golden into a per-worktree dev VM.
#   up.sh [WORKTREE_DIR] [NAME]
# Defaults: the current Git worktree, named after its directory. Idempotent:
# an existing VM for NAME is started if stopped and its OMP config refreshed.
set -eu
. "$(dirname "$0")/common.sh"

WORKTREE=${1:-$(git rev-parse --show-toplevel 2>/dev/null || true)}
[ -n "$WORKTREE" ] && [ -d "$WORKTREE" ] || die "not in a Git worktree; pass the directory"
WORKTREE=$(CDPATH='' cd -- "$WORKTREE" && pwd)
NAME=${2:-$(dev_vm_name_for "$WORKTREE")}
CHILD=$(dev_vm_machine "$NAME")

dev_vm_require_golden

if "$SMOLVM_BIN" machine status --name "$CHILD" >/dev/null 2>&1; then
  "$SMOLVM_BIN" machine start --name "$CHILD" >/dev/null 2>&1 || true
  echo "dev VM '$CHILD' already exists"
  "$(dirname "$0")/omp-sync.sh" "$NAME"
  exit 0
fi

# A linked worktree's .git file points at the common Git directory; mount it at
# the same absolute path so Git works inside the guest.
GIT_COMMON=$(git -C "$WORKTREE" rev-parse --git-common-dir 2>/dev/null) \
  || die "not a Git worktree: $WORKTREE"
case "$GIT_COMMON" in
  /*) ;;
  *) GIT_COMMON=$(CDPATH='' cd -- "$WORKTREE/$GIT_COMMON" && pwd) ;;
esac

"$SMOLVM_BIN" machine branch --from "$PRELOOP_DEV_BASE" --name "$CHILD"
"$SMOLVM_BIN" machine stop --name "$CHILD"

set -- --volume "$WORKTREE:/workspace:rw" \
       --volume "$GIT_COMMON:$GIT_COMMON:rw" \
       --workdir /workspace
if dev_vm_gateway_enabled; then
  set -- "$@" --env "OMP_GATEWAY_URL=$PRELOOP_DEV_GATEWAY_URL"
fi
# Host->guest port forwards (HOST:GUEST, space separated). SmolVM does not relay
# a guest's localhost; a server must be published AND bind 0.0.0.0 in the guest.
for _p in ${PRELOOP_DEV_PORTS:-}; do
  set -- "$@" --port "$_p"
done
"$SMOLVM_BIN" machine update --name "$CHILD" "$@"
"$SMOLVM_BIN" machine start --name "$CHILD"

"$SMOLVM_BIN" machine exec --name "$CHILD" --timeout 30s -- \
  git config --global --add safe.directory /workspace

"$(dirname "$0")/omp-sync.sh" "$NAME"

echo "dev VM '$CHILD' ready: $WORKTREE -> /workspace"
echo "run commands with: just dev-vm-exec <command...>"
[ -z "${PRELOOP_DEV_PORTS:-}" ] || echo "published (host:guest): $PRELOOP_DEV_PORTS - bind servers to 0.0.0.0 in the guest"
